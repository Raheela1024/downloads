import os
import string
import random  # define the random module
from django.http import HttpResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from .helpers import send_message
from pandas._libs import json
from elasticsearch import Elasticsearch
from django.conf import settings
from elasticsearch import RequestsHttpConnection
from .celery import download_assemblies, download_annotations
from celery.result import AsyncResult

es = Elasticsearch([settings.NODE],
                   connection_class=RequestsHttpConnection,
                   use_ssl=False, verify_certs=False)


@csrf_exempt
def downloads(request):
    global total_records
    if request.method == 'POST':
        print('in post method')
        print(request.body)
        body_unicode = request.body.decode('utf-8')
        request_body = json.loads(body_unicode)
        print(request_body.get('taxonomyFilter'))
        print(request_body.get('downloadLocation'))
        # if os.path.exists(request_body.get('downloadLocation')):
        query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
        if True:
            for index, taxonomy in enumerate(request_body.get('taxonomyFilter')):
                print(taxonomy.get('rank'))
                if len(request_body.get('taxonomyFilter')) == 1:
                    query_param = query_param + '"nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                                '"path" : ' \
                                                '"taxonomies.' + taxonomy.get(
                        'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                                  '"term" : { ' \
                                  '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                        'taxonomy') + '"' '}}]}}}}} '
                elif (len(request_body.get('taxonomyFilter')) - 1) == index:
                    query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                                '"path" : ' \
                                                '"taxonomies.' + taxonomy.get(
                        'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                                  '"term" : { ' \
                                  '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                        'taxonomy') + '"' '}}]}}}}}} '
                else:
                    query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                                '"path" : ' \
                                                '"taxonomies.' + taxonomy.get(
                        'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                                  '"term" : { ' \
                                  '"taxonomies.' + taxonomy.get('rank') + '.scientificName" :''"' + taxonomy.get(
                        'taxonomy') + '"' '}}]}}}}}}, '

            query_param = query_param + '] }}}'
            print('query_param')
            x = specific_string(8)
            count_size = 0
            print(x)
            print(query_param)
            data_portal = es.search(index="data_portal", size=10000, body=json.loads(
                query_param))
            names = list()
            if data_portal['hits']['hits']:
                total_records = calculate_total_records(data_portal['hits']['hits'], request_body.get('downloadOption'))
            send_message(room_id=x, download_status=0)
            for organism in data_portal['hits']['hits']:
                if request_body.get('downloadOption') == 'assemblies' and organism.get('_source').get("assemblies"):
                    print(len(organism.get('_source').get("assemblies")))
                    print('connection created')
                    task = download_assemblies.delay(organism.get('_source').get("assemblies"),
                                              request_body.get('downloadOption'),
                                              request_body.get('downloadLocation'), x, total_records)
                    count_size = count_size + 1
                    print(task.task.task_id)
                elif request_body.get('downloadOption') == 'annotation' and organism.get('_source').get("annotation"):
                    print(organism.get('_source').get("annotation"))
                    print(organism.get('_source').get("annotation")[0].get("annotation"))
                    download_annotations.delay(organism.get('_source').get("annotation"),
                                               request_body.get('downloadOption'),
                                               request_body.get('downloadLocation'), x, total_records)
                    count_size += 1
                    print(x)

            print(count_size)
            send_message(room_id=x, total_records=count_size)
            return HttpResponse(json.dumps({"id": x,"task_id": task.task_id}))
        else:
            return HttpResponse({"Please Provide a Valid Location"})
    # else:
    #     return HttpResponse({"Please Provide a Valid Location"})

# result = my_task.AsyncResult(task_id)
# x = result.get()

def calculate_total_records(dataList, downloadOption):
    total_records = 0
    for organism in dataList:
        if downloadOption == 'assemblies' and organism.get('_source').get("assemblies"):
            total_records = total_records + 1
        elif downloadOption == 'annotation' and organism.get('_source').get("annotation"):
            total_records = total_records + 1

    return total_records


def specific_string(length):
    letters = string.ascii_uppercase
    result = ''.join(random.choice(letters) for i in range(length))
    print(" Randomly generated string is: ", result)
    return result


@csrf_exempt
def get_downloaded_file(request, filename):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    file_path = os.path.join(filename)
    print(file_path)
    if os.path.exists(file_path):
        with open(file_path, 'rb') as fh:
            response = HttpResponse(fh.read(), content_type="application/zip")
            response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
            return response
    raise Http404


@csrf_exempt
def get_task_status(request, task_id):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
        res = AsyncResult(task_id)
        return res.ready()
    raise Http404
