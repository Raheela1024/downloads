import os
import string
import random  # define the random module
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from downloads.helpers import send_message
from pandas._libs import json
from elasticsearch import Elasticsearch
from .celery import download_assemblies

es = Elasticsearch("http://45.88.81.118:80/elasticsearch")


@csrf_exempt
def downloads(request):
    if request.method == 'POST':
        print('in post method')
        print(request.body)
        body_unicode = request.body.decode('utf-8')
        request_body = json.loads(body_unicode)
        print(request_body.get('taxonomyFilter'))
        print(request_body.get('downloadLocation'))
        if os.path.exists(request_body.get('downloadLocation')):
            query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
            if request_body.get('taxonomyFilter'):
                for index, taxonomy in enumerate(request_body.get('taxonomyFilter')):
                    print(taxonomy.get('rank'))
                    if len(request_body.get('taxonomyFilter')) == 1:
                        query_param = query_param + '"nested" : { "path" : "taxonomies", "query" : { "nested" : { "path" : ' \
                                                    '"taxonomies.' + taxonomy.get(
                            'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                                      '"term" : { ' \
                                      '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                            'taxonomy') + '"' '}}]}}}}} '
                    elif (len(request_body.get('taxonomyFilter')) - 1) == index:
                        query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { "path" : ' \
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
            print(x)
            print(query_param)
            data_portal = es.search(index="data_portal", size=10000, body=query_param)
            names = list()
            for organism in data_portal['hits']['hits']:
                print(organism['_id'])

                if request_body.get('downloadOption') == 'assemblies' and organism.get('_source').get("assemblies"):
                    print(organism.get('_source').get("assemblies"))
                    send_message(room_id=x, download_status="100")
                    download_assemblies.delay(organism.get('_source').get("assemblies"),
                                              request_body.get('downloadOption'),
                                              request_body.get('downloadLocation'), x)
                if request_body.get('downloadOption') == 'annotation' and organism.get('_source').get("annotation"):
                    print(organism.get('_source').get("annotation"))
                    print(organism.get('_source').get("annotation")[0].get("annotation"))
                    # x = download_annotations.delay(organism.get('_source').get("annotation"), request.downloadOption,
                    #                                request_body.get('downloadOption'))
                    # print(x.task_id)

                print('---------------------------------------------')
            return HttpResponse(json.dumps({"id": x}))
        else:
            return HttpResponse({"Please Provide a Valid Location"})
    else:
        return HttpResponse({"Please Provide a Valid Location"})


def specific_string(length):
    letters = string.ascii_uppercase
    result = ''.join(random.choice(letters) for i in range(length))
    print(" Randomly generated string is: ", result)
    return result
