from __future__ import absolute_import, unicode_literals
import os
import time
from celery import Celery
from decouple import config
import os
import sys
import urllib
from celery.utils.log import get_task_logger

import shutil

from urllib import request

from .helpers import send_message

celery_log = get_task_logger(__name__)
# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'downloads.settings')

app = Celery(
    'downloads',
    broker=config('BROKER_URL'),
    backend=config('CELERY_BACKED_URL'))

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.broker_transport_options = {'visibility_timeout': 43200}

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task
def debug_task(self, message):
    print(message)
    print('Request: {0!r}'.format(self.request))
    time.sleep(10)
    return message


@app.task
def download_annotations(annotationList, downloadOption, parent_dir,x):
    for index,  annotation in enumerate(annotationList):

        # print(annotation)
        if annotation.get('annotation'):
            print(annotation.get('annotation').get('GTF'))
            print(annotation.get('annotation').get('GFF3'))
            download_file(annotation.get('annotation').get('GTF'), downloadOption + '/GFT', parent_dir)
            download_file(annotation.get('annotation').get('GFF3'), downloadOption + '/GFF3', parent_dir)
        if annotation.get('proteins'):
            print(annotation.get('proteins').get('FASTA'))
            download_file(annotation.get('proteins').get('FASTA'), downloadOption + '/proteins', parent_dir)
        if annotation.get('transcripts'):
            print(annotation.get('transcripts').get('FASTA'))
            download_file(annotation.get('transcripts').get('FASTA'), downloadOption + '/transcripts', parent_dir)
        if annotation.get('softmasked_genome'):
            print(annotation.get('softmasked_genome').get('FASTA'))
            download_file(annotation.get('softmasked_genome').get('FASTA'), downloadOption +
                          '/softmasked-genome', parent_dir)
            celery_log.info(x)
        send_message(room_id=x, download_status=(100 * float(index + 1) / float(len(annotationList))))
    celery_log.info((100 * float(index + 1) / float(len(annotationList))))
    shutil.make_archive(downloadOption, 'zip', downloadOption)
    return {"message": f"Hi , Your order has completed!"}


@app.task
def download_assemblies(assembliesList, downloadOption, parent_dir, x):
    print(parent_dir)
    celery_log.info(f"download started!")
    for index, assemblies in enumerate(assembliesList):
        print(assemblies.get('accession'))
        url = "https://www.ebi.ac.uk/ena/browser/api/fasta/" + assemblies.get('accession') + "?download=true&gzip=true"
        print(url)
        download_file(url, assemblies.get('accession'), downloadOption, parent_dir)
        celery_log.info(x)
        send_message(room_id=x, download_status=(100 * float(index + 1) / float(len(assembliesList))))
        celery_log.info((100 * float(index + 1) / float(len(assembliesList))))
    # shutil.make_archive(downloadOption, 'zip', downloadOption)
    celery_log.info(f"download completed!")
    return {"message": f"Hi , Your order has completed!"}


def download_file(url, filename, directory, parent_dir):
    def hook(blocks, block_size, total_size):
        current = blocks * block_size
        percent = 100.0 * current / total_size
        # Found this somewhere, don't remember where sorry
        line = '[{0}{1}]'.format('=' * int(percent / 2), ' ' * (50 - int(percent / 2)))
        status = '\r{0:3.0f}%{1} {2:3.1f}/{3:3.1f} MB'
        sys.stdout.write(status.format(percent, line, current / 1024 / 1024, total_size / 1024 / 1024))

    urllib.request.urlretrieve(url,
                               create_directory(url, filename, directory, parent_dir))
    sys.stdout.write('Download Completed !!')


def create_directory(url, filename, directory, parent_dir):
    path = os.path.join(parent_dir, directory)
    print("Directory '% s' created" % path)
    try:
        os.makedirs(path, exist_ok=True)
        print("Directory '%s' created successfully" % directory)
    except OSError as error:
        print("Directory '%s' can not be created" % directory)
    print("Directory '% s' created" % directory)
    if filename:
        local_filename = os.path.join(parent_dir, directory + '/' + filename + '.fasta')
    else:
        local_filename = os.path.join(parent_dir, directory + '/' + url.split('/')[-1])
    print("local_filename '%s' is" % local_filename)
    return local_filename
