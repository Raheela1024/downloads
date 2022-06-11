FROM python:3.9.7
ADD downloads downloads
WORKDIR downloads
ADD requirements.txt ./
ADD run_daphne.sh ./
ADD run_celery.sh ./
ADD run_flower.sh ./
RUN pip install -r requirements.txt
RUN pip install --upgrade awscli
