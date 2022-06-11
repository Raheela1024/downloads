python manage.py collectstatic
python manage.py makemigrations
python manage.py migrate
celery -A downloads worker --loglevel=INFO --concurrency=10
