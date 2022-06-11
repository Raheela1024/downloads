python manage.py collectstatic
python manage.py makemigrations
python manage.py migrate
daphne --bind 0.0.0.0 --port 8000 downloads.asgi:application
