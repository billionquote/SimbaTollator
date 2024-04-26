web: gunicorn simbatolls.app:app
worker: celery -A carcharter simbatolls.worker --loglevel=info