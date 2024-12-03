web: gunicorn simbatolls.app:app
worker: python simbatolls/worker.py
worker: rq worker --timeout 600
