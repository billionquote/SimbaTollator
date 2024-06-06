import os
import redis
from urllib.parse import urlparse
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

#redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

#conn = redis.from_url(redis_url, ssl_cert_reqs=None)
#r = redis.from_url(os.environ.get("REDIS_URL"))
url = urlparse(os.environ.get("REDIS_URL"))
r = redis.Redis(host=url.hostname, port=url.port, password=url.password, ssl=True, ssl_cert_reqs=None)
if __name__ == '__main__':
    with Connection(r):
        worker = Worker(map(Queue, listen))
        worker.work()