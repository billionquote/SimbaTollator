import os
import redis
# from redis import Redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

# redis_url = os.getenv('REDIS_URL', 'rediss://:p2d977ad1effa8afc44191abd9c4cba2adba103d6408c7de2bdb0619d1ecc47ee@ec2-54-209-222-58.compute-1.amazonaws.com:6669')
redis_url = os.getenv('REDIS_URL', 'rediss://:p2d977ad1effa8afc44191abd9c4cba2adba103d6408c7de2bdb0619d1ecc47ee@ec2-52-207-125-22.compute-1.amazonaws.com:28009')

conn = redis.from_url(redis_url, ssl_cert_reqs=None)
# conn = Redis(host='localhost', port=6379)
#conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()