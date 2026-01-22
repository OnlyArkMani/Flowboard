from django.conf import settings
from redis import Redis
from rq import Queue
from rq_scheduler import Scheduler

REDIS_URL = getattr(settings, "REDIS_URL", "redis://redis:6379/0")
redis_conn = Redis.from_url(REDIS_URL)

default_queue = Queue("default", connection=redis_conn)
default_scheduler = Scheduler("default", connection=redis_conn)

__all__ = ["redis_conn", "default_queue", "default_scheduler"]
