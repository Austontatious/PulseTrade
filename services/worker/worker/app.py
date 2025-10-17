from celery import Celery
import os

app = Celery("pulse_worker", broker=os.getenv("REDIS_URL","redis://redis:6379/0"))
app.conf.task_queues = ()
