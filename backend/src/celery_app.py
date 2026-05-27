import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv(
    'REDIS_URL',
    'redis://redis:6379/0'
)

celery_app = Celery(
    'distributed_content_pipeline',
    broker=REDIS_URL,
    backend=REDIS_URL.replace('/0', '/1'),
    include=['src.tasks.email_tasks']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    beat_scheduler='redbeat.RedBeatScheduler',
    redbeat_redis_url=REDIS_URL,
    redbeat_lock_timeout=60,
    beat_schedule={
        'dispatch-due-emails-every-60s': {
            'task': 'src.tasks.email_tasks.dispatch_due_emails',
            'schedule': 60.0,
        }
    }
)