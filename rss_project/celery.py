import os
from celery import Celery
from celery.schedules import crontab
import logging.config
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rss_project.settings")

app = Celery("rss_project")
logging.config.dictConfig(settings.LOGGING)
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Schedule: run every 5 minutes
app.conf.beat_schedule = {
    "fetch-feeds-in-batches-every-20-minutes": {
        "task": "rss_collector.tasks.fetch_feeds_in_batches",
        "schedule": crontab(minute="*/20"),
        "args": (10,),  # batch size = 10
    },
}
