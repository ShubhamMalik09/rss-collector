from celery import shared_task, group
from django.utils import timezone
from rss_collector.models import Feed
from rss_collector.services import process_feeds 


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def fetch_feed(self, feed_id):
    try:
        feed = Feed.objects.get(id=feed_id)
    except Feed.DoesNotExist:
        return {"feed_id": feed_id, "error": "Feed not found"}

    try:
        result = process_feeds([feed]) 
        return {
            "feed_id": feed_id,
            "feed_name": feed.name,
            "added": result["details"][0]["added"],
            "updated": result["details"][0]["updated"],
        }
    except Exception as e:
        raise self.retry(exc=e, countdown=10)


@shared_task
def fetch_all_feeds():
    """
    Fetch all feeds stored in DB using process_feeds().
    This runs synchronously (non-batched).
    """
    from rss_collector.models import Feed

    feeds = Feed.objects.all().order_by("id")
    result = process_feeds(feeds)
    total = result["total_new"]
    updated = sum(r["updated"] for r in result["details"])
    return f"✅ Total feeds processed: {len(feeds)}, New: {total}, Updated: {updated}"


@shared_task
def fetch_feeds_in_batches(batch_size=100):
    """
    Split feeds into batches
    and process each batch concurrently using Celery group().
    """
    feed_ids = list(Feed.objects.values_list("id", flat=True))
    total_feeds = len(feed_ids)
    if not total_feeds:
        return "⚠️ No feeds available."

    batches = [feed_ids[i:i + batch_size] for i in range(0, total_feeds, batch_size)]
    batch_count = len(batches)

    for index, batch in enumerate(batches, start=1):
        job = group(fetch_feed.s(feed_id) for feed_id in batch)
        job.apply_async()
        print(f"🚀 Dispatched batch {index}/{batch_count} ({len(batch)} feeds)")

    return f"✅ {batch_count} batches dispatched for {total_feeds} feeds."
