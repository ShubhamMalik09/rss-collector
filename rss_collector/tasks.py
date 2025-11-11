from celery import shared_task, group
from django.utils import timezone
from rss_collector.models import Feed
from rss_collector.services import process_feeds
import logging

logger = logging.getLogger("rss_collector")

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def fetch_feed(self, feed_id):
    try:
        feed = Feed.objects.get(id=feed_id)
    except Feed.DoesNotExist:
        msg = f"[{timezone.now()}] ❌ Feed {feed_id} not found"
        logger.error(msg)
        return {"feed_id": feed_id, "error": "Feed not found"}
    
    if not feed.should_fetch():
        msg = f"[{timezone.now()}] ⏩ Skipped: {feed.url} (Not due for fetching)"
        logger.info(msg)
        return {
            "feed_id": feed.id,
            "feed_name": feed.name,
            "skipped": True,
            "reason": "Not due for fetching yet"
        }
    
    start_time = timezone.now()
    logger.info(f"🚀 Started fetching: {feed.url} at {start_time}")

    try:
        result = process_feeds([feed], max_entries=None, start_date=None, end_date=None, update_last_fetched=True)
        details = result["details"][0] if result["details"] else {}
        end_time = timezone.now()

        added = details.get("added", 0)
        updated = details.get("updated", 0)

        logger.info(
            f" Completed: {feed.url}\n"
            f" Start: {start_time} | End: {end_time} | Duration: {(end_time - start_time).total_seconds()}s\n"
            f"Added: {added} | 🔁 Updated: {updated}"
        )

        return {
            "feed_id": feed_id,
            "feed_name": feed.name,
            "added": result["details"][0]["added"],
            "updated": result["details"][0]["updated"],
            "skipped": False,
        }
    except Exception as e:
        logger.exception(f"❌ Error processing feed {feed.url}: {e}")
        raise self.retry(exc=e, countdown=10)


@shared_task
def fetch_all_feeds():
    """
    Fetch all feeds stored in DB using process_feeds().
    This runs synchronously (non-batched).
    """

    start_time = timezone.now()
    logger.info(f"🚀 Batch started at {start_time}")
    feeds_to_fetch = [feed for feed in Feed.objects.all() if feed.should_fetch()]
    total_due = len(feeds_to_fetch)

    if not total_due:
        msg = f"[{timezone.now()}] ⚠️ No feeds due for fetching."
        logger.info(msg)
        return "⚠️ No feeds due for fetching."
    
    result = process_feeds(feeds_to_fetch)
    total_new = result["total_new"]
    updated = sum(r["updated"] for r in result["details"])

    end_time = timezone.now()
    logger.info(
        f" Batch completed at {end_time}\n"
        f" Duration: {(end_time - start_time).total_seconds()}s\n"
        f" Total Feeds: {total_due} |  New: {total_new} |  Updated: {updated}"
    )

    return f"✅ {total_due} feeds fetched. New: {total_new}, Updated: {updated}"


@shared_task
def fetch_feeds_in_batches(batch_size=100):
    """
    Split feeds into batches
    and process each batch concurrently using Celery group().
    """

    due_feeds = Feed.objects.all()
    feed_ids = [feed.id for feed in due_feeds if feed.should_fetch()]
    total_feeds = len(feed_ids)

    if not total_feeds:
        msg = f"[{timezone.now()}] ⚠️ No feeds due for fetching."
        logger.info(msg)
        return "⚠️ No feeds due for fetching."
    
    start_time = timezone.now()
    logger.info(f"🚀 Batch processing started at {start_time} for {total_feeds} feeds")
    
    batches = [feed_ids[i:i + batch_size] for i in range(0, total_feeds, batch_size)]
    batch_count = len(batches)

    for index, batch in enumerate(batches, start=1):
        logger.info(f"📦 Dispatching batch {index}/{batch_count} ({len(batch)} feeds)")
        job = group(fetch_feed.s(feed_id) for feed_id in batch)
        job.apply_async()
        print(f"🚀 Dispatched batch {index}/{batch_count} ({len(batch)} feeds)")
    
    end_time = timezone.now()
    logger.info(
        f"✅ All batches dispatched at {end_time}. "
        f"Total Feeds: {total_feeds}, Total Batches: {batch_count}"
    )

    return f"✅ {batch_count} batches dispatched for {total_feeds} feeds."
