from celery import shared_task, group
from django.utils import timezone
from rss_collector.models import Feed
from rss_collector.services import process_feeds
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("rss_collector")

@shared_task
def fetch_feeds_in_batches(batch_size=100):
    now = timezone.now()
    due_feeds = Feed.objects.filter(next_fetch__lt=now).only("id")

    feed_ids = list(due_feeds.values_list("id", flat=True))
    total_feeds = len(feed_ids)

    if not total_feeds:
        msg = f"[{timezone.now()}] ⚠️ No feeds due for fetching."
        logger.info(msg)
        return
    
    logger.info(f"🚀 Batch processing started at {now} for {total_feeds} feeds")
    
    batches = [feed_ids[i:i + batch_size] for i in range(0, total_feeds, batch_size)]
    batch_count = len(batches)

    for index, batch in enumerate(batches, start=1):
        logger.info(f"📦 Dispatching batch {index}/{batch_count} ({len(batch)} feeds)")
        process_batch_feeds.apply_async(args=[batch])
    
    end_time = timezone.now()
    logger.info(
        f"✅ All batches dispatched at {end_time}. "
        f"Total Feeds: {total_feeds}, Total Batches: {batch_count}"
    )

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=2)
def process_batch_feeds(feed_ids):
    start_time = timezone.now()
    feeds = list(Feed.objects.filter(id__in=feed_ids))

    max_workers = min(10, len(feeds))
    total_processed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_feed, feed): feed.id for feed in feeds
        }

        for future in as_completed(futures):
            feed_id = futures[future]
            try:
                future.result()
                total_processed += 1
            except Exception as e:
                logger.exception(f"Feed {feed_id} failed: {e}")

    end_time = timezone.now()
    logger.info(
        f"✅ Completed batch of {len(feeds)} feeds in "
        f"{(end_time - start_time).total_seconds():.2f}s "
        f"({total_processed} succeeded)"
    )

def process_single_feed(feed):
    start_time = timezone.now()
    logger.info(f" Started: {feed.name} ({feed.url})")
    try:
        result = process_feeds([feed], update_last_fetched=True)
        details = result.get("details", [{}])[0]
        added = details.get("added", 0)
        updated = details.get("updated", 0)

        logger.info(
            f"✅ Completed: {feed.name} | Added: {added}, Updated: {updated} | "
            f"Duration: {(timezone.now() - start_time).total_seconds():.2f}s"
        )
    except Exception as e:
        logger.exception(f"❌ Error processing feed {feed.name}: {e}")


#not using currently
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
