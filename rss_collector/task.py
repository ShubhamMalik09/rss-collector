from celery import shared_task, group
from .models import Feed, Article
from .utils import parse_feed
from django.utils import timezone
from django.db import IntegrityError, transaction

@shared_task
def fetch_all_feeds():
    feeds = Feed.objects.all()
    total_new = 0
    for feed in feeds:
        entries = parse_feed(feed.url, last_fetched=feed.last_fetched)
        new_count = 0
        for entry in entries:
            try:
                with transaction.atomic():
                    obj, created = Article.objects.get_or_create(
                        url=entry["url"],
                        defaults={
                            "feed": feed,
                            "title": entry.get("title") or "",
                            "published_at": entry.get("published"),
                            "content": entry.get("content") or "",
                        },
                    )
                    if created:
                        new_count += 1
            except IntegrityError:
                continue
        feed.last_fetched = timezone.now()
        feed.save(update_fields=["last_fetched"])
        total_new += new_count
    return f"Fetched {total_new} new articles."

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def fetch_feed(self, feed_id):
    """
    Task to fetch and save articles for a single feed.
    Automatically retries on transient errors.
    """
    try:
        feed = Feed.objects.get(id=feed_id)
    except Feed.DoesNotExist:
        return f"⚠️ Feed {feed_id} not found."

    new_count = 0
    try:
        entries = parse_feed(feed.url, last_fetched=feed.last_fetched)
    except Exception as e:
        raise self.retry(exc=e, countdown=10)

    for entry in entries:
        try:
            with transaction.atomic():
                _, created = Article.objects.get_or_create(
                    url=entry["url"],
                    defaults={
                        "feed": feed,
                        "title": entry.get("title") or "",
                        "published_at": entry.get("published"),
                        "content": entry.get("content") or "",
                    },
                )
                if created:
                    new_count += 1
        except IntegrityError:
            continue

    feed.last_fetched = timezone.now()
    feed.save(update_fields=["last_fetched"])

    return f"✅ {feed.name}: {new_count} new articles"

@shared_task
def fetch_feeds_in_batches(batch_size=100):
    """
    Divides all feeds into batches (e.g., 100 feeds per batch)
    and processes each batch in parallel using Celery groups.
    """
    feed_ids = list(Feed.objects.values_list("id", flat=True))
    total_feeds = len(feed_ids)
    if not total_feeds:
        return "No feeds available."

    # Split feed_ids into batches
    batches = [feed_ids[i:i + batch_size] for i in range(0, total_feeds, batch_size)]
    batch_count = len(batches)

    for index, batch in enumerate(batches, start=1):
        job = group(fetch_feed.s(feed_id) for feed_id in batch)
        job.apply_async()
        print(f"🚀 Dispatched batch {index}/{batch_count} with {len(batch)} feeds")

    return f"✅ Dispatched {batch_count} batches ({total_feeds} feeds total)"