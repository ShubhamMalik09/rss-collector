from django.utils import timezone
from datetime import datetime, timedelta
from django.db import IntegrityError
from rss_collector.models import Feed, Article
from rss_collector.utils import parse_feed

# common for both celery and fetch_feeds_view
def process_feeds(feeds, max_entries=None, start_date=None, end_date=None, update_last_fetched=False):
    """
    Common logic for processing a list of feed objects or feed-like dicts.
    Each feed should have 'name' and 'url' attributes or keys.
    """
    total_new = 0
    feed_results = []

    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if timezone.is_naive(start_date):
            start_date = timezone.make_aware(start_date)
    
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        if timezone.is_naive(end_date):
            end_date = timezone.make_aware(end_date)

    for feed in feeds:
        if not feed.name or not feed.url:
            feed_results.append({
                "feed": getattr(feed, "url", "Unknown"),
                "error": "Missing required fields (name or URL), skipping.",
                "added": 0,
                "updated": 0
            })
            continue

        feed_name = feed.name
        feed_last_fetched = feed.last_fetched

        new_count = 0
        updated_count = 0

        try:
            entries = parse_feed(
                feed,
                last_fetched=feed_last_fetched,
                max_entries=max_entries,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            feed_results.append({
                "feed": feed_name,
                "error": f"Failed to parse feed: {str(e)}",
                "added": 0,
                "updated": 0
            })
            continue

        entry_urls = [entry.get("url") for entry in entries if entry.get("url")]
        existing_articles = Article.objects.filter(url__in=entry_urls).all()
        existing_articles_map = { a.url: a for a in existing_articles}
        existing_urls = set(existing_articles_map.keys())

        new_entries = [e for e in entries if e.get("url") not in existing_urls]
        existing_entries = [e for e in entries if e.get("url") in existing_urls]

        new_objects = [
            Article(
                feed=feed,
                url=e["url"],
                title=e.get("title", ""),
                published_at=e.get("published"),
                content=e.get("content", ""),
                authors=e.get("authors", []),
                categories=e.get("categories", []),
                meta_keywords=e.get("meta_keywords", []),
            )
            for e in new_entries
        ]

        if new_objects:
            try:
                Article.objects.bulk_create(new_objects, ignore_conflicts=True)
                new_count = len(new_objects)
            except IntegrityError:
                new_count = 0
        
        updated_articles = []
        for entry in existing_entries:
            url = entry.get("url")

            article = existing_articles_map.get(url)
            if not article:
                continue

            changed = False

            def has_changed(field, new_value):
                old_value = getattr(article, field, None)
                if isinstance(old_value, datetime) and isinstance(new_value, datetime):
                    return new_value > old_value

                if isinstance(old_value, list) and isinstance(new_value, list):
                    return old_value != new_value
    
                return (old_value or "").strip() != (new_value or "").strip()

            for field, new_value in {
                "title": entry.get("title", ""),
                "content": entry.get("content", ""),
                "authors": entry.get("authors", []),
                "categories": entry.get("categories", []),
                "meta_keywords": entry.get("meta_keywords", []),
                "published_at": entry.get("published"),
            }.items():
                if has_changed(field, new_value):
                    setattr(article, field, new_value)
                    changed = True

            if changed:
                updated_articles.append(article)

        if updated_articles:
            Article.objects.bulk_update(
                updated_articles,
                ["title", "content", "authors", "categories", "meta_keywords", "published_at"]
            )
            updated_count = len(updated_articles)
        
        if update_last_fetched:
            now = timezone.now()
            feed.last_fetched = now
            feed.next_fetch = now + timedelta(minutes=feed.call_frequency)
            feed.save(update_fields=["last_fetched", "next_fetch"])

        feed_results.append({
            "feed": feed_name,
            "added": new_count,
            "updated": updated_count,
            "total_articles": len(entries)
        })
        total_new += new_count

    return {
        "total_new": total_new,
        "details": feed_results
    }


# using for fetch_feed_view
def fetch_all_feeds(limit_feeds=None, max_entries=None, start_date=None, end_date=None, update_last_fetched=False):
    """
    Fetch feeds stored in DB and process them via process_feeds().
    """
    feeds = Feed.objects.all().order_by("id")
    if limit_feeds:
        feeds = feeds[:limit_feeds]

    return process_feeds(
        feeds=feeds,
        max_entries=max_entries,
        start_date=start_date,
        end_date=end_date,
        update_last_fetched=update_last_fetched
    )

# using for fetch_feed_view
def fetch_custom_feeds(urls, max_entries=None, start_date=None, end_date=None):
    """
    Fetch custom list of RSS URLs provided by user and process via process_feeds().
    """
    feeds = Feed.objects.filter(url__in=urls).order_by("id")

    return process_feeds(
        feeds=feeds,
        max_entries=max_entries,
        start_date=start_date,
        end_date=end_date,
        update_last_fetched=False
    )
