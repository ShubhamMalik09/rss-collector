from django.utils import timezone
from datetime import datetime
from django.db import transaction, IntegrityError
from rss_collector.models import Feed, Article
from rss_collector.utils import parse_feed

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
        feed_url = feed.url
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

        # Save new articles
        for entry in entries:
            article_url = entry.get("url")
            if not article_url:
                continue

            title = entry.get("title") or ""
            published = entry.get("published")
            content = entry.get("content") or ""
            authors = entry.get("authors", [])
            categories = entry.get("categories", [])
            meta_keywords = entry.get("meta_keywords", [])

            try:
                with transaction.atomic():
                    article, created = Article.objects.get_or_create(
                        url=article_url,
                        defaults={
                            "feed": feed,
                            "title": title,
                            "published_at": published,
                            "content": content,
                            "authors": authors,
                            "categories": categories,
                            "meta_keywords": meta_keywords,
                        },
                    )
                    if created:
                        new_count += 1
                    else:
                        updated_fields = []

                        def has_changed(field, new_value):
                            """Compare normalized values safely."""
                            old_value = getattr(article, field, None)
                            if isinstance(old_value, list) and isinstance(new_value, list):
                                return sorted(old_value) != sorted(new_value)
                            return (old_value or "").strip() != (new_value or "").strip()
                        
                        for field, new_value in {
                            "title": title,
                            "content": content,
                            "authors": authors,
                            "categories": categories,
                            "meta_keywords": meta_keywords,
                        }.items():
                            if has_changed(field, new_value):
                                setattr(article, field, new_value)
                                updated_fields.append(field)

                        if published and (not article.published_at or published > article.published_at):
                            article.published_at = published
                            updated_fields.append("published_at")

                        if updated_fields:
                            article.save(update_fields=updated_fields)
                            updated_count += 1
            except IntegrityError:
                continue
        
        if update_last_fetched:
            feed.last_fetched = timezone.now()
            feed.save(update_fields=["last_fetched"])

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
