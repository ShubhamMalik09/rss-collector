from django.utils import timezone
from datetime import datetime
from django.db import transaction, IntegrityError
from rss_collector.models import Feed, Article
from rss_collector.utils import parse_feed

def process_feeds(feeds, max_entries=None, start_date=None, end_date=None):
    """
    Common logic for processing a list of feed objects or feed-like dicts.
    Each feed should have 'name' and 'url' attributes or keys.
    """
    total_new = 0
    feed_results = []

    for feed in feeds:
        # handle Feed model or dict
        feed_url = getattr(feed, "url", None) or feed.get("url")
        feed_name = getattr(feed, "name", None) or feed.get("name") or feed_url.split("/")[2]
        feed_last_fetched = getattr(feed, "last_fetched", None) or None
        new_count = 0
        updated_count = 0

        try:
            entries = parse_feed(
                feed_url,
                last_fetched=feed_last_fetched,
                max_entries=max_entries,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            feed_results.append({"feed": feed_name, "error": str(e), "added": 0})
            continue

        # Ensure a Feed record exists
        db_feed, _ = Feed.objects.get_or_create(
            url=feed_url,
            defaults={"name": feed_name, "last_fetched": timezone.now()}
        )

        # Save new articles
        for entry in entries:
            article_url = entry["url"]
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
                            "feed": db_feed,
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
                        
                        if has_changed("title", title):
                            article.title = title
                            updated_fields.append("title")

                        if has_changed("content", content):
                            article.content = content
                            updated_fields.append("content")

                        if has_changed("authors", authors):
                            article.authors = authors
                            updated_fields.append("authors")

                        if has_changed("categories", categories):
                            article.categories = categories
                            updated_fields.append("categories")

                        if has_changed("meta_keywords", meta_keywords):
                            article.meta_keywords = meta_keywords
                            updated_fields.append("meta_keywords")

                        new_published = published
                        if (new_published and (not article.published_at or new_published > article.published_at)):
                            article.published_at = new_published
                            updated_fields.append("published_at")

                        if updated_fields:
                            article.save(update_fields=updated_fields)
                            updated_count += 1
            except IntegrityError:
                continue

        db_feed.last_fetched = timezone.now()
        db_feed.save(update_fields=["last_fetched"])
        total_new += new_count
        feed_results.append({"feed": feed_name, "added": new_count, "updated": updated_count})

    return {"total_new": total_new, "details": feed_results}

def fetch_all_feeds(limit_feeds=None, max_entries=None, start_date=None, end_date=None):
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
        end_date=end_date
    )

def fetch_custom_feeds(urls, max_entries=None, start_date=None, end_date=None):
    """
    Fetch custom list of RSS URLs provided by user and process via process_feeds().
    """
    feeds = [{"url": url, "name": url.split("/")[2]} for url in urls]

    return process_feeds(
        feeds=feeds,
        max_entries=max_entries,
        start_date=start_date,
        end_date=end_date
    )
