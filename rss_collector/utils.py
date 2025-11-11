import feedparser
from newspaper import Article as NewspaperArticle
from datetime import datetime
import time
from django.utils import timezone
import requests
import re
from bs4 import BeautifulSoup

def parse_feed(feed_url, config, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    config = config or {}
    parser_type = config.get("parser_type", "generic")

    if parser_type == "generic":
        return parse_generic_feed(feed_url, config, last_fetched, max_entries, start_date, end_date)
    elif parser_type == 'json_feed':
        return parse_json_feed(feed_url, config, last_fetched, max_entries, start_date, end_date)
    elif parser_type == "article":
        return parse_html_feed(feed_url, config, last_fetched, max_entries, start_date, end_date)
    else:
        raise ValueError(f"Unknown parser type: {parser_type}")

def parse_generic_feed(feed_url, config, last_fetched=None, max_entries=None, start_date=None, end_date=None):
    config = config or {}
    field_map = config.get("field_mapping", {})
    extract_full_content = config.get("extract_full_content", False)

    try:
        xml_text = requests.get(feed_url, timeout=10).text
        parsed = feedparser.parse(xml_text)
    except Exception as e:
        print(f"[parse_generic_feed] Failed to fetch {feed_url}: {e}")
        return []

    entries = parsed.entries or []
    if max_entries:
        entries = entries[:max_entries]

    results = []

    for entry in entries:
        try:
            url_field = field_map.get("url")
            title_field = field_map.get("title")
            desc_field = field_map.get("description")
            author_field = field_map.get("author")
            published_field = field_map.get("published_date")
            categories_field = field_map.get("categories", "tags")

            url = entry.get(url_field) if url_field else None
            if not url:
                continue

            title = (entry.get(title_field) or "").strip() if title_field else ""
            description = (entry.get(desc_field) or "").strip() if desc_field else ""
            author = (entry.get(author_field) or "").strip() if author_field else ""


            published = None
            if published_field:
                pub_raw = entry.get(published_field)
                if isinstance(pub_raw, time.struct_time):
                    published = datetime(*pub_raw[:6])
                elif isinstance(pub_raw, str):
                    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S%z"):
                        try:
                            published = datetime.strptime(pub_raw, fmt)
                            break
                        except Exception:
                            continue

            if published and timezone.is_naive(published):
                published = timezone.make_aware(published)
            
            if start_date and published and published < start_date:
                continue
            if end_date and published and published > end_date:
                continue
            if last_fetched and not start_date and published and published <= last_fetched:
                continue
            
            content = description
            authors = [author] if author else []
            meta_keywords = []
            categories = []

            tags_data = entry.get(categories_field)
            if tags_data:
                if isinstance(tags_data, list):
                    categories = [
                        t.get("term") for t in tags_data if isinstance(t, dict) and t.get("term")
                    ]
                elif isinstance(tags_data, str):
                    categories = [tags_data]
            
            if extract_full_content:
                try:
                    article = NewspaperArticle(url)
                    article.download()
                    article.parse()

                    if article.text and len(article.text) > len(content):
                        content = article.text.strip()

                    if article.authors:
                        authors = article.authors

                    try:
                        article.nlp()
                        meta_keywords = article.keywords or []
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[parse_generic_feed] Newspaper3k failed for {url}: {e}")

            results.append({
                "url": url,
                "title": title,
                "summary": description,
                "content": content,
                "authors": authors,
                "categories": categories,
                "meta_keywords": meta_keywords or categories,
                "published": published,
            })
        except Exception as e:
            print(f"[parse_generic_feed] Skipped entry: {e}")
            continue

    return results

def parse_html_feed(feed_url, config, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    if not config or "article_config" not in config:
        print(f"[parse_article_feed] Missing article_config for {feed_url}")
        return []

    article_config = config.get("article_config", {})
    urls = config.get("urls", [])
    if not urls:
        print(f"[parse_article_feed] No URLs provided in config for {feed_url}")
        return []

    if max_entries:
        urls = urls[:max_entries]

    results = []

    for url in urls:
        try:
            response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code != 200:
                print(f"[parse_article_feed] Failed to fetch {url} ({response.status_code})")
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            title_selector = article_config.get("title")
            title = ""
            if title_selector:
                el = soup.select_one(title_selector)
                if el:
                    title = el.get("content") or el.get_text(strip=True)
            
            author_selector = article_config.get("author")
            author = ""
            if author_selector:
                el = soup.select_one(author_selector)
                if el:
                    author = el.get("content") or el.get_text(strip=True)
            
            content_selector = article_config.get("content")
            content = ""
            if content_selector:
                els = soup.select(content_selector)
                if els:
                    parts = []
                    for e in els:
                        text = e.get_text(strip=True)
                        if text:
                            parts.append(text)
                    content = "\n".join(parts).strip()
            
            keywords_selector = article_config.get("keywords")
            keywords = []
            if keywords_selector:
                el = soup.select_one(keywords_selector)
                if el:
                    kw_content = el.get("content") or el.get_text(strip=True)
                    if kw_content:
                        if "," in kw_content:
                            keywords = [k.strip() for k in kw_content.split(",") if k.strip()]
                        else:
                            keywords = [kw_content.strip()]
            
            category_selector = article_config.get("categories")
            categories = []
            if category_selector:
                el = soup.select_one(category_selector)
                if el:
                    temp = []
                    for el in els:
                        val = el.get("content") or el.get_text(strip=True)
                        if not val:
                            continue
                        if "," in val:
                            temp += [c.strip() for c in val.split(",") if c.strip()]
                        else:
                            temp.append(val.strip())
                    seen = set()
                    categories = [c for c in temp if not (c in seen or seen.add(c))]
            
            published_selector = article_config.get("published_date")
            published = None
            if published_selector:
                el = soup.select_one(published_selector)
                pub_raw = None
                if el:
                    pub_raw = el.get("content") or el.get_text(strip=True)
                if pub_raw:
                    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%a, %d %b %Y %H:%M:%S %Z"):
                        try:
                            published = datetime.strptime(pub_raw, fmt)
                            break
                        except Exception:
                            continue
                    if published and timezone.is_naive(published):
                        published = timezone.make_aware(published)

            if start_date and published and published < start_date:
                continue
            if end_date and published and published > end_date:
                continue
            if last_fetched and not start_date and published and published <= last_fetched:
                continue
            
            summary = content[:300] + "..." if len(content) > 300 else content
            results.append({
                "url": url,
                "title": title,
                "summary": summary,
                "content": content,
                "authors": [author] if author else [],
                "categories": categories,
                "meta_keywords": keywords,
                "published": published,
            })

        except Exception as e:
            print(f"[parse_article_feed] Failed for {url}: {e}")
            continue

    return results

def parse_json_feed(feed_url, config, last_fetched=None, max_entries=None, start_date=None, end_date=None):
    """
    Parses JSON-based feeds (standard JSONFeed or custom API feeds)
    without any helper functions. Everything is inline and config-driven.
    """

    api_config = config.get("api_config", {}) if config else {}
    feed_endpoint = api_config.get("url", feed_url)
    field_map = api_config.get("field_mapping", {})
    items_path = api_config.get("items_path", "items")

    try:
        response = requests.get(feed_endpoint, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[parse_json_feed] Failed to fetch or parse JSON feed: {e}")
        return []

    items = data
    if "." in items_path:
        for key in items_path.split("."):
            if isinstance(items, dict):
                items = items.get(key)
            else:
                items = None
                break
    else:
        items = data.get(items_path)

    if not isinstance(items, list):
        print(f"[parse_json_feed] Invalid or missing 'items' list at path '{items_path}'")
        return []

    if max_entries:
        items = items[:max_entries]

    results = []

    for entry in items:
        try:
            # Extract simple and nested values using inline traversal
            title = ""
            if field_map.get("title"):
                obj = entry
                for k in field_map["title"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                title = obj or ""

            url = None
            if field_map.get("url"):
                obj = entry
                for k in field_map["url"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                url = obj

            content = ""
            if field_map.get("content"):
                obj = entry
                for k in field_map["content"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                content = obj or ""

            summary = ""
            if field_map.get("summary"):
                obj = entry
                for k in field_map["summary"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                summary = obj or ""
            if not summary:
                summary = (content[:300] + "...") if len(content) > 300 else content

            author = ""
            if field_map.get("author"):
                obj = entry
                for k in field_map["author"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                author = obj or ""

            published = None
            if field_map.get("published_date"):
                obj = entry
                for k in field_map["published_date"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                pub_raw = obj
                if isinstance(pub_raw, str):
                    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
                        try:
                            published = datetime.strptime(pub_raw, fmt)
                            break
                        except Exception:
                            continue
                    if published and timezone.is_naive(published):
                        published = timezone.make_aware(published)

            categories = []
            if field_map.get("categories"):
                obj = entry
                for k in field_map["categories"].split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                if obj:
                    if isinstance(obj, list):
                        categories = [str(x).strip() for x in obj if x]
                    elif isinstance(obj, str):
                        if "," in obj:
                            categories = [c.strip() for c in obj.split(",") if c.strip()]
                        else:
                            categories = [obj.strip()]

            if not url:
                continue

            if start_date and published and published < start_date:
                continue
            if end_date and published and published > end_date:
                continue
            if last_fetched and not start_date and published and published <= last_fetched:
                continue

            results.append({
                "url": url,
                "title": str(title).strip(),
                "summary": str(summary).strip(),
                "content": str(content).strip(),
                "authors": [str(author).strip()] if author else [],
                "categories": categories,
                "meta_keywords": categories,
                "published": published,
            })

        except Exception as e:
            print(f"[parse_json_feed] Skipped entry: {e}")
            continue

    return results
