import feedparser
from newspaper import Article as NewspaperArticle
from datetime import datetime
import time
from django.utils import timezone
import requests
import re
from bs4 import BeautifulSoup
from markdownify import markdownify as html_to_md

def clean_to_markdown(content: str) -> str:
    """
    Converts HTML or plain text into clean Markdown format.
    Ensures consistent storage.
    """
    if not content:
        return ""
    try:
        return html_to_md(content, strip=["script", "style"]).strip()
    except Exception:
        # fallback if content is already plain text
        return content.strip()

def parse_feed(feed, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    if not feed or not feed.url:
        raise ValueError("Invalid Feed object: missing URL")

    parser_type = getattr(feed, "parser_type", "generic").lower().strip()

    if parser_type == "generic":
        return parse_generic_feed(feed, last_fetched, max_entries, start_date, end_date)
    elif parser_type == 'json_feed':
        return parse_json_feed(feed, last_fetched, max_entries, start_date, end_date)
    elif parser_type == "article":
        return parse_html_feed(feed, last_fetched, max_entries, start_date, end_date)
    else:
        raise ValueError(f"Unknown parser type: {parser_type}")

def parse_generic_feed(feed, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    date_format = feed.date_format or "%a, %d %b %Y %H:%M:%S %z"
    extract_full_content = getattr(feed, "extract_full_content", False)

    try:
        response = requests.get(feed.url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        xml_text = response.text
    except Exception as e:
        print(f"[parse_generic_feed] Failed to fetch {feed.url}: {e}")
        return []

    soup = BeautifulSoup(xml_text, "xml")
    entries = soup.find_all("item") or soup.find_all("entry")
    if not entries:
        print(f"[parse_generic_feed] No <item> tags found in {feed.url}")
        return []

    if max_entries:
        entries = entries[:max_entries]

    results = []

    url_tag = feed.url_field
    title_tag = feed.title_field
    desc_tag = getattr(feed, "description_field", None) 
    content_tag = feed.content_field 
    author_tag = feed.author_field
    published_tag = feed.published_field
    categories_tag = feed.categories_field
    for entry in entries:
        try:
            def get_text(tag_name):
                tag = entry.find(tag_name)
                return tag.get_text(strip=True) if tag else ""
            
            url = get_text(url_tag)
            title = get_text(title_tag)
            description = get_text(desc_tag)
            author = get_text(author_tag)
            content = get_text(content_tag) or description

            categories = []
            for cat_tag in entry.find_all(categories_tag):
                text = cat_tag.get_text(strip=True)
                if text:
                    categories.append(text)
            
            published = None
            pub_raw = get_text(published_tag)
            if pub_raw:
                pub_raw = pub_raw.strip()
                try:
                    published = datetime.strptime(pub_raw, date_format)
                    if timezone.is_naive(published):
                        published = timezone.make_aware(published)
                except Exception:
                    pass
            
            if start_date and published and published < start_date:
                continue
            if end_date and published and published > end_date:
                continue
            if last_fetched and not start_date and published and published <= last_fetched:
                continue

            meta_keywords = []
            authors = [author] if author else []

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
            
            content = clean_to_markdown(content)

            results.append({
                "url": url,
                "title": title,
                "summary": description,
                "content": content,
                "authors": authors,
                "categories": categories,
                "meta_keywords": meta_keywords,
                "published": published,
            })
        except Exception as e:
            print(f"[parse_generic_feed] Skipped entry: {e}")
            continue

    return results

def parse_html_feed(feed, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    date_format = feed.date_format or "%Y-%m-%dT%H:%M:%S%z"

    try:
        response = requests.get(feed.url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        html_text = response.text
    except Exception as e:
        print(f"[parse_html_feed] Failed to fetch {feed.url}: {e}")
        return []
    
    soup = BeautifulSoup(html_text, "html.parser")

    article_selector = getattr(feed, "article_selector", None)
    if not article_selector:
        print(f"[parse_html_feed] Missing 'article_selector' for {feed.url}")
        return []

    article_blocks = soup.select(article_selector)
    if not article_blocks:
        print(f"[parse_html_feed] No article blocks found for {feed.url}")
        return []
    
    if max_entries:
        article_blocks = article_blocks[:max_entries]

    results = []

    url_selector = getattr(feed, "url_field", None)
    title_selector = getattr(feed, "title_field", None)
    content_selector = getattr(feed, "content_field", None)
    author_selector = getattr(feed, "author_field", None)
    published_selector = getattr(feed, "published_field", None)
    categories_selector = getattr(feed, "categories_field", None)

    for block in article_blocks:
        try:
            url = ""
            if url_selector:
                el = block.select_one(url_selector)
                if el:
                    url = el.get("href") or el.get("content") or el.get_text(strip=True)

            title = ""
            if title_selector:
                el = block.select_one(title_selector)
                if el:
                    title = el.get("content") or el.get_text(strip=True)
            
            author = ""
            if author_selector:
                el = block.select_one(author_selector)
                if el:
                    author = el.get("content") or el.get_text(strip=True)
            
            content = ""
            if content_selector:
                content_parts = []
                for el in block.select(content_selector):
                    text = el.get_text(strip=True)
                    if text:
                        content_parts.append(text)
                content = "\n".join(content_parts).strip()
            
            categories = []
            if categories_selector:
                cat_els = block.select(categories_selector)
                if cat_els:
                    categories = list({
                        c.get_text(strip=True)
                        for c in cat_els if c.get_text(strip=True)
                    })
            
            published = None
            if published_selector:
                el = block.select_one(published_selector)
                if el:
                    pub_raw = el.get("content") or el.get_text(strip=True)
                    if pub_raw:
                        try:
                            published = datetime.strptime(pub_raw, date_format)
                            if timezone.is_naive(published):
                                published = timezone.make_aware(published)
                        except Exception:
                            pass
            
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
                "meta_keywords": categories,
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
    date_format = api_config.get("date_format") or "%a, %d %b %Y %H:%M:%S %Z"

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
                    try:
                        published = datetime.strptime(pub_raw, date_format)
                    except Exception:
                        print(f"[parse_json_feed] Failed to parse date '{pub_raw}' using '{date_format}'")

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
