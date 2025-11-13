from newspaper import Article as NewspaperArticle
from datetime import datetime
from django.utils import timezone
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as html_to_md

def clean_to_markdown(content):
    """
    Converts HTML or plain text into Markdown.
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
    
    #not currently in use
    elif parser_type == 'json_feed':
        return parse_json_feed(feed, last_fetched, max_entries, start_date, end_date)
    
    #not currently in use
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
    desc_tag = feed.description_field
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
    extract_full_content = getattr(feed, "extract_full_content", False)

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

    blocks = soup.select(article_selector)
    if not blocks:
        print(f"[parse_html_feed] No article blocks found for {feed.url}")
        return []
    
    if max_entries:
        blocks = blocks[:max_entries]

    results = []

    url_sel = feed.url_field
    title_sel = feed.title_field
    desc_sel = feed.description_field
    content_sel = feed.content_field
    author_sel = feed.author_field
    published_sel = feed.published_field
    categories_sel = feed.categories_field

    for block in blocks:
        try:
            url = ""
            if url_sel:
                el = block.select_one(url_sel)
                if el:
                    url = el.get("href") or el.get("content") or el.get_text(strip=True)

            if not url:
                continue

            title = ""
            if title_sel:
                el = block.select_one(title_sel)
                if el:
                    title = el.get("content") or el.get_text(strip=True)
            
            description = ""
            if desc_sel:
                el = block.select_one(desc_sel)
                if el:
                    description = el.get_text(strip=True)
            
            content = ""
            if content_sel:
                parts = []
                for el in block.select(content_sel):
                    txt = el.get_text(strip=True)
                    if txt:
                        parts.append(txt)
                content = "\n".join(parts).strip()

            if not content:
                content = description
            
            author = ""
            if author_sel:
                el = block.select_one(author_sel)
                if el:
                    author = el.get("content") or el.get_text(strip=True)

            authors = [author] if author else []

            categories = []
            if categories_sel:
                for c in block.select(categories_sel):
                    text = c.get_text(strip=True)
                    if text:
                        categories.append(text)
            
            published = None
            if published_sel:
                el = block.select_one(published_sel)
                if el:
                    raw = el.get("content") or el.get_text(strip=True)
                    if raw:
                        try:
                            published = datetime.strptime(raw.strip(), date_format)
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
            if extract_full_content:
                try:
                    art = NewspaperArticle(url)
                    art.download()
                    art.parse()

                    if art.text and len(art.text) > len(content):
                        content = art.text.strip()

                    if art.authors:
                        authors = art.authors

                    try:
                        art.nlp()
                        meta_keywords = art.keywords or []
                    except Exception:
                        pass

                except Exception:
                    pass
            
            content = clean_to_markdown(content)

            summary = description or content[:300] + ("..." if len(content) > 300 else "")

            results.append({
                "url": url,
                "title": title,
                "summary": summary,
                "content": content,
                "authors": authors,
                "categories": categories,
                "meta_keywords": meta_keywords or categories,
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
