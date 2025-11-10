import feedparser
from newspaper import Article as NewspaperArticle
from datetime import datetime
import time
from django.utils import timezone
import requests
import re
from bs4 import BeautifulSoup

def parse_feed(feed_url, last_fetched=None, max_entries=None, start_date=None, end_date=None):

    try:
        xml_text = requests.get(feed_url, timeout=10).text
    except Exception as e:
        print(f"Failed to fetch feed: {feed_url}, error: {e}")
        return []

    parsed = feedparser.parse(feed_url)
    results = []
    entries = parsed.entries or []
    if max_entries:
        entries = entries[:max_entries]
    
    if start_date:
        start_date = timezone.make_aware(datetime.strptime(start_date, "%Y-%m-%d"))
    if end_date:
        end_date = timezone.make_aware(datetime.strptime(end_date, "%Y-%m-%d"))

    item_blocks = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL | re.IGNORECASE)
    pub_dates = []
    for block in item_blocks:
        match = re.search(r"<pubDate>(.*?)</pubDate>", block, re.IGNORECASE)
        pub_dates.append(match.group(1).strip() if match else None)

    for i, entry in enumerate(entries):
        try:
            url = entry.get('link') or entry.get('id')
            if not url:
                continue
            title = (entry.get('title') or '').strip()
            summary = (entry.get('summary') or entry.get('description') or '')
            authors = []
            categories = []

            for key in ["author", "dc_creator", "creator"]:
                if entry.get(key):
                    authors.append(entry.get(key))
                    break
            
            if "tags" in entry:
                categories = [tag.get("term") for tag in entry["tags"] if tag.get("term")]
            elif entry.get("category"):
                categories = [entry.get("category")]
            elif entry.get("dc_subject"):
                categories = [entry.get("dc_subject")]


            published = None
            published_fields = [
                entry.get("published_parsed"),
                entry.get("updated_parsed"),
                entry.get("created_parsed"),
            ]

            for p in published_fields:
                if p:
                    published = datetime(*p[:6])
                    break

            if not published and i < len(pub_dates) and pub_dates[i]:
                try:
                    published = datetime.strptime(pub_dates[i], "%a, %d %b %Y %H:%M:%S %Z")
                except Exception:
                    # Some feeds don't have timezone abbrev — try without it
                    try:
                        published = datetime.strptime(pub_dates[i], "%a, %d %b %Y %H:%M:%S")
                    except Exception:
                        published = None
            
            if published:
                published = timezone.make_aware(published)
            
            if start_date and published and published < start_date:
                continue
            if end_date and published and published > end_date:
                continue

            if last_fetched and not start_date and published and published <= last_fetched:
                continue


            # Extract full text using newspaper3k
            article_authors = authors.copy()
            meta_keywords = []
            try:
                art = NewspaperArticle(url)
                art.download()
                art.parse()
                content = art.text or ''

                try:
                    art.nlp()
                except Exception:
                    pass


                meta_keywords = getattr(art, "keywords", [])

                if not article_authors and getattr(art, "authors", []):
                    article_authors = art.authors
                
                if not meta_keywords:
                    # RSS tag-based keywords or tags
                    meta_keywords = []
                    if "tags" in entry:
                        meta_keywords = [tag.get("term") for tag in entry["tags"] if tag.get("term")]
                    elif entry.get("category"):
                        meta_keywords = [entry.get("category")]
                    elif entry.get("dc_subject"):
                        meta_keywords = [entry.get("dc_subject")]

            except Exception:
                # Fallback if newspaper3k fails
                content = entry.get('summary', '') or entry.get('description', '') or ''
                meta_keywords = []
                article_authors = [authors] if authors else []
            
            if not article_authors or not meta_keywords:
                try:
                    resp = requests.get(url, timeout=8)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, "html.parser")

                        if not article_authors:
                            author_meta = soup.find("meta", attrs={"name": "author"})
                            if author_meta and author_meta.get("content"):
                                article_authors = [author_meta["content"].strip()]
                            else:
                                author_span = soup.find(attrs={"itemprop": "author"})
                                if author_span:
                                    article_authors = [author_span.get_text(strip=True)]

                        if not meta_keywords:
                            keyword_meta = soup.find("meta", attrs={"name": "keywords"})
                            if keyword_meta and keyword_meta.get("content"):
                                meta_keywords = [
                                    kw.strip() for kw in keyword_meta["content"].split(",")
                                ]

                        if not categories:
                            section_meta = soup.find("meta", attrs={"property": "article:section"})
                            if section_meta and section_meta.get("content"):
                                categories = [section_meta["content"].strip()]
                except Exception as e:
                    print(f"[BeautifulSoup fallback failed for {url}] {e}")


            results.append({
                "url": url,
                "title": title,
                "published": published,
                "summary": summary,
                "content": content,
                "authors": article_authors,
                "categories": categories,
                "meta_keywords": meta_keywords,
            })

        except Exception as exc:
            print(f"[parse_feed] error for {feed_url}: {exc}")
            continue
    return results
