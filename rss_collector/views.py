from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from .services import fetch_all_feeds, fetch_custom_feeds
from .utils import parse_feed
from .models import Article, Feed
from datetime import datetime

@api_view(['POST'])
def fetch_feeds_view(request):
    """
    Manually trigger RSS feed fetching.
    JSON:
        {
            "urls": [
                "http://feeds.bbci.co.uk/news/rss.xml",
                "https://www.theverge.com/rss/index.xml"
            ],
            "limit": 5,
            "max_entries": 3,
            "start_date": "2025-11-01",
            "end_date": "2025-11-05"
        }
    """
    urls = request.data.get('urls', [])
    limit = request.data.get('limit')
    max_entries = request.data.get('max_entries')
    start_date = request.data.get('start_date')
    end_date = request.data.get('end_date')

    if urls and not isinstance(urls, list):
        return Response({"error": "urls must be a list of RSS feed URLs"}, status=status.HTTP_400_BAD_REQUEST)


    try:
        limit = int(limit) if limit else None
        max_entries = int(max_entries) if max_entries else None
    except ValueError:
        return Response({"error": "limit and max must be integers"}, status=status.HTTP_400_BAD_REQUEST)
    
    for date_str in [start_date, end_date]:
        if date_str:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                return Response(
                    {"error": "Dates must be in YYYY-MM-DD format."},
                    status=status.HTTP_400_BAD_REQUEST
                )

    if urls:
        result = fetch_custom_feeds(urls, max_entries, start_date, end_date)
    else:
        result = fetch_all_feeds(limit, max_entries, start_date, end_date)
    return Response({
        "message": "Feed fetching completed.",
        "summary": result
    }, status=status.HTTP_200_OK)


@api_view(['GET'])
def stored_articles_view(request):

    articles = Article.objects.select_related('feed').order_by('-published_at', '-id')

    data = [
        {
            "id": a.id,
            "title": a.title,
            "url": a.url,
            "published_at": a.published_at.isoformat() if a.published_at else None,
            "content": a.content[:300] + "..." if len(a.content) > 300 else a.content,
            "feed_name": a.feed.name if a.feed else None,
            "feed_url": a.feed.url if a.feed else None,
            "created_at": a.created_at.isoformat(),
        }
        for a in articles
    ]

    return Response({
        "count": len(data),
        "articles": data
    }, status=status.HTTP_200_OK)


# for testing purpose only
@api_view(['GET'])
def test_feed_parser(request):
    """
    Example: /api/test-feed/?url=http://feeds.bbci.co.uk/news/rss.xml&max=3
    """
    feed_url = request.query_params.get('url')
    max_entries = request.query_params.get('max')

    if not feed_url:
        return Response({"error": "Missing 'url' query parameter."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        max_entries = int(max_entries) if max_entries else None
        articles = parse_feed(feed_url, max_entries=max_entries)
        # Limit content length for easier viewing
        for a in articles:
            a['content'] = a['content'][:200] + "..." if len(a['content']) > 200 else a['content']
        return Response({
            "feed_url": feed_url,
            "total_articles": len(articles),
            "articles": articles
        }, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#for testing purpose only
@api_view(['POST'])
def reset_last_fetched(request):
    """
    Sets last_fetched = None for all Feed records.
    Use only for admin/debugging purposes.
    """
    updated_count = Feed.objects.update(last_fetched=None)
    return Response({
        "status": "success",
        "message": f"Reset last_fetched for {updated_count} feeds."
    }, status=status.HTTP_200_OK)