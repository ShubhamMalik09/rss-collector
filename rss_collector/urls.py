from django.urls import path
from .views import stored_articles_view, test_feed_parser, fetch_feeds_view

urlpatterns = [
    path('sarticles/', stored_articles_view, name='stored-articles'),
    path('test-feed/', test_feed_parser, name='test-feed'),
    path('fetch-feeds/', fetch_feeds_view, name='fetch-feeds'),
]