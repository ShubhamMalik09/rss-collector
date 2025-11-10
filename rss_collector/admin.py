from django.contrib import admin
from .models import Feed, Article

# Register your models here.
@admin.register(Feed)
class FeedAdmin(admin.ModelAdmin):
    list_display = ("name", "url", "last_fetched")
    search_fields = ("name", "url")


@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):
    list_display = ("title", "feed", "published_at", "created_at")
    list_filter = ("feed",)
    search_fields = ("title", "content", "feed__name")
