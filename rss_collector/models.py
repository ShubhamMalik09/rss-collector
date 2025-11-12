from django.db import models
from datetime import timedelta, datetime, timezone

# Create your models here.
class Feed(models.Model):
    name = models.CharField(max_length=255)
    url = models.URLField(unique=True)
    last_fetched = models.DateTimeField(null=True, blank=True)
    next_fetch = models.DateTimeField(null=True, blank=True)

    url_field = models.CharField(max_length=100, default="link")
    title_field = models.CharField(max_length=100, default="title")
    description_field = models.CharField(max_length=100, default="description")
    content_field = models.CharField(max_length=100, default="content:encoded")
    author_field = models.CharField(max_length=100, default="dc:creator")
    published_field = models.CharField(max_length=100, default="pubDate")
    categories_field = models.CharField(max_length=100, default="category")
    date_format = models.CharField(max_length=100,default="%a, %d %b %Y %H:%M:%S %z")

    parser_type = models.CharField(
        max_length=50,
        choices=[
            ('generic', 'Generic Parser')
        ],
        default='generic'
    )

    call_frequency = models.PositiveIntegerField(default=10) 
    extract_full_content = models.BooleanField(default=False)

    def __str__(self):
        return self.name
    
    def schedule_next_fetch(self):
        """Set next_fetch based on refresh interval."""
        self.next_fetch = timezone.now() + timedelta(minutes=self.refresh_interval)
        self.save(update_fields=["next_fetch"])

class Article(models.Model):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE, related_name='articles')
    title = models.TextField()
    url = models.URLField(unique=True)  # ensures no duplicate article links
    content = models.TextField()

    authors = models.JSONField(default=list, blank=True)
    categories = models.JSONField(default=list, blank=True)      
    meta_keywords = models.JSONField(default=list, blank=True)    

    published_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [models.Index(fields=['-published_at'])]
        ordering = ['-published_at']

    def __str__(self):
        return self.title[:80]