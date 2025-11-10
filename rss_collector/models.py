from django.db import models

# Create your models here.
class Feed(models.Model):
    name = models.CharField(max_length=255)
    url = models.URLField(unique=True)
    last_fetched = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.name

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