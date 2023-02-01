from django.db import models

# Create your models here.

class Topic(models.Model):
    id = models.IntegerField(primary_key = True)
    name = models.CharField(max_length=255, blank=False, unique=True)

    def __str__(self):
        return str(self.id)

class Producer(models.Model):
    id = models.IntegerField(primary_key = True)
    topic_id = models.ForeignKey(to= Topic, on_delete=models.CASCADE)

    def __str__(self):
        return str(self.id)
    
class Consumer(models.Model):
    id = models.IntegerField( primary_key = True)
    topic_id = models.ForeignKey(to= Topic, on_delete=models.CASCADE)
    offset = models.IntegerField( blank=False)

    def __str__(self):
        return str(self.id)


class Message(models.Model):
    id = models.IntegerField(primary_key = True)
    topic_id = models.ForeignKey(to= Topic, on_delete=models.CASCADE)
    message_content = models.CharField(max_length=255)
    producer_client = models.CharField(max_length=255)

    def __str__(self):
        return str(self.id)