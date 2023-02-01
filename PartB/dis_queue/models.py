from django.db import models

class Banks(models.Model):
    # id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=122, blank= False)
    main_address = models.TextField(blank=False)
    bank_id = models.BigIntegerField(default = datetime.now().strftime('%y%m%d%f') + str(random.randint(1000,9999)), unique = False, primary_key=True,)
    is_verified = models.BooleanField(default=False )
    owner = models.ForeignKey(to=CustomUser, on_delete=models.CASCADE)

    def __str__(self):
        return self.name

# Create your models here.

class Topic(models.Model):
    id = models.IntegerField(primary_key = True)
    name = models.CharField(max_length=255, nullable=False, unique=True)
    # producers = models.relationship('Producer', backref='topic')
    # consumers = models.relationship('Consumer', backref='topic')
    # messages  = models.relationship('Message', backref='topic')

    def __str__(self):
        return self.id

class Producer(models.Model):
    id = models.IntegerField(primary_key = True)
    topic_id = models.IntegerField(to=Topic)

    def __str__(self):
        return self.id
    
class Consumer(models.Model):
    id = models.IntegerField( primary_key = True)
    topic_id = models.ForeignKey(to= Topic)
    offset = models.IntegerField( nullable=False)

    def __str__(self):
        return self.id


class Message(models.Model):
    id = models.IntegerField(primary_key = True)
    topic_id = models.ForeignKey(to= Topic)
    message_content = models.CharField(max_length=255)
    producer_client = models.CharField(max_length=255)

    def __str__(self):
        return self.id