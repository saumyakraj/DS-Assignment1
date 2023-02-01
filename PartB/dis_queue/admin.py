from django.contrib import admin
from .models import Topic, Producer, Consumer, Message

# Register your models here.
admin.site.register(Topic)
admin.site.register(Producer)
admin.site.register(Consumer)
admin.site.register(Message)
