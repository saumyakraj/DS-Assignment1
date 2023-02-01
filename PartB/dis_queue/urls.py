from django.urls import path
from django.urls.conf import include
from .views import Topics, ProducerRegister, ConsumerRegister, ProducerProduce, ConsumerConsume, Size

urlpatterns = [
    path('topics/',Topics.as_view(),name = "topics"),
    path('producer/register/', ProducerRegister.as_view(), name = "producer_register"),
    path('consumer/register/', ConsumerRegister.as_view(), name = "consumer_register"),
    path('producer/produce/', ProducerProduce.as_view(), name = "producer_produce"),
    path('consumer/consume/', ConsumerConsume.as_view(), name = "consumer_consume"),
    path('size/', Size.as_view(), name = "size"),
]
