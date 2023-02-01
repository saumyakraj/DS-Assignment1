# Generated by Django 4.1.5 on 2023-02-01 19:07

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Topic',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='Producer',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('topic_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='dis_queue.topic')),
            ],
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('message_content', models.CharField(max_length=255)),
                ('producer_client', models.CharField(max_length=255)),
                ('topic_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='dis_queue.topic')),
            ],
        ),
        migrations.CreateModel(
            name='Consumer',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('offset', models.IntegerField()),
                ('topic_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='dis_queue.topic')),
            ],
        ),
    ]