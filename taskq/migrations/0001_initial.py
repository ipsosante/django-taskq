# -*- coding: utf-8 -*-

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Task',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('uuid', models.CharField(max_length=36, unique=True)),
                ('name', models.CharField(blank=True, default='', max_length=255)),
                ('function_name', models.CharField(blank=True, default='', max_length=255)),
                ('function_args', models.TextField(blank=True, default='')),
                ('due_at', models.DateTimeField()),
                ('status', models.IntegerField(default=0)),
                ('retries', models.IntegerField(default=0)),
                ('max_retries', models.IntegerField(default=None)),
            ],
            options={
                'db_table': 'tasks_tasks',
            },
        ),
    ]
