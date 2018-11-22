# -*- coding: utf-8 -*-
# Generated by Django 1.11.16 on 2018-11-22 17:19
from __future__ import unicode_literals

from django.db import migrations, models
import taskq.models


class Migration(migrations.Migration):

    dependencies = [
        ('taskq', '0004_modify_max_retries_default'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='status',
            field=models.IntegerField(choices=[(0, 'Queued'), (1, 'Running'), (2, 'Success'), (3, 'Failed'), (4, 'Canceled')], default=0),
        ),
        migrations.AlterField(
            model_name='task',
            name='uuid',
            field=models.CharField(default=taskq.models.generate_task_uuid, editable=False, max_length=36, unique=True),
        ),
        migrations.AlterField(
            model_name='task',
            name='function_name',
            field=models.CharField(default=None, max_length=255),
        ),
        migrations.AlterModelTable(
            name='task',
            table=None,
        ),
    ]