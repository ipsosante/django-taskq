# -*- coding: utf-8 -*-

import datetime
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('taskq', '0006_auto_20190705_0601'),
    ]

    operations = [
        migrations.AddField(
            model_name='task',
            name='timeout',
            field=models.DurationField(default=datetime.timedelta(0)),
        ),
    ]
