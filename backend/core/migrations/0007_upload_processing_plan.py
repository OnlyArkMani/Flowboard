from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0006_seed_default_jobs"),
    ]

    operations = [
        migrations.AddField(
            model_name="upload",
            name="process_mode",
            field=models.CharField(blank=True, default="transform_gradebook", max_length=50),
        ),
        migrations.AddField(
            model_name="upload",
            name="process_config",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
