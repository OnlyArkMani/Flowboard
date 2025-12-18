from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0004_upload_report_storage"),
    ]

    operations = [
        migrations.AddField(
            model_name="incident",
            name="analysis_notes",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="archived_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="auto_retry_count",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="incident",
            name="category",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="detection_source",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="impact_summary",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="max_auto_retries",
            field=models.IntegerField(default=2),
        ),
        migrations.AddField(
            model_name="incident",
            name="resolution_report",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="resolved_by",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="incident",
            name="severity",
            field=models.CharField(
                choices=[("low", "Low"), ("medium", "Medium"), ("high", "High"), ("critical", "Critical")],
                default="medium",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="incident",
            name="timeline",
            field=models.JSONField(blank=True, default=list),
        ),
    ]

