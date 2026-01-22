from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0003_upload_report_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="upload",
            name="report_csv",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="upload",
            name="report_meta",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
