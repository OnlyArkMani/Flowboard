from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0007_upload_processing_plan"),
    ]

    operations = [
        migrations.AddField(
            model_name="upload",
            name="report_pdf",
            field=models.TextField(blank=True, default=""),
        ),
    ]
