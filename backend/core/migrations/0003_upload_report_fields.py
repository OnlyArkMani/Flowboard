from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0002_jobrun_details"),
    ]

    operations = [
        migrations.AddField(
            model_name="upload",
            name="report_generated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="upload",
            name="report_path",
            field=models.CharField(blank=True, default="", max_length=500),
        ),
    ]
