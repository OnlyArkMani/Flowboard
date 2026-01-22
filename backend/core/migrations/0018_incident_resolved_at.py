from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0017_rename_core_emailv_user_id_3c2b2d_idx_core_emailv_user_id_63ceb9_idx_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="incident",
            name="resolved_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
