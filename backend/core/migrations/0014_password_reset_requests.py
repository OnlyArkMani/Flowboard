from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0013_merge_0012_branches"),
    ]

    operations = [
        migrations.CreateModel(
            name="PasswordResetRequest",
            fields=[
                ("request_id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("code", models.CharField(max_length=12)),
                ("expires_at", models.DateTimeField()),
                ("used_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="password_resets", to="core.user")),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="passwordresetrequest",
            index=models.Index(fields=["user", "-created_at"], name="core_passwo_user_id_d33f1f_idx"),
        ),
        migrations.AddIndex(
            model_name="passwordresetrequest",
            index=models.Index(fields=["code"], name="core_passwo_code_7f8d9a_idx"),
        ),
    ]
