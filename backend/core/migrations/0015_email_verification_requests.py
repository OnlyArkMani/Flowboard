from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0014_password_reset_requests"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="email_verified",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="EmailVerificationRequest",
            fields=[
                ("request_id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("code", models.CharField(max_length=12)),
                ("expires_at", models.DateTimeField()),
                ("used_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="email_verifications", to="core.user")),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="emailverificationrequest",
            index=models.Index(fields=["user", "-created_at"], name="core_emailv_user_id_3c2b2d_idx"),
        ),
        migrations.AddIndex(
            model_name="emailverificationrequest",
            index=models.Index(fields=["code"], name="core_emailv_code_53a3b4_idx"),
        ),
    ]
