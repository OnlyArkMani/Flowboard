from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0008_upload_report_pdf"),
    ]

    operations = [
        migrations.CreateModel(
            name="DepartmentSource",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120, unique=True)),
                ("code", models.CharField(max_length=50, unique=True)),
                ("description", models.TextField(blank=True, default="")),
                ("schedule_hint", models.CharField(blank=True, default="", max_length=120)),
                ("active", models.BooleanField(default=True)),
                ("last_ingested_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="DepartmentRecord",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("student_id", models.CharField(max_length=40)),
                ("student_name", models.CharField(max_length=120)),
                ("class_name", models.CharField(blank=True, default="", max_length=50)),
                ("score", models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True)),
                ("attendance_percent", models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ("status", models.CharField(blank=True, default="", max_length=40)),
                ("recorded_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("notes", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "source",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="records", to="core.departmentsource"),
                ),
            ],
            options={
                "ordering": ["-recorded_at"],
            },
        ),
        migrations.AddIndex(
            model_name="departmentrecord",
            index=models.Index(fields=["source", "-recorded_at"], name="core_deprecord_source_recorded_idx"),
        ),
        migrations.AddIndex(
            model_name="departmentrecord",
            index=models.Index(fields=["student_id"], name="core_deprecord_student_idx"),
        ),
    ]
