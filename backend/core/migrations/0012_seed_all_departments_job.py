from django.db import migrations


def seed_all_departments_job(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    Job.objects.update_or_create(
        name="weekly_ingest_all_departments",
        defaults={
            "job_type": "python",
            "config": {
                "callable": "core.automation.tasks.schedule_all_department_ingest",
                "args": [],
                "kwargs": {},
            },
            "schedule_cron": "30 6 * * 1",
        },
    )


def remove_all_departments_job(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    Job.objects.filter(name="weekly_ingest_all_departments").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0011_seed_department_ingest_jobs"),
    ]

    operations = [
        migrations.RunPython(seed_all_departments_job, remove_all_departments_job),
    ]
