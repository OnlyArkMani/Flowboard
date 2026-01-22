from django.db import migrations


DEPARTMENT_JOBS = [
    {
        "name": "weekly_ingest_admissions",
        "department": "Admissions",
        "schedule": "0 6 * * 1",
    },
    {
        "name": "weekly_ingest_attendance",
        "department": "Attendance",
        "schedule": "0 7 * * 1",
    },
    {
        "name": "monthly_ingest_fees",
        "department": "Fees",
        "schedule": "0 5 1 * *",
    },
    {
        "name": "weekly_ingest_library",
        "department": "Library",
        "schedule": "0 8 * * 2",
    },
    {
        "name": "monthly_ingest_examination",
        "department": "Examination",
        "schedule": "0 9 1 * *",
    },
]


def seed_department_jobs(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    for cfg in DEPARTMENT_JOBS:
        Job.objects.update_or_create(
            name=cfg["name"],
            defaults={
                "job_type": "python",
                "config": {
                    "callable": "core.automation.tasks.schedule_file_ingest",
                    "args": [cfg["department"]],
                    "kwargs": {},
                },
                "schedule_cron": cfg["schedule"],
            },
        )


def remove_department_jobs(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    Job.objects.filter(name__in=[cfg["name"] for cfg in DEPARTMENT_JOBS]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0010_seed_department_sources"),
    ]

    operations = [
        migrations.RunPython(seed_department_jobs, remove_department_jobs),
    ]
