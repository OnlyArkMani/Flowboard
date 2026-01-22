from django.db import migrations

DEFAULT_JOBS = [
    {
        "name": "attendance_reminders",
        "callable": "core.automation.tasks.send_attendance_reminders",
        "args": [],
        "schedule": "0 7 * * 1-5",
    },
    {
        "name": "system_status_digest",
        "callable": "core.automation.tasks.send_system_status_digest",
        "args": [],
        "schedule": "0 8 * * *",
    },
    {
        "name": "web_scrape_portal",
        "callable": "core.automation.tasks.run_web_scrape",
        "args": ["admissions_portal"],
        "schedule": "*/30 * * * *",
    },
    {
        "name": "nightly_ingest_science",
        "callable": "core.automation.tasks.schedule_file_ingest",
        "args": ["Science"],
        "schedule": "30 2 * * *",
    },
    {
        "name": "weekly_cleanup",
        "callable": "core.automation.tasks.purge_old_records",
        "args": [90],
        "schedule": "0 3 * * 0",
    },
    {
        "name": "daily_backup",
        "callable": "core.automation.tasks.run_daily_backup",
        "args": [],
        "schedule": "0 1 * * *",
    },
]


def seed_jobs(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    for cfg in DEFAULT_JOBS:
        Job.objects.update_or_create(
            name=cfg["name"],
            defaults={
                "job_type": "python",
                "config": {"callable": cfg["callable"], "args": cfg["args"], "kwargs": {}},
                "schedule_cron": cfg["schedule"],
            },
        )


def remove_jobs(apps, schema_editor):
    Job = apps.get_model("core", "Job")
    Job.objects.filter(name__in=[cfg["name"] for cfg in DEFAULT_JOBS]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0005_incident_workflow_fields"),
    ]

    operations = [
        migrations.RunPython(seed_jobs, remove_jobs),
    ]
