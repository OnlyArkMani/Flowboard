from decimal import Decimal
from django.db import migrations
from django.utils import timezone


def seed_department_sources(apps, schema_editor):
    DepartmentSource = apps.get_model("core", "DepartmentSource")
    DepartmentRecord = apps.get_model("core", "DepartmentRecord")

    sources = [
        {
            "name": "Admissions",
            "code": "ADM",
            "description": "Weekly admissions intake and entrance test results.",
            "schedule_hint": "Weekly (Mon 06:00)",
        },
        {
            "name": "Attendance",
            "code": "ATT",
            "description": "Weekly attendance rollup per student.",
            "schedule_hint": "Weekly (Mon 07:00)",
        },
        {
            "name": "Fees",
            "code": "FEE",
            "description": "Monthly fee ledger updates.",
            "schedule_hint": "Monthly (1st 05:00)",
        },
        {
            "name": "Library",
            "code": "LIB",
            "description": "Weekly library usage and late returns.",
            "schedule_hint": "Weekly (Tue 08:00)",
        },
        {
            "name": "Examination",
            "code": "EXM",
            "description": "Monthly exam score snapshot.",
            "schedule_hint": "Monthly (1st 09:00)",
        },
        {
            "name": "Science",
            "code": "SCI",
            "description": "Nightly science lab score ingest.",
            "schedule_hint": "Daily (02:30)",
        },
    ]

    source_map = {}
    for src in sources:
        obj, _ = DepartmentSource.objects.update_or_create(
            code=src["code"],
            defaults={
                "name": src["name"],
                "description": src["description"],
                "schedule_hint": src["schedule_hint"],
                "active": True,
            },
        )
        source_map[src["code"]] = obj

    now = timezone.now()
    records = {
        "ADM": [
            {"student_id": "ADM001", "student_name": "Aarav Sharma", "class_name": "10-A", "score": Decimal("86.5"), "attendance_percent": Decimal("98.0"), "status": "verified"},
            {"student_id": "ADM002", "student_name": "Neha Patel", "class_name": "10-B", "score": Decimal("82.0"), "attendance_percent": Decimal("96.5"), "status": "verified"},
            {"student_id": "ADM003", "student_name": "Kabir Singh", "class_name": "9-A", "score": Decimal("79.2"), "attendance_percent": Decimal("94.0"), "status": "pending"},
            {"student_id": "ADM004", "student_name": "Isha Rao", "class_name": "9-B", "score": Decimal("88.1"), "attendance_percent": Decimal("97.0"), "status": "verified"},
        ],
        "ATT": [
            {"student_id": "ATT101", "student_name": "Priya Nair", "class_name": "8-A", "score": Decimal("90.0"), "attendance_percent": Decimal("93.2"), "status": "present"},
            {"student_id": "ATT102", "student_name": "Arjun Das", "class_name": "8-B", "score": Decimal("84.0"), "attendance_percent": Decimal("88.9"), "status": "present"},
            {"student_id": "ATT103", "student_name": "Rohan Mehta", "class_name": "7-A", "score": Decimal("76.0"), "attendance_percent": Decimal("91.0"), "status": "absent"},
            {"student_id": "ATT104", "student_name": "Sara Ali", "class_name": "7-B", "score": Decimal("89.5"), "attendance_percent": Decimal("95.4"), "status": "present"},
        ],
        "FEE": [
            {"student_id": "FEE201", "student_name": "Vikram Joshi", "class_name": "11-A", "score": Decimal("91.0"), "attendance_percent": Decimal("92.0"), "status": "paid"},
            {"student_id": "FEE202", "student_name": "Meera Kapoor", "class_name": "11-B", "score": Decimal("88.0"), "attendance_percent": Decimal("90.5"), "status": "partial"},
            {"student_id": "FEE203", "student_name": "Dev Shah", "class_name": "12-A", "score": Decimal("93.4"), "attendance_percent": Decimal("94.2"), "status": "paid"},
            {"student_id": "FEE204", "student_name": "Anaya Gupta", "class_name": "12-B", "score": Decimal("85.6"), "attendance_percent": Decimal("89.0"), "status": "pending"},
        ],
        "LIB": [
            {"student_id": "LIB301", "student_name": "Ishaan Verma", "class_name": "6-A", "score": Decimal("78.0"), "attendance_percent": Decimal("87.5"), "status": "overdue"},
            {"student_id": "LIB302", "student_name": "Nisha Kumar", "class_name": "6-B", "score": Decimal("80.5"), "attendance_percent": Decimal("90.0"), "status": "returned"},
            {"student_id": "LIB303", "student_name": "Ravi Menon", "class_name": "5-A", "score": Decimal("74.2"), "attendance_percent": Decimal("85.0"), "status": "returned"},
            {"student_id": "LIB304", "student_name": "Tara Bose", "class_name": "5-B", "score": Decimal("81.0"), "attendance_percent": Decimal("88.0"), "status": "overdue"},
        ],
        "EXM": [
            {"student_id": "EXM401", "student_name": "Kiran Rao", "class_name": "10-A", "score": Decimal("92.0"), "attendance_percent": Decimal("96.0"), "status": "pass"},
            {"student_id": "EXM402", "student_name": "Sameer Khan", "class_name": "10-B", "score": Decimal("77.5"), "attendance_percent": Decimal("90.0"), "status": "pass"},
            {"student_id": "EXM403", "student_name": "Anita Roy", "class_name": "9-A", "score": Decimal("69.0"), "attendance_percent": Decimal("88.0"), "status": "review"},
            {"student_id": "EXM404", "student_name": "Ritika Jain", "class_name": "9-B", "score": Decimal("84.7"), "attendance_percent": Decimal("92.5"), "status": "pass"},
        ],
        "SCI": [
            {"student_id": "SCI501", "student_name": "Om Prakash", "class_name": "11-A", "score": Decimal("86.0"), "attendance_percent": Decimal("94.0"), "status": "lab-ok"},
            {"student_id": "SCI502", "student_name": "Diya Sen", "class_name": "11-B", "score": Decimal("91.5"), "attendance_percent": Decimal("97.0"), "status": "lab-ok"},
            {"student_id": "SCI503", "student_name": "Yash Arora", "class_name": "12-A", "score": Decimal("88.2"), "attendance_percent": Decimal("93.0"), "status": "lab-ok"},
            {"student_id": "SCI504", "student_name": "Pooja Iyer", "class_name": "12-B", "score": Decimal("79.8"), "attendance_percent": Decimal("90.0"), "status": "lab-review"},
        ],
    }

    for code, rows in records.items():
        source = source_map.get(code)
        if not source:
            continue
        for row in rows:
            DepartmentRecord.objects.update_or_create(
                source=source,
                student_id=row["student_id"],
                defaults={
                    "student_name": row["student_name"],
                    "class_name": row["class_name"],
                    "score": row["score"],
                    "attendance_percent": row["attendance_percent"],
                    "status": row["status"],
                    "recorded_at": now,
                },
            )


def remove_department_sources(apps, schema_editor):
    DepartmentSource = apps.get_model("core", "DepartmentSource")
    DepartmentRecord = apps.get_model("core", "DepartmentRecord")
    DepartmentRecord.objects.all().delete()
    DepartmentSource.objects.filter(code__in=["ADM", "ATT", "FEE", "LIB", "EXM", "SCI"]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0009_department_sources"),
    ]

    operations = [
        migrations.RunPython(seed_department_sources, remove_department_sources),
    ]
