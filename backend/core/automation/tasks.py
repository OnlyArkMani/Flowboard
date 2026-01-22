from __future__ import annotations

import csv
import logging
import os
from datetime import timedelta, date

from django.conf import settings
from django.utils import timezone
from django.db import transaction

from ..models import Upload, Incident, JobRun, DepartmentSource, DepartmentRecord

logger = logging.getLogger("core.automation")


def _current_local_date() -> date:
    """
    Return a timezone-aware local date even if USE_TZ=False in this worker.
    Falls back to a naive date() when localtime cannot be applied.
    """
    now = timezone.now()
    if timezone.is_naive(now):
        return now.date()
    return timezone.localtime(now).date()


def _format_summary(**metrics) -> str:
    return ", ".join(f"{k}={v}" for k, v in metrics.items())


def _resolve_department_source(department: str) -> DepartmentSource | None:
    if not department:
        return None
    return (
        DepartmentSource.objects.filter(code__iexact=department).first()
        or DepartmentSource.objects.filter(name__iexact=department).first()
    )


def _ingest_source(source: DepartmentSource, limit: int = 250) -> tuple[int, str]:
    records = list(DepartmentRecord.objects.filter(source=source).order_by("-recorded_at")[:limit])
    if not records:
        return 0, f"No records available for {source.name}."

    timestamp = timezone.now()
    filename = f"{source.code.lower()}-ingest-{timestamp.strftime('%Y%m%d-%H%M')}.csv"
    upload = Upload.objects.create(
        department=source.name,
        filename=filename,
        mime_type="text/csv",
        status="processing",
        notes="Automated department ingest",
        process_mode="transform_gradebook",
        process_config={"source": source.code, "source_name": source.name},
    )

    upload_dir = getattr(settings, "UPLOAD_DIR", "/app/storage/uploads")
    target_dir = os.path.join(upload_dir, str(upload.upload_id))
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, filename)

    columns = [
        "student_id",
        "student_name",
        "class",
        "score",
        "attendance_percent",
        "status",
        "recorded_at",
    ]
    with open(file_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in records:
            writer.writerow(
                {
                    "student_id": row.student_id,
                    "student_name": row.student_name,
                    "class": row.class_name,
                    "score": row.score if row.score is not None else "",
                    "attendance_percent": row.attendance_percent if row.attendance_percent is not None else "",
                    "status": row.status,
                    "recorded_at": row.recorded_at.isoformat() if row.recorded_at else "",
                }
            )

    upload.file_path = file_path
    upload.save(update_fields=["file_path"])

    source.last_ingested_at = timestamp
    source.save(update_fields=["last_ingested_at"])

    from ..queues import default_queue
    from ..workers import job_chain_standardize

    default_queue.enqueue(job_chain_standardize, str(upload.upload_id))
    return len(records), f"Ingested {len(records)} records from {source.name} and started processing."


def send_attendance_reminders(target_grade: str | None = None) -> str:
    today = _current_local_date()
    pending_uploads = Upload.objects.filter(received_at__date=today, status__in=["pending", "processing"])
    scope = pending_uploads
    if target_grade:
        scope = scope.filter(department__iexact=target_grade)
    count = scope.count()
    message = f"Queued attendance reminders for {count} cohort(s) on {today}"
    logger.info(message)
    return message


def send_system_status_digest() -> str:
    now = timezone.now()
    incidents_open = Incident.objects.filter(state__in=["open", "in_progress"]).count()
    uploads_today = Upload.objects.filter(received_at__date=now.date()).count()
    latest_run = JobRun.objects.order_by("-started_at").first()
    payload = _format_summary(
        timestamp=str(now),
        open_incidents=incidents_open,
        todays_uploads=uploads_today,
        last_run=str(latest_run.run_id if latest_run else "—"),
    )
    logger.info("System status digest: %s", payload)
    return payload


def run_web_scrape(target: str = "admissions_portal") -> str:
    # Placeholder for real scraping logic
    message = f"Scraped latest data source for {target} at {timezone.now().isoformat()}"
    logger.info(message)
    return message


def schedule_file_ingest(department: str = "General") -> str:
    source = _resolve_department_source(department)
    if not source:
        message = f"No department source found for {department}."
        logger.warning(message)
        return message

    count, message = _ingest_source(source)
    logger.info(message)
    return message


def schedule_all_department_ingest() -> str:
    sources = list(DepartmentSource.objects.filter(active=True).order_by("name"))
    if not sources:
        message = "No active department sources to ingest."
        logger.warning(message)
        return message

    timestamp = timezone.now()
    filename = f"all-departments-ingest-{timestamp.strftime('%Y%m%d-%H%M')}.csv"
    upload = Upload.objects.create(
        department="All Departments",
        filename=filename,
        mime_type="text/csv",
        status="processing",
        notes="Automated all-departments ingest",
        process_mode="transform_gradebook",
        process_config={
            "source": "ALL",
            "source_names": [src.name for src in sources],
            "per_source_limit": 250,
        },
    )

    upload_dir = getattr(settings, "UPLOAD_DIR", "/app/storage/uploads")
    target_dir = os.path.join(upload_dir, str(upload.upload_id))
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, filename)

    columns = [
        "department",
        "student_id",
        "student_name",
        "class",
        "score",
        "attendance_percent",
        "status",
        "recorded_at",
    ]

    total_records = 0
    failures = []
    with open(file_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for source in sources:
            records = list(DepartmentRecord.objects.filter(source=source).order_by("-recorded_at")[:250])
            if not records:
                failures.append(f"{source.name}: no records")
                continue
            for row in records:
                writer.writerow(
                    {
                        "department": source.name,
                        "student_id": row.student_id,
                        "student_name": row.student_name,
                        "class": row.class_name,
                        "score": row.score if row.score is not None else "",
                        "attendance_percent": row.attendance_percent if row.attendance_percent is not None else "",
                        "status": row.status,
                        "recorded_at": row.recorded_at.isoformat() if row.recorded_at else "",
                    }
                )
                total_records += 1
            source.last_ingested_at = timestamp
            source.save(update_fields=["last_ingested_at"])

    upload.file_path = file_path
    upload.save(update_fields=["file_path"])

    from ..queues import default_queue
    from ..workers import job_chain_standardize

    default_queue.enqueue(job_chain_standardize, str(upload.upload_id))
    summary = f"All departments ingest started ({len(sources)} sources, {total_records} records)."
    if failures:
        summary = f"{summary} Issues: {', '.join(failures)}"
    logger.info(summary)
    return summary


def purge_old_records(days: int = 90) -> str:
    threshold = timezone.now() - timedelta(days=days)
    with transaction.atomic():
        runs_deleted, _ = JobRun.objects.filter(finished_at__lt=threshold).delete()
        incidents_deleted, _ = Incident.objects.filter(created_at__lt=threshold).delete()
    message = f"Purged {runs_deleted} job runs and {incidents_deleted} incidents older than {days} days."
    logger.info(message)
    return message


def run_daily_backup() -> str:
    # Placeholder: In real deployment, hook into filesystem/DB backup script
    timestamp = timezone.now().isoformat()
    message = f"Daily backup simulated at {timestamp}"
    logger.info(message)
    return message
