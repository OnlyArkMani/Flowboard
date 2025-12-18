from __future__ import annotations

import logging
from datetime import timedelta, date

from django.utils import timezone
from django.db import transaction

from ..models import Upload, Incident, JobRun

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
    recent_upload = Upload.objects.filter(department__iexact=department).order_by("-received_at").first()
    info = recent_upload.filename if recent_upload else "no prior upload"
    message = f"Prepared ingest workflow for {department} (reference: {info})"
    logger.info(message)
    return message


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
