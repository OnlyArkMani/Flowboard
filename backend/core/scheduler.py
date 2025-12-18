from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .queues import default_scheduler, default_queue

if TYPE_CHECKING:
  from .models import Job


def _schedule_identifier(job_id: int) -> str:
  return f"job:{job_id}"


def enqueue_job_execution(job_id: int, payload: Optional[dict] = None) -> None:
  from .workers import run_custom_job

  default_queue.enqueue(run_custom_job, job_id, payload)


def register_cron_schedule(job: "Job") -> None:
  identifier = _schedule_identifier(job.id)
  cancel_cron_schedule(job.id)
  cron_expr = (job.schedule_cron or "").strip()
  if not cron_expr:
    return
  default_scheduler.cron(
      cron_expr,
      func=enqueue_job_execution,
      args=[job.id, None],
      id=identifier,
      repeat=None,
  )


def cancel_cron_schedule(job_id: int) -> None:
  identifier = _schedule_identifier(job_id)
  try:
    default_scheduler.cancel(identifier)
  except ValueError:
    return


def enqueue_job_now(job: "Job", payload: Optional[dict] = None) -> None:
  enqueue_job_execution(job.id, payload)
