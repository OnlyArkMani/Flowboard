# backend/core/metrics.py
from __future__ import annotations

from collections import Counter
from typing import Optional
from django.utils import timezone

_JOB = Counter()       # keys: (job_name, status)
_INCIDENT = Counter()  # keys: (state)

def record_job_metric(job_name: str, status: str, duration_ms: int = 0) -> None:
    # Keep it simple: count runs by (job, status).
    _JOB[(job_name or "unknown", status or "unknown")] += 1

def record_incident_metric(state: str) -> None:
    _INCIDENT[(state or "unknown",)] += 1

def get_metrics_data() -> str:
    # Prometheus text format
    lines = []
    lines.append("# HELP flowboard_job_runs_total Total job runs by job and status")
    lines.append("# TYPE flowboard_job_runs_total counter")
    for (job_name, status), value in sorted(_JOB.items()):
        lines.append(f'flowboard_job_runs_total{{job="{job_name}",status="{status}"}} {value}')

    lines.append("# HELP flowboard_incidents_total Total incidents by state")
    lines.append("# TYPE flowboard_incidents_total counter")
    for (state,), value in sorted(_INCIDENT.items()):
        lines.append(f'flowboard_incidents_total{{state="{state}"}} {value}')

    lines.append(f'# HELP flowboard_build_info Build info')
    lines.append(f'# TYPE flowboard_build_info gauge')
    lines.append(f'flowboard_build_info{{ts="{timezone.now().isoformat()}"}} 1')

    return "\n".join(lines) + "\n"
