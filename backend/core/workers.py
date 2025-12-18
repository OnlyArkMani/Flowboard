import importlib
import logging
import os
import re
import io
from datetime import timedelta
from typing import Optional

import django
from django.conf import settings
from django.utils import timezone

import pandas as pd

# Ensure Django app registry is loaded when this module is imported by an
# out-of-process RQ worker (started via `rq worker` and not manage.py).
django.setup()

from .models import Upload, Job, JobRun, KnownError, Incident, Ticket
from .metrics import record_job_metric, record_incident_metric
from .queues import default_queue

logger = logging.getLogger(__name__)

PIPELINE_JOB_NAME = "results_pipeline"

PIPELINE = [
    "standardize_results",
    "validate_results",
    "transform_gradebook",
    "generate_summary",
    "publish_results",
]

# Minimal required columns – can be expanded or made configurable per department.
REQUIRED_COLUMNS_DEFAULT = ["student_id", "score"]
REQUIRED_COLUMNS_BY_DEPARTMENT = {
    # Example: override shape for a specific department
    # "Examination": ["student_id", "subject", "score"],
}

DEFAULT_KNOWN_ERRORS = [
    {
        "name": "No columns detected",
        "pattern": "No columns detected",
        "fix": {
            "severity": "high",
            "category": "ingest",
            "root_cause": "The uploaded file has no header row or could not be parsed into columns.",
            "corrective_action": "Ensure the first row contains column names and re-export the file as a well-formed CSV or Excel file.",
            "resolution_report": "Pipeline rejected the file before validation because the schema was empty.",
        },
    },
    {
        "name": "No rows detected",
        "pattern": "No rows detected",
        "fix": {
            "severity": "medium",
            "category": "ingest",
            "root_cause": "The uploaded file is empty or only contains a header row.",
            "corrective_action": "Verify the source system is exporting data and re-upload a file with at least one data row.",
            "resolution_report": "Upload completed but zero records were available for processing.",
        },
    },
    {
        "name": "Required columns missing",
        "pattern": "Required columns missing",
        "fix": {
            "severity": "high",
            "category": "schema",
            "root_cause": "The file schema does not match the expected template for this department.",
            "corrective_action": "Update the export to include all required columns (e.g. student_id, score) and re-upload.",
            "resolution_report": "Schema validation blocked the job until the template is fixed.",
        },
    },
    {
        "name": "Unsupported file type",
        "pattern": "Unsupported file type",
        "fix": {
            "severity": "low",
            "category": "ingest",
            "root_cause": "The file extension is not supported by the pipeline loader.",
            "corrective_action": "Convert the file to CSV, XLSX/XLS or a tabular PDF and try again.",
            "resolution_report": "Rejected because the parser could not infer a loader.",
        },
    },
    {
        "name": "No table found in first PDF page",
        "pattern": "No table found in first PDF page",
        "fix": {
            "severity": "medium",
            "category": "ingest",
            "root_cause": "The PDF does not contain an extractable table on the first page.",
            "corrective_action": "Export the results as a table-based PDF or use CSV/Excel instead.",
            "resolution_report": "PDF extraction returned zero tables.",
        },
    },
    {
        "name": "File not found",
        "pattern": "File not found",
        "fix": {
            "severity": "critical",
            "category": "storage",
            "root_cause": "The on-disk file path for this upload is missing or has been moved.",
            "corrective_action": "Re-upload the original file so the pipeline can access it again.",
            "auto_retry": {"enabled": False},
        },
    },
    {
        "name": "Temporary storage lock",
        "pattern": "(Resource temporarily unavailable|share violation)",
        "fix": {
            "severity": "medium",
            "category": "infrastructure",
            "root_cause": "The storage layer briefly locked the file when the pipeline tried to read it.",
            "corrective_action": "No manual action required unless the issue persists. The engine retries automatically.",
            "auto_retry": {"enabled": True, "max": 2, "delay_seconds": 45},
            "resolution_report": "Storage lock cleared after retry.",
        },
    },
    {
        "name": "Encoding mismatch",
        "pattern": "(UnicodeDecodeError|codec can't decode)",
        "fix": {
            "severity": "high",
            "category": "ingest",
            "root_cause": "The CSV encoding differs from UTF-8.",
            "corrective_action": "Re-export the source file as UTF-8 or specify UTF-8 BOM.",
            "resolution_report": "Parser failed while decoding file contents.",
        },
    },
    {
        "name": "Grade outside range",
        "pattern": "(score must be between|value out of range)",
        "fix": {
            "severity": "medium",
            "category": "validation",
            "root_cause": "One or more numeric fields contain values outside the permitted range.",
            "corrective_action": "Review the highlighted rows and correct the data before re-uploading.",
            "resolution_report": "Validation rejected the payload due to data quality issues.",
        },
    },
    {
        "name": "Duplicate student rows",
        "pattern": "Duplicate rows detected",
        "fix": {
            "severity": "medium",
            "category": "validation",
            "root_cause": "The upload contains duplicate student IDs.",
            "corrective_action": "Deduplicate records in the source file and upload again.",
            "resolution_report": "Encountered duplicate keys while enforcing uniqueness.",
        },
    },
]


def _get_or_create_job(name: str) -> Job:
    job, _ = Job.objects.get_or_create(name=name, defaults={"job_type": "python"})
    return job


def _start_run(job: Job, upload: Upload) -> JobRun:
    return JobRun.objects.create(job=job, upload=upload, status="running", started_at=timezone.now())


def _start_generic_run(job: Job) -> JobRun:
    return JobRun.objects.create(job=job, status="running", started_at=timezone.now())


def _finish_run(run: JobRun, status: str, logs: str = "", exit_code: int = 0) -> None:
    run.status = status
    run.finished_at = timezone.now()
    run.exit_code = exit_code
    run.logs = (logs or "")[:20000]
    if run.started_at and run.finished_at:
        run.duration_ms = int((run.finished_at - run.started_at).total_seconds() * 1000)
    run.save()
    record_job_metric(run.job.name, status, run.duration_ms or 0)


def _ensure_default_known_errors() -> None:
    """
    Seed a small library of KnownError patterns so incidents can be auto-tagged.
    Safe to call many times – uses get_or_create under the hood.
    """
    for cfg in DEFAULT_KNOWN_ERRORS:
        KnownError.objects.get_or_create(
            pattern=cfg["pattern"],
            defaults={
                "name": cfg["name"],
                "fix": cfg.get("fix", {}),
                "examples": cfg.get("examples", []),
                "active": True,
            },
        )


def _match_known_error(error_text: str) -> Optional[KnownError]:
    for ke in KnownError.objects.filter(active=True).order_by("-updated_at"):
        try:
            if re.search(ke.pattern, error_text or "", re.IGNORECASE):
                return ke
        except re.error:
            # bad regex in DB shouldn't crash pipeline
            continue
    return None


def _append_incident_event(incident: Incident, event: str, actor: str = "engine", notes: Optional[str] = None) -> None:
    timeline = list(incident.timeline or [])
    timeline.append(
        {
            "timestamp": timezone.now().isoformat(),
            "event": event,
            "actor": actor,
            "notes": notes,
        }
    )
    incident.timeline = timeline


def _auto_triage_incident(incident: Incident, matched: Optional[KnownError], run: JobRun) -> None:
    if not matched:
        _append_incident_event(incident, "Unknown incident awaiting manual triage")
        incident.save(update_fields=["timeline", "updated_at"])
        return

    fix = matched.fix if isinstance(matched.fix, dict) else {}
    updates = ["timeline", "updated_at"]

    severity = fix.get("severity")
    if severity:
        incident.severity = severity
        updates.append("severity")

    category = fix.get("category")
    if category:
        incident.category = category
        updates.append("category")

    if fix.get("root_cause") and not incident.root_cause:
        incident.root_cause = fix["root_cause"]
        updates.append("root_cause")

    if fix.get("corrective_action") and not incident.corrective_action:
        incident.corrective_action = fix["corrective_action"]
        updates.append("corrective_action")

    if fix.get("resolution_report") and not incident.resolution_report:
        incident.resolution_report = fix["resolution_report"]
        updates.append("resolution_report")

    auto_cfg = fix.get("auto_retry") or {}
    auto_enabled = auto_cfg.get("enabled", False)
    if auto_enabled and auto_cfg.get("max"):
        incident.max_auto_retries = auto_cfg.get("max")
        updates.append("max_auto_retries")

    if auto_enabled:
        delay = auto_cfg.get("delay_seconds", 60)
        if incident.auto_retry_count < incident.max_auto_retries:
            incident.auto_retry_count += 1
            incident.state = "in_progress"
            updates.extend(["auto_retry_count", "state"])
            _append_incident_event(
                incident,
                "Auto retry scheduled",
                notes=f"Retry #{incident.auto_retry_count} queued in {delay}s for {run.job.name}",
            )
            default_queue.enqueue_in(timedelta(seconds=delay), job_chain_standardize, str(incident.upload.upload_id))
        else:
            _append_incident_event(
                incident,
                "Auto retry limit reached",
                notes=f"Max retries ({incident.max_auto_retries}) exhausted for {run.job.name}",
            )
    else:
        _append_incident_event(incident, "Known error tagged", notes=f"Matched {matched.name}")

    incident.save(update_fields=list(set(updates)))


def _create_incident_and_ticket(upload: Upload, run: JobRun, error_text: str) -> Incident:
    matched = _match_known_error(error_text)

    root_cause: Optional[str] = None
    corrective_action: Optional[str] = None

    if matched and isinstance(matched.fix, dict):
        root_cause = matched.fix.get("root_cause") or matched.fix.get("rca")
        corrective_action = matched.fix.get("corrective_action") or matched.fix.get("action")

    incident = Incident.objects.create(
        upload=upload,
        job_run=run,
        error=error_text,
        state="open",
        matched_known_error=matched,
        root_cause=root_cause,
        corrective_action=corrective_action,
        detection_source="engine",
    )
    _append_incident_event(incident, "Incident detected", notes=error_text[:280])
    incident.save(update_fields=["timeline"])

    Ticket.objects.create(
        incident=incident,
        source="system",
        status="in_progress",
        assignee="engine",
        title=f"Auto ticket: {run.job.name}",
        description=(error_text or "")[:500],
        timeline=[
            {"timestamp": timezone.now().isoformat(), "event": "System ticket created", "actor": "system"},
        ],
    )
    _auto_triage_incident(incident, matched, run)
    record_incident_metric("open")
    return incident


def _load_df(upload: Upload) -> pd.DataFrame:
    if not upload.file_path or not os.path.exists(upload.file_path):
        raise FileNotFoundError(f"File not found: {upload.file_path}")

    ext = os.path.splitext(upload.file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(upload.file_path)
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(upload.file_path)
    if ext == ".pdf":
        import pdfplumber

        with pdfplumber.open(upload.file_path) as pdf:
            tables = pdf.pages[0].extract_tables()
            if not tables or not tables[0]:
                raise ValueError("No table found in first PDF page")
            header = tables[0][0]
            rows = tables[0][1:]
            return pd.DataFrame(rows, columns=header)

    raise ValueError(f"Unsupported file type: {ext}")


def run_custom_job(job_id: int, payload: Optional[dict] = None) -> None:
    job = Job.objects.get(id=job_id)
    job_run = _start_generic_run(job)
    config = dict(job.config or {})
    if payload:
        config.update(payload)
    job_run.details = {"config": config}
    job_run.save(update_fields=["details"])
    logs = ""
    try:
        if job.job_type == "python":
            callable_path = config.get("callable")
            if not callable_path or "." not in callable_path:
                raise ValueError("Python jobs require a dotted callable path in config['callable'].")
            module_name, func_name = callable_path.rsplit(".", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            args = config.get("args", [])
            kwargs = config.get("kwargs", {})
            result = func(*args, **kwargs)
            logs = f"Job executed successfully. Result: {result!r}"
        else:
            raise ValueError(f"Unsupported job_type '{job.job_type}'. Only 'python' jobs are supported.")
        _finish_run(job_run, "success", logs)
    except Exception as exc:  # noqa: BLE001
        logs = f"Scheduled job failed: {exc}"
        _finish_run(job_run, "failed", logs, exit_code=1)
        logger.exception("Scheduled job %s failed", job.name)
        raise


def job_chain_standardize(upload_id: str) -> None:
    # Ensure known error patterns exist so we can tag incidents consistently.
    _ensure_default_known_errors()

    upload = Upload.objects.get(upload_id=upload_id)
    upload.status = "processing"
    upload.save(update_fields=["status"])

    pipeline_job = _get_or_create_job(PIPELINE_JOB_NAME)
    pipeline_run = _start_run(pipeline_job, upload)

    df: Optional[pd.DataFrame] = None
    summary: dict = {}
    step_records = []
    pipeline_logs = []

    def _record_step(step_name: str, status: str, started_at, finished_at, log_text: str = ""):
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)
        step_records.append(
            {
                "name": step_name,
                "status": status,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_ms": duration_ms,
                "logs": log_text,
            }
        )
        record_job_metric(f"{PIPELINE_JOB_NAME}.{step_name}", status, duration_ms)

    try:
        for step in PIPELINE:
            step_start = timezone.now()
            log_msg = ""
            status = "running"

            try:
                if step == "standardize_results":
                    df = _load_df(upload)
                    df.columns = [" ".join(str(c).split()).strip().lower() for c in df.columns]
                    summary["rows"] = int(len(df))
                    summary["cols"] = int(len(df.columns))
                    summary["columns"] = df.columns.tolist()
                    log_msg = f"Loaded {summary['rows']} rows, {summary['cols']} cols"

                elif step == "validate_results":
                    if df is None:
                        raise RuntimeError("No dataframe loaded")
                    errs = []
                    if len(df.columns) == 0:
                        errs.append("No columns detected")
                    if len(df) == 0:
                        errs.append("No rows detected")

                    dept = upload.department or ""
                    required = REQUIRED_COLUMNS_BY_DEPARTMENT.get(dept, REQUIRED_COLUMNS_DEFAULT)
                    missing = [c for c in required if c not in [str(col).lower() for col in df.columns]]
                    if missing:
                        errs.append(f"Required columns missing: {', '.join(missing)}")

                    if errs:
                        raise ValueError("; ".join(errs))
                    log_msg = "Validation passed"

                elif step == "transform_gradebook":
                    if df is None:
                        raise RuntimeError("No dataframe loaded")
                    for c in df.columns:
                        series = df[c]
                        if series.dtype == "object":
                            series = series.astype(str).str.strip()
                            df[c] = pd.to_numeric(series, errors="ignore")
                        else:
                            df[c] = series
                    log_msg = "Transformed gradebook (trim + safe numeric coercion)"

                elif step == "generate_summary":
                    if df is None:
                        raise RuntimeError("No dataframe loaded")
                    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                    summary["numeric_cols"] = numeric_cols
                    rows = [
                        ["upload_id", str(upload.upload_id)],
                        ["department", upload.department],
                        ["filename", upload.filename],
                        ["rows", summary.get("rows", len(df))],
                        ["cols", summary.get("cols", len(df.columns))],
                        ["columns", ", ".join(summary.get("columns", df.columns.tolist()))],
                    ]
                    if numeric_cols:
                        desc = df[numeric_cols].describe()
                        summary["describe"] = desc.to_dict()
                        for col in numeric_cols:
                            stats = desc[col].to_dict()
                            for stat_name, value in stats.items():
                                rows.append([f"{col}.{stat_name}", value])
                    summary["summary_rows"] = rows
                    log_msg = f"Summary built. Numeric cols: {len(numeric_cols)}"

                elif step == "publish_results":
                    export_dir = getattr(settings, "EXPORT_DIR", "/app/storage/exports")
                    os.makedirs(export_dir, exist_ok=True)
                    export_path = os.path.join(export_dir, f"{upload.upload_id}-summary.csv")

                    rows = summary.get("summary_rows")
                    if not rows:
                        rows = [
                            ["upload_id", str(upload.upload_id)],
                            ["department", upload.department],
                            ["filename", upload.filename],
                            ["rows", summary.get("rows", 0)],
                            ["cols", summary.get("cols", 0)],
                            ["columns", ", ".join(summary.get("columns", []))],
                        ]
                    if not rows:
                        rows = [["message", "No summary data available"]]
                    df_rows = pd.DataFrame(rows, columns=["field", "value"])
                    df_rows.to_csv(export_path, index=False)

                    csv_buf = io.StringIO()
                    df_rows.to_csv(csv_buf, index=False)

                    upload.status = "published"
                    upload.report_path = export_path
                    upload.report_generated_at = timezone.now()
                    upload.report_csv = csv_buf.getvalue()
                    upload.report_meta = summary
                    upload.save(
                        update_fields=["status", "report_path", "report_generated_at", "report_csv", "report_meta"],
                    )

                    log_msg = f"Published export: {export_path}"

                status = "success"
            except Exception as e:  # noqa: BLE001
                status = "failed"
                log_msg = str(e)
                raise
            finally:
                finished = timezone.now()
                _record_step(step, status, step_start, finished, log_msg)
                pipeline_logs.append(f"[{step}] {log_msg or status}")

    except Exception as e:  # noqa: BLE001
        msg = f"{step} failed: {str(e)}"
        logger.exception(msg)
        upload.status = "failed"
        upload.save(update_fields=["status"])
        pipeline_run.details = {"steps": step_records}
        pipeline_run.logs = "\n".join(pipeline_logs + [msg])[:20000]
        _finish_run(pipeline_run, "failed", pipeline_run.logs, exit_code=1)
        _create_incident_and_ticket(upload, pipeline_run, msg)
        return

    pipeline_run.details = {"steps": step_records}
    pipeline_run.logs = "\n".join(pipeline_logs)[:20000]
    _finish_run(pipeline_run, "success", pipeline_run.logs)
