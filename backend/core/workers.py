import importlib
import logging
import os
import re
import io
import csv
import base64
from datetime import timedelta
from typing import Optional, Tuple

import django
from django.conf import settings
from django.utils import timezone

import pandas as pd
from fpdf import FPDF

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
            header = None
            rows = None
            text_lines = []
            for page in pdf.pages:
                raw_text = page.extract_text()
                if raw_text:
                    text_lines.extend(line.strip() for line in raw_text.splitlines() if line.strip())
                tables = page.extract_tables()
                if not tables:
                    continue
                for table in tables:
                    if not table or len(table) <= 1:
                        continue
                    header = table[0]
                    rows = table[1:]
                    break
                if header is not None:
                    break
            text_lines = _stitch_pdf_lines(text_lines)
            text_blob = "\n".join(text_lines)
            if header is None or rows is None:
                candidate_rows = []
                if text_lines:
                    has_commas = any("," in line for line in text_lines[:10])
                    if has_commas:
                        parsed = [row for row in csv.reader(text_lines) if any(col.strip() for col in row)]
                        if parsed and len(parsed[0]) > 1:
                            candidate_rows = parsed
                    if not candidate_rows:
                        for line in text_lines:
                            cols = [col.strip() for col in re.split(r"\s{2,}", line) if col.strip()]
                            if len(cols) <= 1:
                                cols = [token.strip() for token in line.split() if token.strip()]
                            if cols:
                                candidate_rows.append(cols)
                if candidate_rows and len(candidate_rows[0]) > 1:
                    header = candidate_rows[0]
                    rows = candidate_rows[1:]
                else:
                    if text_blob:
                        for delimiter in [",", ";", "|", "\t"]:
                            try:
                                df_candidate = pd.read_csv(io.StringIO(text_blob), sep=delimiter)
                                if df_candidate.shape[1] > 1:
                                    return df_candidate
                            except Exception:
                                continue
                        try:
                            df_candidate = pd.read_csv(io.StringIO(text_blob), sep=None, engine="python")
                            if df_candidate.shape[1] > 1:
                                return df_candidate
                        except Exception:
                            pass
                        fwf_candidate = _try_fixed_width_table(text_blob, 0)
                        if fwf_candidate is not None:
                            return fwf_candidate
            if header is None or rows is None:
                raise ValueError("No table found in PDF pages")
            return _finalize_pdf_dataframe(header, rows, text_blob)

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
                    df.columns = [_normalize_column_label(c) for c in df.columns]
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
                            try:
                                df[c] = pd.to_numeric(series)
                            except Exception:
                                df[c] = series
                        else:
                            df[c] = series
                    plan_df, plan_mode, plan_summary = _apply_processing_plan(df, upload)
                    df = plan_df
                    summary["processing_plan"] = {
                        "mode": plan_mode,
                        "description": plan_summary,
                        "config": upload.process_config or {},
                    }
                    log_pieces = ["Transformed gradebook (trim + safe numeric coercion)"]
                    if plan_summary:
                        log_pieces.append(plan_summary)
                    log_msg = " | ".join(log_pieces)

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
                    mode = (upload.process_mode or "transform_gradebook").strip().lower()
                    is_summary = mode == "transform_gradebook"
                    file_suffix = "summary" if is_summary else "processed"
                    export_path = os.path.join(export_dir, f"{upload.upload_id}-{file_suffix}.csv")

                    csv_buf = io.StringIO()
                    if is_summary:
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
                        df_rows.to_csv(csv_buf, index=False)
                    else:
                        if df is None:
                            raise RuntimeError("No dataframe available for export")
                        df.to_csv(export_path, index=False)
                        df.to_csv(csv_buf, index=False)
                        pdf_columns = list(df.columns)
                        pdf_rows = df.astype(str).values.tolist()

                    upload.status = "published"
                    upload.report_path = export_path
                    upload.report_generated_at = timezone.now()
                    upload.report_csv = csv_buf.getvalue()
                    if is_summary:
                        pdf_rows = rows or []
                        pdf_columns = ["field", "value"]
                    pdf_titles = upload.filename or f"Upload {upload.upload_id}"
                    pdf_bytes = _build_pdf_table(pdf_titles, pdf_columns, pdf_rows or [])
                    upload.report_pdf = base64.b64encode(pdf_bytes if isinstance(pdf_bytes, bytes) else bytes(pdf_bytes)).decode("ascii")
                    upload.report_meta = summary
                    upload.save(
                        update_fields=["status", "report_path", "report_generated_at", "report_csv", "report_pdf", "report_meta"],
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
def _apply_processing_plan(df: pd.DataFrame, upload: Upload) -> Tuple[pd.DataFrame, str, str]:
    mode = (upload.process_mode or "transform_gradebook").strip() or "transform_gradebook"
    normalized = mode.lower()
    cfg = upload.process_config or {}
    summary = ""

    if normalized == "append_record":
        records = cfg.get("records") or cfg.get("row")
        rows_added = 0
        payload = []
        if isinstance(records, dict):
            payload = [records]
        elif isinstance(records, list):
            payload = [row for row in records if isinstance(row, dict)]
        if payload:
            df = pd.concat([df, pd.DataFrame(payload)], ignore_index=True)
            rows_added = len(payload)
        summary = f"Appended {rows_added} custom row(s)" if rows_added else "Append skipped (no valid rows)"

    elif normalized == "delete_record":
        rules = cfg.get("rules")
        if isinstance(rules, list) and rules:
            total_removed = 0
            for rule in rules:
                column = rule.get("column")
                value = rule.get("value")
                if not column:
                    continue
                if column in df.columns:
                    before = len(df)
                    df = df[df[column].astype(str) != str(value)]
                    total_removed += before - len(df)
            summary = f"Removed {total_removed} row(s) via {len(rules)} rule(s)"
        else:
            column = cfg.get("column")
            value = cfg.get("value")
            if column and column in df.columns:
                before = len(df)
                df = df[df[column].astype(str) != str(value)]
                removed = before - len(df)
                summary = f"Removed {removed} row(s) where {column} == {value}"
            elif column:
                summary = f"Delete skipped (column '{column}' missing)"
            else:
                summary = "Delete skipped (column not specified)"

    elif normalized == "custom_rules":
        note = cfg.get("notes") or cfg.get("instructions")
        summary = f"Custom instructions noted: {note}" if note else "Custom instructions captured"

    else:
        summary = "Standard transform applied"

    return df, mode, summary


def _build_pdf_table(title: str, columns: list[str], rows: list[list[str]]) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, title, ln=True, align="L")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 9)
    effective_width = pdf.w - 2 * pdf.l_margin
    col_count = max(1, len(columns))
    col_widths = [effective_width / col_count for _ in range(col_count)]

    def _draw_row(values: list[str], fill: bool = False) -> None:
        pdf.set_fill_color(245, 245, 245) if fill else pdf.set_fill_color(255, 255, 255)
        for idx in range(col_count):
            text = str(values[idx]) if idx < len(values) else ""
            text = text[:300]
            pdf.cell(col_widths[idx], 6, text, border=1, ln=0, align="L", fill=fill)
        pdf.ln(6)

    pdf.set_font("Helvetica", "B", 10)
    _draw_row(columns, fill=True)

    pdf.set_font("Helvetica", "", 9)
    for row in rows:
        _draw_row([str(cell) for cell in row])

    return bytes(pdf.output(dest="S"))
def _finalize_pdf_dataframe(header: list[str], rows: list[list[str]], text_blob: str) -> pd.DataFrame:
    expected = len(header)
    normalized_rows = [list(row) for row in rows or []]
    needs_alignment = expected > 0 and any(len(row) != expected for row in normalized_rows if row)
    if needs_alignment:
        fwf_candidate = _try_fixed_width_table(text_blob, expected)
        if fwf_candidate is not None:
            return fwf_candidate
        normalized_rows = [_align_row_tokens(row, expected) for row in normalized_rows]
    else:
        normalized_rows = [row if len(row) == expected else _align_row_tokens(row, expected) for row in normalized_rows]
    return pd.DataFrame(normalized_rows, columns=header)


def _try_fixed_width_table(text_blob: str, expected_cols: int) -> Optional[pd.DataFrame]:
    if not text_blob or not text_blob.strip():
        return None
    try:
        fwf_df = pd.read_fwf(io.StringIO(text_blob), header=None)
    except Exception:
        return None
    if fwf_df.empty or fwf_df.shape[1] <= 1:
        return None
    header_row = [str(value).strip() for value in fwf_df.iloc[0].tolist()]
    data = fwf_df.iloc[1:].reset_index(drop=True)
    if expected_cols and len(header_row) >= expected_cols:
        header_row = header_row[:expected_cols]
        data = data.iloc[:, :expected_cols]
    elif expected_cols and len(header_row) < expected_cols:
        return None
    data.columns = [label or f"column_{idx+1}" for idx, label in enumerate(header_row)]
    data = data.dropna(how="all").reset_index(drop=True)
    if data.empty:
        return None
    return data


def _align_row_tokens(row: list[str], expected_cols: int) -> list[str]:
    tokens = [str(token).strip() for token in row if str(token).strip() or len(row) == 1]
    if expected_cols <= 0:
        return tokens
    id_idx = None
    for idx, token in enumerate(tokens):
        if _looks_student_identifier(token):
            id_idx = idx
            break
    if id_idx is not None and id_idx != 0:
        id_token = tokens.pop(id_idx)
        tokens.insert(0, id_token)
    protect_edges = len(tokens) > 2 and expected_cols >= 2
    if expected_cols <= 0:
        return tokens
    protected_zero = protect_edges and tokens and _looks_student_identifier(tokens[0])
    while len(tokens) > expected_cols:
        merge_idx = None
        start_idx = 1 if protect_edges else 0
        end_idx = len(tokens) - 3 if protect_edges else len(tokens) - 2
        if end_idx < start_idx:
            end_idx = start_idx
        for idx in range(start_idx, end_idx + 1):
            if protected_zero and (idx == 0 or idx + 1 == 0):
                continue
            left = tokens[idx]
            right = tokens[idx + 1]
            if not _looks_numeric(left) and not _looks_numeric(right):
                merge_idx = idx
                break
        if merge_idx is None:
            for idx in range(start_idx, len(tokens) - 1):
                if protected_zero and (idx == 0 or idx + 1 == 0):
                    continue
                left = tokens[idx]
                right = tokens[idx + 1]
                if not (_looks_numeric(left) and _looks_numeric(right)):
                    merge_idx = idx
                    break
        if merge_idx is None:
            merge_idx = len(tokens) - 2
        merged = " ".join(token for token in [tokens[merge_idx], tokens[merge_idx + 1]] if token).strip()
        if not merged:
            merged = tokens[merge_idx] + tokens[merge_idx + 1]
        tokens = tokens[:merge_idx] + [merged] + tokens[merge_idx + 2 :]
        protect_edges = len(tokens) > 2 and expected_cols >= 2
    if len(tokens) < expected_cols:
        tokens.extend([""] * (expected_cols - len(tokens)))
    elif len(tokens) > expected_cols:
        tokens = tokens[:expected_cols]
    return tokens


def _looks_numeric(value: str) -> bool:
    if not value:
        return False
    filtered = value.replace(",", "").replace("%", "")
    try:
        float(filtered)
        return True
    except ValueError:
        return False


def _looks_student_identifier(value: str) -> bool:
    if not value:
        return False
    text = str(value).strip()
    if not text or " " in text:
        return False
    has_alpha = any(ch.isalpha() for ch in text)
    has_digit = any(ch.isdigit() for ch in text)
    return has_alpha and has_digit


def _stitch_pdf_lines(lines: list[str]) -> list[str]:
    """Merge identifier-only lines with their subsequent detail rows to keep rows intact."""
    if not lines:
        return []
    stitched: list[str] = []
    id_pattern = re.compile(r"^[A-Za-z]{1,10}\d[\w-]*$")
    idx = 0
    total = len(lines)
    while idx < total:
        line = lines[idx].strip()
        if not line:
            idx += 1
            continue
        if id_pattern.match(line):
            parts = [line]
            idx += 1
            while idx < total:
                next_line = lines[idx].strip()
                if not next_line:
                    idx += 1
                    continue
                if id_pattern.match(next_line):
                    break
                parts.append(next_line)
                idx += 1
            stitched.append(" ".join(parts).strip())
        else:
            stitched.append(line)
            idx += 1
    return stitched


def _normalize_column_label(label: str) -> str:
    text = " ".join(str(label or "").split()).lower()
    text = re.sub(r"[^\w\s-]+", "", text)
    text = re.sub(r"[\s-]+", "_", text)
    return text.strip("_")
