import importlib
import logging
import os
import re
import io
import csv
import base64
import math
from collections import Counter
from datetime import timedelta
from typing import Optional, Tuple

import django
from django.conf import settings
from django.utils import timezone

import pandas as pd
from fpdf import FPDF
from django.db.models import Q

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
            "auto_fix": {"actions": ["promote_header"]},
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
            "auto_fix": {"actions": ["ensure_row"]},
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
            "auto_fix": {"actions": ["alias_columns"]},
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
            "auto_fix": {"actions": ["convert_to_csv"]},
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
            "auto_fix": {"actions": ["reencode_utf8"]},
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
            "auto_fix": {"actions": ["clip_scores"]},
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
            "auto_fix": {"actions": ["dedupe_students"]},
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


def _sanitize_json(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(k): _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_json(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return _sanitize_json(value.item())
    except Exception:
        return str(value)


def _coerce_numeric_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    numeric_hints = {
        "score",
        "marks",
        "mark",
        "total",
        "total_marks",
        "total_score",
        "points",
        "point",
        "percent",
        "percentage",
        "gpa",
    }
    exclude_hints = {
        "id",
        "name",
        "student",
        "code",
        "roll",
        "admission",
        "registration",
        "reg",
        "enroll",
    }
    converted = []
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            continue
        if not pd.api.types.is_object_dtype(series):
            continue
        label = _normalize_column_label(col)
        tokens = {token for token in label.split("_") if token}
        has_numeric_hint = bool(tokens & numeric_hints)
        if (tokens & exclude_hints) and not has_numeric_hint:
            continue
        cleaned = series.astype(str).str.strip()
        cleaned = cleaned.replace({"": None, "nan": None, "None": None})
        cleaned = cleaned.str.replace(",", "", regex=False).str.replace("%", "", regex=False)
        numeric = pd.to_numeric(cleaned, errors="coerce")
        total = len(numeric)
        if total == 0:
            continue
        ratio = numeric.notna().sum() / total
        threshold = 0.3 if has_numeric_hint else 0.6
        if ratio >= threshold and numeric.notna().sum() > 0:
            df[col] = numeric
            converted.append(col)
    return df, converted


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


def _resolve_auto_fix_actions(matched: Optional[KnownError], error_text: str) -> list[str]:
    actions: list[str] = []
    if matched and isinstance(matched.fix, dict):
        auto_fix = matched.fix.get("auto_fix")
        if isinstance(auto_fix, dict):
            raw = auto_fix.get("actions") or []
            if isinstance(raw, str):
                actions = [raw]
            elif isinstance(raw, list):
                actions = [str(item) for item in raw if item]
    if matched and matched.name == "Required columns missing":
        actions = [action for action in actions if action != "add_missing_columns"]
    if actions:
        return actions
    lowered = (error_text or "").lower()
    mapping = [
        ("no columns detected", ["promote_header"]),
        ("no rows detected", ["ensure_row"]),
        ("required columns missing", ["alias_columns"]),
        ("unsupported file type", ["convert_to_csv"]),
        ("unicode", ["reencode_utf8"]),
        ("codec can't decode", ["reencode_utf8"]),
        ("duplicate rows detected", ["dedupe_students"]),
        ("score must be between", ["clip_scores"]),
        ("value out of range", ["clip_scores"]),
    ]
    for needle, mapped in mapping:
        if needle in lowered:
            actions.extend(mapped)
    return actions


def _load_dataframe_for_fix(upload: Upload, header_mode: Optional[str] = None, encoding: Optional[str] = None) -> Optional[pd.DataFrame]:
    if not upload.file_path or not os.path.exists(upload.file_path):
        return None
    ext = os.path.splitext(upload.file_path)[1].lower()
    if ext == ".csv":
        try:
            if header_mode == "none":
                return pd.read_csv(upload.file_path, header=None, sep=None, engine="python", encoding=encoding)
            return pd.read_csv(upload.file_path, encoding=encoding)
        except Exception:
            if header_mode == "none":
                try:
                    return pd.read_csv(upload.file_path, header=None, encoding=encoding)
                except Exception:
                    return None
            return None
    if ext in [".xlsx", ".xls"]:
        try:
            return pd.read_excel(upload.file_path, header=None if header_mode == "none" else 0)
        except Exception:
            return None
    if ext == ".pdf":
        try:
            return _load_df(upload)
        except Exception:
            return None
    return None


def _write_fixed_dataframe(upload: Upload, df: pd.DataFrame, label: str) -> str:
    upload_dir = os.path.dirname(upload.file_path) if upload.file_path else getattr(settings, "UPLOAD_DIR", "/app/storage/uploads")
    os.makedirs(upload_dir, exist_ok=True)
    timestamp = timezone.now().strftime("%Y%m%d%H%M%S")
    filename = f"auto-fix-{label}-{timestamp}.csv"
    path = os.path.join(upload_dir, filename)
    df.to_csv(path, index=False)
    upload.file_path = path
    upload.save(update_fields=["file_path"])
    return path


def _apply_alias_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    alias_map = {
        "student_id": [
            "student_id",
            "studentid",
            "student_no",
            "student_number",
            "student_num",
            "student_code",
            "student_roll",
            "roll_no",
            "roll_number",
            "rollno",
            "roll",
            "admission_no",
            "admission_id",
            "admission_number",
            "admission",
            "registration_no",
            "registration_id",
            "reg_no",
            "reg_id",
            "enrollment_no",
            "enrollment_id",
            "enroll_no",
            "enroll_id",
            "id",
            "id_no",
            "id_number",
            "unique_id",
        ],
        "student_name": [
            "student_name",
            "studentname",
            "student_full_name",
            "name",
            "full_name",
            "full_name_of_student",
            "candidate_name",
            "pupil_name",
        ],
        "score": [
            "score",
            "marks",
            "mark",
            "total_marks",
            "total_mark",
            "total_score",
            "score_total",
            "points",
            "point",
            "grade",
            "percentage",
            "percent",
            "score_percent",
            "score_percentage",
            "final_score",
            "overall_score",
        ],
        "class": [
            "class",
            "class_name",
            "class_level",
            "grade_level",
            "grade",
            "level",
            "section",
            "class_section",
            "class_section_name",
        ],
        "subject": ["subject", "subject_name", "course", "course_name", "paper", "paper_name"],
    }
    normalized = {col: _normalize_column_label(col) for col in df.columns}
    rename = {}
    matched = []
    for col, norm in normalized.items():
        for target, aliases in alias_map.items():
            if norm in aliases and target not in df.columns and target not in rename.values():
                rename[col] = target
                matched.append(f"{col}->{target}")
                break
    if rename:
        df = df.rename(columns=rename)
    return df, matched


def _apply_auto_fix(upload: Upload, matched: Optional[KnownError], error_text: str) -> Optional[str]:
    actions = _resolve_auto_fix_actions(matched, error_text)
    if not actions:
        return None

    notes = []

    if "reencode_utf8" in actions:
        df = None
        used = None
        for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
            df = _load_dataframe_for_fix(upload, encoding=enc)
            if df is not None:
                used = enc
                break
        if df is not None:
            _write_fixed_dataframe(upload, df, "utf8")
            notes.append(f"Re-encoded CSV using {used}")

    if "convert_to_csv" in actions:
        df = _load_dataframe_for_fix(upload)
        if df is not None:
            _write_fixed_dataframe(upload, df, "converted")
            notes.append("Converted file to CSV for retry")

    df_actions = {"promote_header", "alias_columns", "add_missing_columns", "ensure_row", "dedupe_students", "clip_scores"}
    if any(action in df_actions for action in actions):
        header_mode = "none" if "promote_header" in actions else None
        df = _load_dataframe_for_fix(upload, header_mode=header_mode)
        if df is None:
            return "; ".join(notes) if notes else None

        changed = False
        if "promote_header" in actions:
            if df.empty or df.shape[0] < 2:
                return "; ".join(notes) if notes else None
            header_row = [str(value).strip() for value in df.iloc[0].tolist()]
            if not any(header_row):
                return "; ".join(notes) if notes else None
            df = df.iloc[1:].reset_index(drop=True)
            df.columns = [(_normalize_column_label(label) or f"column_{idx+1}") for idx, label in enumerate(header_row)]
            notes.append("Promoted first row to headers")
            changed = True

        if "alias_columns" in actions:
            df.columns = [_normalize_column_label(c) for c in df.columns]
            df, matched_aliases = _apply_alias_columns(df)
            if matched_aliases:
                notes.append("Aliased columns: " + ", ".join(matched_aliases))
                changed = True

        if "add_missing_columns" in actions:
            dept = upload.department or ""
            required = REQUIRED_COLUMNS_BY_DEPARTMENT.get(dept, REQUIRED_COLUMNS_DEFAULT)
            missing = [c for c in required if c not in [str(col).lower() for col in df.columns]]
            if missing:
                for col in missing:
                    df[col] = ""
                notes.append("Added missing columns: " + ", ".join(missing))
                changed = True

        if "ensure_row" in actions and df.empty and df.columns.tolist():
            df.loc[0] = ["" for _ in df.columns]
            notes.append("Inserted placeholder row")
            changed = True

        if "dedupe_students" in actions and "student_id" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["student_id"])
            removed = before - len(df)
            if removed > 0:
                notes.append(f"Removed {removed} duplicate student rows")
                changed = True

        if "clip_scores" in actions and "score" in df.columns:
            series = pd.to_numeric(df["score"], errors="coerce")
            df["score"] = series.clip(lower=0, upper=100)
            notes.append("Clipped score values to 0-100")
            changed = True

        if changed:
            _write_fixed_dataframe(upload, df, "datafix")

    return "; ".join(notes) if notes else None


def _attempt_auto_remediation(incident: Incident, matched: Optional[KnownError], run: JobRun, error_text: str) -> None:
    if not matched:
        return
    if incident.assignee:
        return
    if incident.detection_source != "engine":
        return
    if incident.state == "resolved":
        return
    if incident.auto_retry_count >= incident.max_auto_retries:
        _append_incident_event(
            incident,
            "Auto-remediation skipped",
            notes=f"Retry limit reached ({incident.auto_retry_count}/{incident.max_auto_retries})",
        )
        incident.save(update_fields=["timeline", "updated_at"])
        return

    result = _apply_auto_fix(incident.upload, matched, error_text)
    if not result:
        return

    incident.auto_retry_count += 1
    incident.state = "in_progress"
    _append_incident_event(incident, "Auto-remediation applied", notes=result)
    incident.save(update_fields=["auto_retry_count", "state", "timeline", "updated_at"])
    default_queue.enqueue(job_chain_standardize, str(incident.upload.upload_id))


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
    _attempt_auto_remediation(incident, matched, run, error_text)
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
                    df, matched_aliases = _apply_alias_columns(df)
                    summary["rows"] = int(len(df))
                    summary["cols"] = int(len(df.columns))
                    summary["columns"] = df.columns.tolist()
                    log_pieces = [f"Loaded {summary['rows']} rows, {summary['cols']} cols"]
                    if matched_aliases:
                        summary["column_aliases"] = matched_aliases
                        log_pieces.append("Aliased columns: " + ", ".join(matched_aliases))
                    log_msg = " | ".join(log_pieces)

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
                    df, coerced_cols = _coerce_numeric_columns(df)
                    plan_df, plan_mode, plan_summary = _apply_processing_plan(df, upload)
                    df = plan_df
                    summary["processing_plan"] = {
                        "mode": plan_mode,
                        "description": plan_summary,
                        "config": upload.process_config or {},
                    }
                    log_pieces = ["Transformed gradebook (trim + numeric coercion)"]
                    if coerced_cols:
                        log_pieces.append("Numeric columns: " + ", ".join(coerced_cols))
                    if plan_summary:
                        log_pieces.append(plan_summary)
                    log_msg = " | ".join(log_pieces)

                elif step == "generate_summary":
                    if df is None:
                        raise RuntimeError("No dataframe loaded")

                    def _format_stat(value: object) -> str:
                        if value is None:
                            return "-"
                        try:
                            if pd.isna(value):
                                return "-"
                        except TypeError:
                            pass
                        if isinstance(value, (int, float)):
                            return f"{value:.2f}".rstrip("0").rstrip(".")
                        return str(value)

                    def _format_int(value: object) -> str:
                        if value is None:
                            return "-"
                        try:
                            if pd.isna(value):
                                return "-"
                        except TypeError:
                            pass
                        try:
                            return f"{int(value):,}"
                        except (TypeError, ValueError):
                            return str(value)

                    def _format_percent(value: float | None) -> str:
                        if value is None:
                            return "-"
                        return f"{value * 100:.1f}%"

                    numeric_candidates = df.select_dtypes(include=["number"]).columns.tolist()
                    filtered_numeric = []
                    exclude_tokens = {
                        "id",
                        "code",
                        "name",
                        "student",
                        "roll",
                        "admission",
                        "registration",
                        "reg",
                        "enroll",
                    }
                    for col in numeric_candidates:
                        label = _normalize_column_label(col)
                        tokens = {token for token in label.split("_") if token}
                        if tokens & exclude_tokens:
                            continue
                        series = df[col].dropna()
                        if series.empty:
                            continue
                        if series.nunique(dropna=True) <= 1:
                            continue
                        filtered_numeric.append(col)
                    numeric_cols = filtered_numeric
                    summary["numeric_cols"] = numeric_cols

                    plan = summary.get("processing_plan", {})
                    row_count = summary.get("rows", len(df))
                    col_count = summary.get("cols", len(df.columns))
                    total_cells = int(row_count) * int(col_count) if row_count and col_count else 0
                    missing_cells = int(df.isna().sum().sum()) if total_cells else 0
                    missing_rate = (missing_cells / total_cells) if total_cells else 0.0
                    duplicate_rows = int(df.duplicated().sum()) if row_count else 0
                    duplicate_students = (
                        int(df["student_id"].duplicated().sum()) if "student_id" in df.columns else None
                    )
                    summary["missing_cells"] = missing_cells
                    summary["missing_rate"] = missing_rate
                    summary["duplicate_rows"] = duplicate_rows
                    summary["duplicate_student_ids"] = duplicate_students
                    completeness_rows = []
                    if row_count:
                        col_missing = df.isna().sum()
                        col_complete = ((row_count - col_missing) / row_count).sort_values()
                        for col_name, pct in col_complete.head(6).items():
                            completeness_rows.append([f"{col_name}", _format_percent(float(pct))])

                    rows = [
                        ["section", "Executive summary"],
                        ["status", "Completed"],
                        ["processed_at", timezone.localtime().isoformat()],
                        ["processing_mode", plan.get("mode", upload.process_mode or "transform_gradebook")],
                        ["plan_notes", plan.get("description", "") or "Standard batch processing"],
                        ["rows", _format_int(row_count)],
                        ["cols", _format_int(col_count)],
                        ["total_cells", _format_int(total_cells)],
                        ["missing_cells", _format_int(missing_cells)],
                        ["missing_cell_rate", _format_percent(missing_rate)],
                        ["duplicate_rows", _format_int(duplicate_rows)],
                        [
                            "duplicate_student_ids",
                            _format_int(duplicate_students) if duplicate_students is not None else "Not applicable",
                        ],
                        ["numeric_columns", ", ".join(numeric_cols) if numeric_cols else "None detected"],
                        ["section", "Dataset profile"],
                        ["department", upload.department],
                        ["filename", upload.filename],
                        ["columns", ", ".join(summary.get("columns", df.columns.tolist()))],
                    ]

                    if completeness_rows:
                        rows.append(["section", "Column completeness (lowest)"])
                        rows.extend(completeness_rows)

                    if numeric_cols:
                        desc = df[numeric_cols].describe()
                        summary["describe"] = desc.to_dict()
                        rows.append(["section", "Numeric statistics"])
                        for col in numeric_cols:
                            stats = desc[col].to_dict()
                            rows.append([f"{col}.count", _format_stat(stats.get("count"))])
                            rows.append([f"{col}.mean", _format_stat(stats.get("mean"))])
                            rows.append([f"{col}.median", _format_stat(stats.get("50%"))])
                            rows.append([f"{col}.min", _format_stat(stats.get("min"))])
                            rows.append([f"{col}.max", _format_stat(stats.get("max"))])

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
                        numeric_cols = summary.get("numeric_cols") or []
                        if not rows:
                            rows = [
                                ["section", "Executive summary"],
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
                    meta_lines = None
                    if is_summary:
                        pdf_rows = rows or []
                        pdf_columns = ["field", "value"]
                        meta_lines = [
                            f"Department: {upload.department}",
                            f"Rows: {summary.get('rows', 0)} | Columns: {summary.get('cols', 0)}",
                        ]
                        missing_rate = summary.get("missing_rate")
                        missing_cells = summary.get("missing_cells")
                        duplicate_rows = summary.get("duplicate_rows")
                        if missing_rate is not None:
                            meta_lines.append(
                                f"Missing cells: {missing_cells or 0} ({missing_rate * 100:.1f}%)"
                            )
                        if duplicate_rows is not None:
                            meta_lines.append(f"Duplicate rows: {duplicate_rows}")
                        if numeric_cols:
                            meta_lines.append(f"Numeric columns: {', '.join(numeric_cols)}")
                    pdf_titles = upload.filename or f"Upload {upload.upload_id}"
                    pdf_bytes = _build_pdf_table(pdf_titles, pdf_columns, pdf_rows or [], meta_lines=meta_lines)
                    upload.report_pdf = base64.b64encode(pdf_bytes if isinstance(pdf_bytes, bytes) else bytes(pdf_bytes)).decode("ascii")
                    upload.report_meta = _sanitize_json(summary)
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
    _auto_resolve_known_incidents(upload)


def _auto_resolve_known_incidents(upload: Upload) -> None:
    incidents = Incident.objects.filter(
        upload=upload,
        matched_known_error__isnull=False,
        state__in=["open", "in_progress"],
    ).filter(Q(assignee__isnull=True) | Q(assignee=""))
    if not incidents.exists():
        return
    for incident in incidents:
        incident.state = "resolved"
        incident.resolved_by = "engine"
        incident.resolution_report = incident.resolution_report or "Auto-resolved after successful remediation."
        incident.resolved_at = timezone.now()
        _append_incident_event(
            incident,
            "Incident auto-resolved",
            actor="engine",
            notes="Pipeline completed successfully after auto-remediation.",
        )
        incident.save(
            update_fields=["state", "resolved_by", "resolved_at", "resolution_report", "timeline", "updated_at"],
        )
        for ticket in incident.tickets.filter(status__in=["open", "in_progress"]):
            ticket.resolve(resolved_by="engine", resolution_type="automatic", notes="Auto-resolved with incident")
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


def _build_pdf_table(
    title: str,
    columns: list[str],
    rows: list[list[str]],
    meta_lines: list[str] | None = None,
) -> bytes:
    orientation = "L" if len(columns) > 6 else "P"

    class ReportPDF(FPDF):
        def footer(self) -> None:  # noqa: D401
            self.set_y(-12)
            self.set_font("Helvetica", "", 8)
            self.set_text_color(130, 130, 130)
            self.cell(0, 8, f"Page {self.page_no()}", align="R")

    pdf = ReportPDF(orientation=orientation)
    pdf.set_auto_page_break(True, margin=16)
    pdf.alias_nb_pages()
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 15)
    pdf.set_text_color(20, 20, 20)
    pdf.cell(0, 10, title, ln=True, align="L")

    if meta_lines:
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(90, 90, 90)
        for line in meta_lines:
            if line:
                pdf.cell(0, 5, str(line), ln=True, align="L")
        pdf.ln(2)

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(30, 30, 30)
    effective_width = pdf.w - 2 * pdf.l_margin
    col_count = max(1, len(columns))

    def _wrap_text(text: str, width: float, max_lines: int = 4) -> list[str]:
        safe = str(text).replace("\n", " ").strip()
        if not safe:
            return [""]
        words = safe.split(" ")
        lines: list[str] = []
        current = ""
        for word in words:
            test = word if not current else f"{current} {word}"
            if pdf.get_string_width(test) <= width:
                current = test
                continue
            if current:
                lines.append(current)
                current = word
            else:
                chunk = ""
                for ch in word:
                    test_chunk = f"{chunk}{ch}"
                    if pdf.get_string_width(test_chunk) <= width:
                        chunk = test_chunk
                    else:
                        if chunk:
                            lines.append(chunk)
                        chunk = ch
                current = chunk
            if len(lines) >= max_lines:
                current = "..."
                break
        if current:
            lines.append(current)
        if len(lines) > max_lines:
            lines = lines[: max_lines - 1] + ["..."]
        return lines

    max_widths = [pdf.get_string_width(col) for col in columns]
    sample_rows = rows[: min(25, len(rows))]
    for row in sample_rows:
        for idx in range(col_count):
            text = str(row[idx]) if idx < len(row) else ""
            width = pdf.get_string_width(text)
            if width > max_widths[idx]:
                max_widths[idx] = width

    total_width = sum(max_widths)
    if total_width <= 0:
        col_widths = [effective_width / col_count for _ in range(col_count)]
    else:
        col_widths = [(w / total_width) * effective_width for w in max_widths]

    line_height = 5

    def _draw_row(values: list[str], fill: bool = False) -> None:
        wrapped_cells = []
        max_lines = 1
        for idx in range(col_count):
            cell_text = str(values[idx]) if idx < len(values) else ""
            cell_lines = _wrap_text(cell_text, col_widths[idx] - 2)
            wrapped_cells.append(cell_lines)
            max_lines = max(max_lines, len(cell_lines))

        row_height = line_height * max_lines
        if pdf.get_y() + row_height > pdf.page_break_trigger:
            pdf.add_page()
            _draw_row(columns, fill=True)

        x_start = pdf.l_margin
        y_start = pdf.get_y()

        for idx in range(col_count):
            x = x_start + sum(col_widths[:idx])
            pdf.set_xy(x, y_start)
            if fill:
                pdf.set_fill_color(242, 245, 248)
                pdf.rect(x, y_start, col_widths[idx], row_height, "F")
            pdf.rect(x, y_start, col_widths[idx], row_height, "D")
            pdf.set_xy(x + 1, y_start + 1)
            pdf.multi_cell(col_widths[idx] - 2, line_height, "\n".join(wrapped_cells[idx]), border=0)

        pdf.set_xy(x_start, y_start + row_height)

    pdf.set_font("Helvetica", "B", 10)
    _draw_row(columns, fill=True)

    pdf.set_font("Helvetica", "", 9)
    for row in rows:
        if col_count == 2 and str(row[0]).strip().lower() == "section":
            if pdf.get_y() + line_height * 2 > pdf.page_break_trigger:
                pdf.add_page()
                _draw_row(columns, fill=True)
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_fill_color(225, 232, 240)
            pdf.cell(0, line_height * 1.6, str(row[1]), ln=True, fill=True)
            pdf.set_font("Helvetica", "", 9)
            continue
        _draw_row([str(cell) for cell in row])

    return bytes(pdf.output(dest="S"))
def _finalize_pdf_dataframe(header: list[str], rows: list[list[str]], text_blob: str) -> pd.DataFrame:
    header_row = list(header or [])
    normalized_rows = [list(row) for row in rows or []]

    header_score = _score_header_row(header_row)
    header_hits = _header_keyword_hits(header_row)
    best_idx = None
    best_score = header_score
    for idx, row in enumerate(normalized_rows[:5]):
        score = _score_header_row(row)
        if score > best_score:
            best_score = score
            best_idx = idx

    if best_idx is not None and best_score >= max(2, header_score + 2):
        header_row = normalized_rows[best_idx]
        header_score = best_score
        header_hits = _header_keyword_hits(header_row)
        normalized_rows = normalized_rows[best_idx + 1 :]

    expected_mode = _most_common_row_length(normalized_rows, len(header_row))
    header_len = len(header_row)
    header_confident = header_hits >= 2 or header_score >= 2

    if header_len and header_confident:
        if expected_mode >= header_len and expected_mode - header_len <= 2:
            expected = header_len
        else:
            expected = expected_mode or header_len
    else:
        expected = expected_mode or header_len

    if expected <= 0 and normalized_rows:
        expected = max(len(row) for row in normalized_rows)
    if expected <= 0:
        expected = max(1, header_len)

    if len(header_row) != expected:
        header_row = _align_row_tokens(header_row, expected)
    header_row = [label or f"column_{idx+1}" for idx, label in enumerate(header_row)]

    needs_alignment = expected > 0 and any(len(row) != expected for row in normalized_rows if row)
    if needs_alignment:
        fwf_candidate = _try_fixed_width_table(text_blob, expected)
        if fwf_candidate is not None:
            return fwf_candidate
        normalized_rows = [_align_row_tokens(row, expected) for row in normalized_rows]
    else:
        normalized_rows = [row if len(row) == expected else _align_row_tokens(row, expected) for row in normalized_rows]
    df = pd.DataFrame(normalized_rows, columns=header_row)
    df = _trim_empty_auto_columns(df)
    return df


def _most_common_row_length(rows: list[list[str]], fallback: int) -> int:
    lengths = [len(row) for row in rows if row]
    if not lengths:
        return fallback
    counts = Counter(lengths)
    return counts.most_common(1)[0][0]


def _score_header_row(row: list[str]) -> int:
    if not row:
        return 0
    keywords = {
        "student",
        "id",
        "roll",
        "admission",
        "reg",
        "enroll",
        "name",
        "score",
        "marks",
        "mark",
        "grade",
        "percent",
        "percentage",
        "subject",
        "class",
        "section",
        "course",
        "paper",
        "result",
        "results",
    }
    alpha = 0
    numeric = 0
    tokens: list[str] = []
    for cell in row:
        text = str(cell or "").strip()
        if not text:
            continue
        if any(ch.isalpha() for ch in text):
            alpha += 1
        if _looks_numeric(text):
            numeric += 1
        normalized = _normalize_column_label(text)
        if normalized:
            tokens.extend(normalized.split("_"))
    keyword_hits = 0
    for token in tokens:
        if token in keywords:
            keyword_hits += 1
            continue
        if any(key in token for key in keywords):
            keyword_hits += 1
    return (keyword_hits * 3) + alpha - (numeric * 2)


def _header_keyword_hits(row: list[str]) -> int:
    if not row:
        return 0
    keywords = {
        "student",
        "id",
        "roll",
        "admission",
        "reg",
        "enroll",
        "name",
        "score",
        "marks",
        "mark",
        "grade",
        "percent",
        "percentage",
        "subject",
        "class",
        "section",
        "course",
        "paper",
    }
    hits = 0
    for cell in row:
        normalized = _normalize_column_label(cell)
        if not normalized:
            continue
        for key in keywords:
            if key in normalized:
                hits += 1
                break
    return hits


def _trim_empty_auto_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    drop_cols = []
    total = len(df)
    if total == 0:
        return df
    for col in df.columns:
        name = str(col)
        if not name.startswith("column_"):
            continue
        non_empty = df[col].astype(str).str.strip().replace({"nan": ""}).ne("").sum()
        if non_empty / total < 0.15:
            drop_cols.append(col)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


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
