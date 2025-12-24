import csv
import io
import os
import json
import logging
import base64
from datetime import datetime

import pandas as pd
from django.conf import settings
from django.db import connection
from django.http import HttpResponse
from django.utils import timezone

from rq import Worker

from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser

from .models import Upload, JobRun, Incident, Ticket, Job
from .serializers import UploadSerializer, JobRunSerializer, IncidentSerializer, TicketSerializer, JobSerializer
from .workers import (
    job_chain_standardize,
    _load_df,
    _append_incident_event,
    _apply_processing_plan,
    _normalize_column_label,
    _build_pdf_table,
)
from .metrics import get_metrics_data
from .queues import default_queue, redis_conn
from .scheduler import enqueue_job_now

logger = logging.getLogger(__name__)


def regenerate_report(upload: Upload) -> str | None:
    """
    Best-effort regeneration of the processed CSV if the pipeline file was removed.
    """
    try:
        df = _load_df(upload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to reload upload %s for report regen: %s", upload.upload_id, exc)
        return None

    df.columns = [_normalize_column_label(c) for c in df.columns]
    summary_rows = [
        ["upload_id", str(upload.upload_id)],
        ["department", upload.department],
        ["filename", upload.filename],
        ["rows", len(df)],
        ["cols", len(df.columns)],
        ["columns", ", ".join(df.columns.tolist())],
    ]
    summary = {
        "rows": len(df),
        "cols": len(df.columns),
        "columns": df.columns.tolist(),
        "summary_rows": list(summary_rows),
    }

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    summary["numeric_cols"] = numeric_cols
    if numeric_cols:
        desc = df[numeric_cols].describe()
        summary["describe"] = desc.to_dict()
        for col in numeric_cols:
            stats = desc[col].to_dict()
            for stat_name, value in stats.items():
                summary_rows.append([f"{col}.{stat_name}", value])
    summary["summary_rows"] = summary_rows

    for c in df.columns:
        series = df[c]
        if series.dtype == "object":
            series = series.astype(str).str.strip()
            df[c] = pd.to_numeric(series, errors="ignore")
        else:
            df[c] = series

    mode = (upload.process_mode or "transform_gradebook").strip().lower()
    export_dir = getattr(settings, "EXPORT_DIR", "/app/storage/exports")
    os.makedirs(export_dir, exist_ok=True)

    if mode == "transform_gradebook":
        export_path = os.path.join(export_dir, f"{upload.upload_id}-summary.csv")
        df_rows = pd.DataFrame(summary_rows, columns=["field", "value"])
        df_rows.to_csv(export_path, index=False)
        csv_buf = io.StringIO()
        df_rows.to_csv(csv_buf, index=False)
        pdf_columns = ["field", "value"]
        pdf_rows = summary_rows
    else:
        plan_df, plan_mode, plan_summary = _apply_processing_plan(df, upload)
        df = plan_df
        summary["processing_plan"] = {
            "mode": plan_mode,
            "description": plan_summary,
            "config": upload.process_config or {},
        }
        export_path = os.path.join(export_dir, f"{upload.upload_id}-processed.csv")
        df.to_csv(export_path, index=False)
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        pdf_columns = list(df.columns)
        pdf_rows = df.astype(str).values.tolist()

    upload.report_path = export_path
    upload.report_generated_at = timezone.now()
    upload.report_csv = csv_buf.getvalue()
    upload.report_meta = summary
    pdf_bytes = _build_pdf_table(f"Upload {upload.upload_id}", pdf_columns, pdf_rows or [])
    upload.report_pdf = base64.b64encode(pdf_bytes).decode("ascii")
    upload.save(update_fields=["report_path", "report_generated_at", "report_csv", "report_pdf", "report_meta"])
    return export_path


class UploadViewSet(viewsets.ModelViewSet):
    queryset = Upload.objects.all()
    serializer_class = UploadSerializer
    parser_classes = (MultiPartParser, FormParser)
    permission_classes = [AllowAny]
    lookup_field = "upload_id"

    def get_queryset(self):
        qs = Upload.objects.all()
        status_q = self.request.query_params.get("status")
        department = self.request.query_params.get("department")
        if status_q:
            qs = qs.filter(status=status_q)
        if department:
            qs = qs.filter(department=department)
        return qs

    def create(self, request, *args, **kwargs):
        f = request.FILES.get("file")
        department = request.data.get("department", "General")
        notes = request.data.get("notes", "")
        process_mode = request.data.get("process_mode") or "transform_gradebook"
        raw_config = request.data.get("process_config")
        process_config = {}
        if isinstance(raw_config, (str, bytes)):
            try:
                process_config = json.loads(raw_config)
            except json.JSONDecodeError:
                process_config = {}
        elif isinstance(raw_config, dict):
            process_config = raw_config

        if not f:
            return Response({"error": "file is required"}, status=status.HTTP_400_BAD_REQUEST)

        upload = Upload.objects.create(
            department=department,
            filename=f.name,
            mime_type=f.content_type or "",
            notes=notes,
            status="processing",
            process_mode=process_mode,
            process_config=process_config,
        )

        upload_dir = getattr(settings, "UPLOAD_DIR", "/app/storage/uploads")
        os.makedirs(upload_dir, exist_ok=True)

        # per-upload folder
        target_dir = os.path.join(upload_dir, str(upload.upload_id))
        os.makedirs(target_dir, exist_ok=True)

        file_path = os.path.join(target_dir, f.name)
        with open(file_path, "wb+") as dest:
            for chunk in f.chunks():
                dest.write(chunk)

        upload.file_path = file_path
        upload.save(update_fields=["file_path"])

        default_queue.enqueue(job_chain_standardize, str(upload.upload_id))
        return Response(UploadSerializer(upload).data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"])
    def retry(self, request, upload_id=None):
        upload = self.get_object()
        upload.status = "processing"
        upload.save(update_fields=["status"])
        default_queue.enqueue(job_chain_standardize, str(upload.upload_id))
        return Response({"status": "requeued", "upload_id": str(upload.upload_id)})


class JobRunViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = JobRun.objects.all()
    serializer_class = JobRunSerializer
    permission_classes = [AllowAny]
    lookup_field = "run_id"

    def get_queryset(self):
        qs = JobRun.objects.select_related("job", "upload")
        upload_id = self.request.query_params.get("upload_id")
        status_q = self.request.query_params.get("status")
        if upload_id:
            qs = qs.filter(upload__upload_id=upload_id)
        if status_q:
            qs = qs.filter(status=status_q)
        return qs


class JobViewSet(viewsets.ModelViewSet):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    permission_classes = [AllowAny]

    @action(detail=True, methods=["post"])
    def trigger(self, request, pk=None):
        job = self.get_object()
        payload = request.data if isinstance(request.data, dict) else {}
        enqueue_job_now(job, payload or None)
        return Response({"status": "queued", "job_id": job.id})


class IncidentViewSet(viewsets.ModelViewSet):
    queryset = Incident.objects.all()
    serializer_class = IncidentSerializer
    permission_classes = [AllowAny]
    lookup_field = "incident_id"

    def get_queryset(self):
        qs = Incident.objects.select_related("upload", "job_run", "matched_known_error")
        state = self.request.query_params.get("state")
        upload_id = self.request.query_params.get("upload_id")
        job_run_id = self.request.query_params.get("job_run")
        known = self.request.query_params.get("known")  # "true"/"false"
        if state:
            qs = qs.filter(state=state)
        if upload_id:
            qs = qs.filter(upload__upload_id=upload_id)
        if job_run_id:
            qs = qs.filter(job_run__run_id=job_run_id)
        if known == "true":
            qs = qs.filter(matched_known_error__isnull=False)
        if known == "false":
            qs = qs.filter(matched_known_error__isnull=True)
        return qs

    @action(detail=True, methods=["patch", "post"])
    def assign(self, request, incident_id=None):
        incident = self.get_object()
        incident.assignee = request.data.get("assignee")
        incident.state = "in_progress"
        _append_incident_event(
            incident,
            f"Assigned to {incident.assignee or 'unassigned'}",
            actor=request.data.get("actor") or "engine",
            notes=request.data.get("notes"),
        )
        incident.save(update_fields=["assignee", "state", "updated_at", "timeline"])
        return Response(IncidentSerializer(incident).data)

    @action(detail=True, methods=["patch", "post"])
    def resolve(self, request, incident_id=None):
        incident = self.get_object()
        resolved_by = request.data.get("resolved_by") or "engine"
        incident.root_cause = request.data.get("root_cause")
        incident.corrective_action = request.data.get("corrective_action")
        incident.resolution_report = request.data.get("resolution_report") or incident.resolution_report
        incident.state = "resolved"
        incident.resolved_by = resolved_by
        _append_incident_event(
            incident,
            "Incident resolved",
            actor=resolved_by,
            notes=incident.resolution_report or request.data.get("notes"),
        )
        incident.save(
            update_fields=[
                "root_cause",
                "corrective_action",
                "state",
                "updated_at",
                "resolution_report",
                "resolved_by",
                "timeline",
            ]
        )

        # auto-resolve tickets under this incident
        resolution_notes = incident.corrective_action or "Incident resolved"
        for ticket in incident.tickets.filter(status__in=["open", "in_progress"]):
            ticket.resolve(resolved_by="engine", resolution_type="automatic", notes=resolution_notes)

        return Response(IncidentSerializer(incident).data)

    @action(detail=True, methods=["patch", "post"])
    def analyze(self, request, incident_id=None):
        incident = self.get_object()
        incident.analysis_notes = request.data.get("analysis_notes") or incident.analysis_notes
        incident.impact_summary = request.data.get("impact_summary") or incident.impact_summary
        incident.severity = request.data.get("severity") or incident.severity
        incident.category = request.data.get("category") or incident.category
        actor = request.data.get("actor") or "engine"
        _append_incident_event(
            incident,
            "Analysis updated",
            actor=actor,
            notes=request.data.get("analysis_notes") or "Analysis details updated",
        )
        incident.save(
            update_fields=[
                "analysis_notes",
                "impact_summary",
                "severity",
                "category",
                "timeline",
                "updated_at",
            ]
        )
        return Response(IncidentSerializer(incident).data)

    @action(detail=True, methods=["post"])
    def retry(self, request, incident_id=None):
        incident = self.get_object()
        default_queue.enqueue(job_chain_standardize, str(incident.upload.upload_id))
        incident.state = "in_progress"
        incident.auto_retry_count = incident.auto_retry_count + 1
        _append_incident_event(
            incident,
            "Manual retry requested",
            actor=request.data.get("actor") or "engine",
            notes=request.data.get("notes"),
        )
        incident.save(update_fields=["state", "auto_retry_count", "timeline", "updated_at"])
        return Response({"status": "requeued", "incident_id": str(incident.incident_id)})

    @action(detail=True, methods=["post"])
    def archive(self, request, incident_id=None):
        incident = self.get_object()
        incident.archived_at = timezone.now()
        incident.state = "resolved"
        _append_incident_event(
            incident,
            "Incident archived",
            actor=request.data.get("actor") or "engine",
            notes=request.data.get("notes"),
        )
        incident.save(update_fields=["archived_at", "state", "timeline", "updated_at"])
        return Response(IncidentSerializer(incident).data)


class TicketViewSet(viewsets.ModelViewSet):
    queryset = Ticket.objects.all()
    serializer_class = TicketSerializer
    permission_classes = [AllowAny]
    lookup_field = "ticket_id"

    def get_queryset(self):
        qs = Ticket.objects.select_related("incident")
        status_q = self.request.query_params.get("status")
        source = self.request.query_params.get("source")
        if status_q:
            qs = qs.filter(status=status_q)
        if source:
            qs = qs.filter(source=source)
        return qs

    def create(self, request, *args, **kwargs):
        data = request.data.copy()
        data["source"] = "manual"
        serializer = self.get_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        ticket = serializer.save()

        tl = ticket.timeline or []
        tl.append({"timestamp": datetime.utcnow().isoformat(), "event": "Ticket created", "actor": "manual"})
        ticket.timeline = tl
        ticket.save(update_fields=["timeline", "updated_at"])

        return Response(TicketSerializer(ticket).data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"])
    def assign(self, request, ticket_id=None):
        ticket = self.get_object()
        assignee = request.data.get("assignee")
        if not assignee:
            return Response({"error": "assignee required"}, status=status.HTTP_400_BAD_REQUEST)

        ticket.assignee = assignee
        ticket.status = "in_progress"
        tl = ticket.timeline or []
        tl.append({"timestamp": datetime.utcnow().isoformat(), "event": f"Assigned to {assignee}", "actor": "engine"})
        ticket.timeline = tl
        ticket.save(update_fields=["assignee", "status", "timeline", "updated_at"])
        return Response(TicketSerializer(ticket).data)

    @action(detail=True, methods=["post"])
    def resolve(self, request, ticket_id=None):
        ticket = self.get_object()
        notes = request.data.get("resolution_notes", "")
        resolution_type = request.data.get("resolution_type", "manual")
        if ticket.status == "resolved":
            return Response({"error": "Ticket already resolved"}, status=status.HTTP_400_BAD_REQUEST)
        ticket.resolve(resolved_by="engine", resolution_type=resolution_type, notes=notes)
        return Response(TicketSerializer(ticket).data)


@api_view(["GET"])
@permission_classes([AllowAny])
def reports_summary(request):
    upload_id = request.query_params.get("upload_id")
    job_run_id = request.query_params.get("job_run_id")
    requested_format = request.query_params.get("format", "csv").lower()
    if requested_format not in {"csv", "pdf"}:
        requested_format = "csv"

    upload = None
    job_run = None

    if job_run_id:
        job_run = JobRun.objects.select_related("upload").filter(run_id=job_run_id).first()
        if job_run:
            if not job_run.upload_id:
                return Response(
                    {"error": "Job run not associated with an upload yet"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            upload = job_run.upload
            upload_id = str(job_run.upload_id)
        else:
            # If someone pasted an upload_id into job_run_id, fall back gracefully.
            upload = Upload.objects.filter(upload_id=job_run_id).first()
            if upload:
                upload_id = str(upload.upload_id)
                job_run_id = None
            else:
                return Response({"error": "Job run not found"}, status=status.HTTP_404_NOT_FOUND)

    if not upload_id:
        return Response({"error": "upload_id or job_run_id required"}, status=status.HTTP_400_BAD_REQUEST)

    if upload is None:
        try:
            upload = Upload.objects.get(upload_id=upload_id)
        except Upload.DoesNotExist:
            return Response({"error": "Upload not found"}, status=status.HTTP_404_NOT_FOUND)

    if job_run and job_run.status not in ["success", "failed"]:
        return Response(
            {"error": "Report still generating", "status": job_run.status},
            status=status.HTTP_409_CONFLICT,
        )

    mode = (upload.process_mode or "transform_gradebook").strip().lower()
    filename_prefix = "summary" if mode == "transform_gradebook" else "processed"

    if requested_format == "pdf":
        if upload.report_pdf:
            data = base64.b64decode(upload.report_pdf)
            resp = HttpResponse(data, content_type="application/pdf")
            resp["Content-Disposition"] = f'attachment; filename="{filename_prefix}-{upload.upload_id}.pdf"'
            return resp
        regenerate_report(upload)
        if upload.report_pdf:
            data = base64.b64decode(upload.report_pdf)
            resp = HttpResponse(data, content_type="application/pdf")
            resp["Content-Disposition"] = f'attachment; filename="{filename_prefix}-{upload.upload_id}.pdf"'
            return resp
        return Response({"error": "PDF not available yet"}, status=status.HTTP_404_NOT_FOUND)

    # Prefer the DB-stored report content when available.
    if upload.report_csv:
        resp = HttpResponse(upload.report_csv, content_type="text/csv")
        resp["Content-Disposition"] = f'attachment; filename="{filename_prefix}-{upload.upload_id}.csv"'
        return resp

    # Prefer the pipeline-generated processed/summary CSV, if it exists.
    export_dir = getattr(settings, "EXPORT_DIR", "/app/storage/exports")
    export_path = os.path.join(export_dir, f"{upload.upload_id}-{filename_prefix}.csv")

    def _try_path(path: str | None):
        if not path:
            return None
        try:
            if os.path.exists(path):
                with open(path, "r", newline="", encoding="utf-8") as f:
                    data = f.read()
                resp = HttpResponse(data, content_type="text/csv")
                resp["Content-Disposition"] = f'attachment; filename="{filename_prefix}-{upload.upload_id}.csv"'
                return resp
        except OSError:
            return None
        return None

    candidate_paths = []
    if upload.report_path:
        candidate_paths.append(upload.report_path)
    candidate_paths.append(export_path)

    for path in candidate_paths:
        resp = _try_path(path)
        if resp:
            return resp

    regenerated = regenerate_report(upload)
    if regenerated:
        resp = _try_path(regenerated)
        if resp:
            return resp

    # Fallback: simple one-row summary if the detailed export is missing.
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Field", "Value"])
    writer.writerow(["Upload ID", str(upload.upload_id)])
    writer.writerow(["Department", upload.department])
    writer.writerow(["Filename", upload.filename])
    writer.writerow(["Status", upload.status])
    writer.writerow(["Received At", upload.received_at.isoformat()])

    resp = HttpResponse(output.getvalue(), content_type="text/csv")
    resp["Content-Disposition"] = f'attachment; filename=\"report-{upload_id}.csv\"'
    return resp


@api_view(["GET"])
@permission_classes([AllowAny])
def api_health(request):
    health = {"django": "Healthy", "redis": "Unknown", "postgres": "Unknown", "rq_workers": "Unknown"}

    try:
        redis_conn.ping()
        health["redis"] = "Healthy"
    except Exception as exc:  # noqa: BLE001
        health["redis"] = f"Unhealthy ({exc.__class__.__name__})"

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
        health["postgres"] = "Healthy"
    except Exception as exc:  # noqa: BLE001
        health["postgres"] = f"Unhealthy ({exc.__class__.__name__})"

    try:
        workers = Worker.all(connection=redis_conn)
        if workers:
            health["rq_workers"] = f"Healthy ({len(workers)} online)"
        else:
            health["rq_workers"] = "Unhealthy (no workers registered)"
    except Exception as exc:  # noqa: BLE001
        health["rq_workers"] = f"Unknown ({exc.__class__.__name__})"

    return Response(health)


@api_view(["GET"])
@permission_classes([AllowAny])
def metrics_view(request):
    """
    Prometheus text-format metrics endpoint.

    This is intentionally *not* JSON – the frontend even checks that /api/metrics
    returns non‑JSON text so it can show a helpful warning.
    """
    body = get_metrics_data()
    # Prometheus text exposition format
    return HttpResponse(body, content_type="text/plain; version=0.0.4; charset=utf-8")


@api_view(["GET"])
@permission_classes([AllowAny])
def dashboard_metrics(request):
    """
    JSON KPIs for the dashboard cards.

    Kept separate from the Prometheus /api/metrics endpoint so that Grafana /
    Prometheus can scrape plain text while the UI can consume structured JSON.
    """
    now = timezone.now()
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    todays_uploads = Upload.objects.filter(received_at__gte=start_today).count()
    open_incidents = Incident.objects.filter(state__in=["open", "in_progress"]).count()
    open_tickets = Ticket.objects.filter(status__in=["open", "in_progress"]).count()

    return Response(
        {
            "kpis": {
                "todays_uploads": todays_uploads,
                "open_incidents": open_incidents,
                "open_tickets": open_tickets,
            }
        }
    )
