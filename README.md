# BatchOps

Incident-resilient batch control room for schools and colleges. BatchOps ingests student files, standardizes and validates them, lets operators amend data on the fly, and publishes CSV/PDF reports while auto-triaging failures into incidents. The platform also orchestrates school-friendly automation jobs (attendance reminders, health checks, scrapers, backups) through an RQ worker + scheduler stack.

---

## Table of Contents

1. [Vision & Use Cases](#vision--use-cases)
2. [Architecture Overview](#architecture-overview)
3. [Domain & Data Model](#domain--data-model)
4. [Pipeline & Processing Plans](#pipeline--processing-plans)
5. [Frontend Experience](#frontend-experience)
6. [Automation & Schedules](#automation--schedules)
7. [Reporting & Downloads](#reporting--downloads)
8. [Deploying & Running](#deploying--running)
9. [Operations Runbook](#operations-runbook)
10. [Extending BatchOps](#extending-batchops)
11. [API Surface](#api-surface)

---

## Vision & Use Cases

BatchOps was built for campus operations desks that need a single control room for:

- **Transforming gradebooks** into standardized, school-branded summaries.
- **Appending or deleting records** from uploaded rosters before they enter downstream ERPs.
- **Handling more complex prompts** (custom rules/instructions) without writing code.
- **Scheduling unattended jobs**: bulk reminders, system-status digests, web scraping, ingestion jobs, database cleanup, and daily backups.
- **Maintaining incident hygiene**: known errors, auto-retries, RCA workflow, and archival for audits.

The platform keeps uploads cached locally until submitted, remembers the last page an operator visited, converts every timestamp to IST (+30 min offset per ops request), and preserves reports so reloads and navigation never lose context.

---

## Architecture Overview

| Layer | Technology | Notes |
| --- | --- | --- |
| Frontend | React 18 + Vite + TypeScript, single shell (`frontend/src/App.tsx`) | LocalStorage & IndexedDB power view persistence + upload queue. |
| API | Django 4.2 + Django REST Framework | `backend/core` app exposes all resources under `/api`. |
| Data | PostgreSQL 15 | Uploads, jobs, job runs, incidents, tickets, known errors. |
| Workers | RQ + Redis 7 | `job_chain_standardize` pipeline, automation jobs, and cron dispatch. |
| Scheduler | rq-scheduler | Registers cron expressions, enqueues jobs via Redis. |
| Messaging | Redis | Shared queue between API, worker, scheduler. |
| Observability | Prometheus scrape (`/api/metrics`), dashboard metrics, `/api/health/` | Health card highlights Redis/Postgres/RQ worker status. |

**docker-compose services**

1. `backend`: Django API (`python manage.py runserver`).
2. `worker`: executes uploads + automation (`python manage.py rqworker default`).
3. `scheduler`: cron loop (`python manage.py rqscheduler --interval 10`).
4. `db`: PostgreSQL with persistent volume.
5. `redis`: queue backend for worker + scheduler.
6. Frontend runs outside Compose via `npm run dev` and connects to backend at `http://localhost:8000` (configurable with `VITE_API_BASE_URL`).

---

## Domain & Data Model

| Model | Purpose |
| --- | --- |
| `Upload` | Stores file metadata, department, status, processing plan, CSV/PDF reports, summary metadata, and processing timestamps. |
| `Job` | Represents reusable automation, including callable path, args/kwargs, job type (`python`), cron expression, and config. |
| `JobRun` | Execution records (pipeline runs or scheduled jobs), step details, logs, exit codes, durations. |
| `KnownError` | Regex + remediation library for automatically tagging incidents with severity, RCA, corrective action, and auto-retry rules. |
| `Incident` | Full lifecycle (state, severity, root cause, corrective action, impact summary, timeline events, auto-retry counters, assignee). |
| `Ticket` | Action items linked to incidents (assign/resolve flow). |
| `Upload.process_config` | JSON field describing operator-selected processing plan (append/delete/custom instructions). |

PostgreSQL migrations also seed six default scheduled jobs and create `report_pdf` storage (base64) to serve PDFs directly via the API.

---

## Pipeline & Processing Plans

The worker (`backend/core/workers.py`) owns the **five-step pipeline**:

1. `standardize_results` - loads CSV/XLSX/PDF into pandas (with PDF heuristics to merge split names/IDs) and collects schema metrics.
2. `validate_results` - enforces presence of required columns (department aware), rows, and general sanity checks.
3. `transform_gradebook` - trims strings, attempts safe numeric coercion, and applies the operator-selected **processing plan**:
   - **Transform gradebook** (default) produces KPI-rich summary tables.
   - **Append record(s)** merges user-provided records (`process_config.records` list or single dict).
   - **Delete record(s)** removes rows by exact match using one or many rules.
   - **Custom rules** simply stores the instruction narrative so downstream humans know what to do.
4. `generate_summary` - builds descriptive stats for numeric columns plus metadata rows (upload ID, department, filename, schema).
5. `publish_results` - emits both CSV & PDF outputs. Transform plans become summary tables, while append/delete/custom plans publish the transformed dataset itself. Reports are kept on disk (`report_path`), in Postgres (`report_csv`, `report_pdf` base64), and timestamped for UI polling.

If any step fails, `_create_incident_and_ticket` matches known errors, seeds incident/ticket records, logs metrics, and optionally requeues retries through RQ.

---

## Frontend Experience

`frontend/src/App.tsx` renders the full SPA with a sidebar-driven layout:

- **Dashboard**: KPIs for today's uploads, incident counts, MTTR placeholder, pipeline overview, system health pulled from `/api/health/`, and Prometheus JSON guardrails.
- **Uploads**:
  - Queue up to **5 files** (CSV, Excel, PDF). IndexedDB keeps the queue even if the page reloads.
  - LocalStorage caches the most recent upload ID to auto-refresh job runs and incidents.
  - Operators pick the **processing plan** (transform, append, delete, custom) and provide record payloads or instructions. Multiple append/delete records are supported.
  - Upload rows show department, notes, processing state, linked job runs/incidents, and actions to download CSV/PDF outputs.
  - Upload form previews status, pipeline logs, and ensures queued files persist until the user explicitly removes them.
- **Job Runs**: Tabular history with durations, statuses, associated upload IDs, and modal details (per-step timings, logs). Useful for auditing both pipeline runs and scheduled jobs.
- **Incidents**: Full workflow with filters, severity badges, timeline, known error match, action buttons (assign/analyze/retry/archive/resolve), impact summary, resolution report, and auto-retry counters.
- **Reports**: Given a `job_run_id`, downloads CSV or PDF (polling `/api/reports/summary/` until published). Guards against failed or still-running jobs.
- **Jobs**: Admin console to create/update/trigger cron jobs. Users can set cron expressions (e.g., `*/3 * * * *` for every three minutes), specify Python callables, and pass JSON args. Inline helpers describe popular automation tasks (bulk emails, scraping, file ingest, cleanup, backups).

Global UX details: persistent navigation state, IST-only timestamps (with +30-minute correction), file size helpers, and consistent card/table styling from `frontend/src/app.css`.

---

## Automation & Schedules

`core/automation/tasks.py` contains ready-to-use callables, each of which can be wired to a cron schedule or triggered manually:

- `send_attendance_reminders`: bulk reminders/emails for absent students.
- `send_system_status_digest`: summarises health metrics and runs as a morning digest.
- `run_web_scrape`: fetches remote resources (e.g., government notices) for ingestion.
- `schedule_file_ingest`: ingests data from external S3/FTP sources at fixed times.
- `purge_old_records`: data hygiene and database cleanup.
- `run_daily_backup`: exports data snapshots to shared storage.

Cron expressions follow the standard 5-field format (minute hour day-of-month month day-of-week). The scheduler registers/de-registers entries whenever a job is created/updated or deleted, so the UI remains the single source of truth for automation.

Schools can leverage these primitives for broader operations: sending reminders, pushing system statuses, scraping attendance portals, processing nightly gradebooks, cleaning stale submissions, and running system health checks without manual intervention.

---

## Reporting & Downloads

- Reports live both on disk (`EXPORT_DIR`) and inside the Upload row as CSV text + base64 PDF.
- `/api/reports/summary/?upload_id=...&format=csv|pdf` streams the latest artifact; the frontend additionally caches report payloads client-side for instant downloads.
- PDF generation uses `fpdf` with equal-width columns, alternating row fills, and titles set to the original filename so audit PDFs look clean.
- CSV outputs differ based on the processing plan:
  - Transform plan -> summary table (upload metadata + numeric stats).
  - Append/delete/custom -> fully processed dataset reflecting the requested edits.

Operators can therefore publish a gradebook, double-check appended rows, or delete erroneous entries and still hand off a downloadable PDF/CSV pair for compliance.

---

## Deploying & Running

### Docker Compose (recommended)

```bash
docker compose up --build
```

This builds backend + frontend images, runs migrations, and starts API, worker, scheduler, Redis, and Postgres containers. Frontend can continue to run via Vite for faster development iterations.

### Local Development

**Backend**
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DJANGO_SETTINGS_MODULE=config.settings
export REDIS_URL=redis://localhost:6379/0
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

Run worker + scheduler in separate terminals:
```bash
python manage.py rqworker default
python manage.py rqscheduler --interval 10
```

**Frontend**
```bash
cd frontend
npm install
npm run dev -- --host --port 5173
```

### Environment Variables

- `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`
- `REDIS_URL`
- `DJANGO_SETTINGS_MODULE`
- Optional `EXPORT_DIR` (defaults to `/app/storage/exports`)
- Frontend: `VITE_API_BASE_URL`

---

## Operations Runbook

1. **Verify health**: `/api/health/` should report `Postgres: healthy`, `Redis: healthy`, and `RQ Workers: n online`. The dashboard health card echoes this.
2. **Workers idle?** Ensure both `rqworker` and `rqscheduler` processes are running. Cron jobs (e.g., `*/3 * * * *`) only enqueue when the scheduler is alive.
3. **Uploads disappear?** IndexedDB caches queued files; ensure the browser allows storage. Upload rows persist until the operator removes them manually.
4. **Reports missing?** Workers save CSV + PDF; the frontend polls up to 6 seconds before warning the user. Check worker logs for `publish_results` errors.
5. **PDF parsing issues?** `_load_df` merges identifier lines and aligns tokens, but malformed PDFs may still fail. Incidents will show `No table found in PDF pages` or `Required columns missing` along with matched known errors.
6. **Scheduling reliability**: After editing a job's cron expression, look for `scheduler` logs confirming registration; trigger the job manually via the UI to test connectivity.
7. **Incident workflow**: Operators can analyze (fill in impact summary, RCA, corrective action), retry (re-queue pipeline), resolve (mark timeline + closure), and archive (for future audits). All fields stay editable for richer reporting.
8. **Time zones**: All UI times go through `fmtDate` (IST only). Backend stores UTC; no changes needed server-side.
9. **Troubleshooting Redis**: Dashboard shows Redis status as `unhealthy` if `redis` container is down or `REDIS_URL` misconfigured. Fix the backing service, then refresh `/api/health/`.

---

## Extending BatchOps

- **New processing plans**: Extend `_apply_processing_plan` with additional modes (e.g., curve scores, anonymize, merge duplicates) and expose them in the Uploads form.
- **Automation ideas** for school ops:
  1. Bulk SMS/email alerts for attendance or fee reminders.
  2. Scheduled web scraping of exam portals for new results.
  3. Periodic ingestion of files dropped in shared drives.
  4. Database cleanup scripts (old tickets/incidents).
  5. System health checks with notifications on failure.
  6. Daily/weekly backups to cloud storage.
- **Observability**: Hook `/api/metrics` into Prometheus + Grafana dashboards. Add log shipping (e.g., Loki) for worker errors.
- **Integrations**: Swap `send_attendance_reminders` implementation with real email/SMS providers, or push incidents into helpdesk tools via `Ticket` webhooks.

---

## API Surface

| Endpoint | Method(s) | Highlights |
| --- | --- | --- |
| `/api/uploads/` | CRUD + `/retry/` | Stores processing plan, references job runs & incidents. |
| `/api/job-runs/` | GET/POST | Includes step logs, duration, upload reference. |
| `/api/jobs/` | CRUD + `/trigger/` | Manage Python jobs and cron expressions. |
| `/api/incidents/` | CRUD + `assign`, `analyze`, `resolve`, `retry`, `archive` | Tied to known errors, tickets, and uploads. |
| `/api/tickets/` | CRUD + `assign`, `resolve` | Incident-backed ticket workflow. |
| `/api/reports/summary/` | GET | Streams CSV/PDF for a given upload. |
| `/api/dashboard-metrics` | GET | Aggregated KPIs for dashboard. |
| `/api/metrics` | GET | Prometheus text-format counters. |
| `/api/health/` | GET | Redis/Postgres/RQ worker heartbeat. |

---

BatchOps now ships with exhaustive documentation for every service, workflow, and operational guardrail. Use this README as the foundation for slide decks, onboarding guides, or audits - it mirrors the current implementation across frontend, backend, workers, and automation.
