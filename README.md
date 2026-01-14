# BatchOps

BatchOps is a batch processing control room for schools and colleges. It ingests departmental files, standardizes and validates them, applies operator selected changes, and publishes CSV/PDF reports. Failures are converted into incidents with a structured analysis and retry workflow, and scheduled jobs automate recurring campus operations.

---

## Table of Contents

1. What BatchOps Is
2. Architecture Overview
3. Core Workflow
4. Pipeline Stages (Deep Dive)
5. Processing Plans
6. Incident Workflow
7. Authentication and Roles
8. Email Verification and Password Reset
9. Data Model
10. Automation and Scheduling
11. Reports and Downloads
12. SMTP Email Delivery
13. Migrations (Full List)
14. Local Development
15. Environment Variables
16. API Surface (Summary)
17. Troubleshooting

---

## What BatchOps Is

BatchOps is a single interface where school operations teams can:

- upload files from departments,
- apply consistent processing steps,
- publish clean reports,
- track failures in an incident workflow,
- and run scheduled jobs for recurring tasks.

It is admin provisioned (no public sign up) and uses role based access control (RBAC).

---

## Architecture Overview

| Layer | Tech | Purpose |
| --- | --- | --- |
| Frontend | React + Vite + TypeScript | Single SPA in `frontend/src/App.tsx`. |
| API | Django 4.2 + DRF | REST endpoints in `backend/core/views.py`. |
| Data | PostgreSQL | Uploads, runs, incidents, users, schedules. |
| Queue | Redis + RQ | Background processing and retries. |
| Scheduler | rq-scheduler | Cron execution. |
| Email | SMTP | Verification and reset codes. |

`docker-compose.yml` runs:

- `backend`: Django API
- `worker`: RQ worker for pipeline and automations
- `scheduler`: cron dispatcher
- `redis`: queue
- `db`: Postgres

---

## Core Workflow

1. A user uploads files in Batch Intake.
2. Uploads are queued and processed by the worker.
3. The pipeline executes five stages in order.
4. If a stage fails, an incident is created and shown in Batch Issues.
5. When processing succeeds, reports are published as CSV and PDF.
6. Operators download results or retry failed items.

---

## Pipeline Stages (Deep Dive)

Pipeline implementation lives in `backend/core/workers.py`.

### 1) `standardize_results`

Purpose: normalize the file into a consistent dataframe.

Key actions:
- Load CSV/XLSX/PDF into pandas.
- Normalize column names and whitespace.
- For PDF, attempt table extraction and realign split identifiers.
- Emit a step log with schema details.

Failure conditions:
- Missing table from PDF.
- File cannot be parsed.

### 2) `validate_results`

Purpose: verify required columns and basic data quality.

Key actions:
- Confirm required columns exist.
- Check for empty or malformed key fields.
- Identify schema mismatches.

Failure conditions:
- Required columns missing.
- Critical fields empty.

### 3) `transform_gradebook`

Purpose: apply the operator selected processing plan.

Key actions:
- Trim strings.
- Attempt numeric coercion.
- Apply transform, append, delete, or custom rules.

Failure conditions:
- Invalid plan payloads.
- JSON parsing errors.

### 4) `generate_summary`

Purpose: generate descriptive statistics for reports.

Key actions:
- Build row and column counts.
- Compute numeric stats.
- Create summary rows for metadata.

### 5) `publish_results`

Purpose: produce and store outputs.

Key actions:
- Generate CSV (summary or processed dataset).
- Generate PDF with fpdf.
- Store CSV and PDF in DB and on disk.
- Mark upload as published.

---

## Processing Plans

Processing plans are chosen in Batch Intake and stored in:

- `Upload.process_mode`
- `Upload.process_config`

Available plans:

- Transform gradebook (default)  
  Produces summary tables instead of raw data.
- Append records  
  `process_config.records` accepts a list of objects.
- Delete records  
  Multiple rules supported using column/value matches.
- Custom rules  
  Stores a human instruction note; no automatic change.

---

## Incident Workflow

If any pipeline stage fails, the system creates:

1. `Incident` (primary record)
2. `Ticket` (action tracking)

Incident lifecycle:

- `open` -> `in_progress` -> `resolved`
- Optional: `archive`

Incident data includes:
- root cause
- corrective action
- impact summary
- analysis notes
- timeline events
- retry counters

Known errors are matched using regex patterns in `KnownError` and can auto tag severity, RCA, and fixes.

---

## Authentication and Roles

Authentication uses DRF Token auth with:
```
Authorization: Token <token>
```

Role matrix:

| Capability | Admin | Moderator | User |
| --- | --- | --- | --- |
| Upload files | Yes | Yes | Yes |
| View runs | Yes | Yes | Yes |
| View issues | Yes | Yes | Yes |
| Manage issues | Yes | Yes | No |
| Create/edit schedules | Yes | No | No |
| Run schedules | Yes | Yes | Yes |

Admins are created via Django admin and can create other accounts.

---

## Email Verification and Password Reset

Email verification is required for user and moderator accounts.

Flow:

1. Admin creates a user with email.
2. User clicks "Verify email" on the sign in page.
3. A code is sent via SMTP.
4. User confirms the code and can sign in.

Password reset:

1. User clicks "Forgot password".
2. A reset code is emailed.
3. User enters code and new password.

Models:
- `EmailVerificationRequest`
- `PasswordResetRequest`

---

## Data Model

| Model | Purpose |
| --- | --- |
| `User` | Authentication, role, email verification. |
| `Upload` | File metadata, status, process plan, reports. |
| `Job` | Scheduled automation definitions. |
| `JobRun` | Execution records and step details. |
| `KnownError` | Regex patterns for auto matching incidents. |
| `Incident` | Full RCA workflow and timeline. |
| `DepartmentSource` | Simulated department feeds. |
| `DepartmentRecord` | Sample records ingested by department jobs. |
| `EmailVerificationRequest` | Email verification codes. |
| `PasswordResetRequest` | Password reset codes. |

---

## Automation and Scheduling

Automations live in `backend/core/automation/tasks.py`.

Examples:
- `send_attendance_reminders`
- `send_system_status_digest`
- `run_web_scrape`
- `schedule_file_ingest`
- `purge_old_records`
- `run_daily_backup`

Cron format uses 5 fields:
```
minute hour day month weekday
```

The scheduler registers and updates cron entries whenever Jobs are created or updated.

---

## Reports and Downloads

Reports are stored in:

- Postgres (`report_csv`, `report_pdf`)
- Disk (`EXPORT_DIR`)

Endpoint:
```
GET /api/reports/summary/?upload_id=<id>&format=csv|pdf
```

Transform plan returns a summary table. Append/delete/custom returns the processed dataset.

---

## SMTP Email Delivery

BatchOps sends verification and password reset emails through SMTP.

Example Gmail SMTP settings:
```
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=you@gmail.com
EMAIL_HOST_PASSWORD=your_app_password
EMAIL_USE_TLS=1
DEFAULT_FROM_EMAIL=you@gmail.com
```

Notes:
- Use an app password for Gmail.
- Any SMTP provider can be used by changing these values.
- Check backend logs if email delivery fails.

---

## Migrations (Full List)

All migrations live in `backend/core/migrations`.

- `0001_initial.py`: base schema (users, uploads, jobs, incidents, tickets, known errors).
- `0002_jobrun_details.py`: adds job run details and logs.
- `0003_upload_report_fields.py`: adds report fields to uploads.
- `0004_upload_report_storage.py`: refines report storage metadata.
- `0005_incident_workflow_fields.py`: expands incident workflow fields.
- `0006_seed_default_jobs.py`: seeds default automation jobs.
- `0007_upload_processing_plan.py`: adds processing plan fields.
- `0008_upload_report_pdf.py`: adds report PDF storage.
- `0009_department_sources.py`: introduces department sources and records.
- `0010_seed_department_sources.py`: seeds department sample data.
- `0011_seed_department_ingest_jobs.py`: seeds department ingest jobs.
- `0012_seed_all_departments_job.py`: adds the combined ingest job.
- `0012_rename_core_deprecord_source_recorded_idx_core_depart_source__0ab2ee_idx_and_more.py`: index rename cleanup.
- `0013_merge_0012_branches.py`: merge migration for the dual 0012 branch.
- `0014_password_reset_requests.py`: adds password reset request model.
- `0015_email_verification_requests.py`: adds email verification model and email_verified flag.
- `0015_rename_core_passwo_user_id_d33f1f_idx_core_passwo_user_id_f12091_idx_and_more.py`: index rename cleanup.
- `0016_merge_0015_branches.py`: merge migration for the dual 0015 branch.
- `0017_rename_core_emailv_user_id_3c2b2d_idx_core_emailv_user_id_63ceb9_idx_and_more.py`: index rename cleanup for email verification.

If you see a migration conflict, it means two branches created migrations with the same number. Resolve by adding a merge migration (already included for current branches).

---

## Local Development

### Docker (recommended)
```
docker compose up -d --build
```

### Create admin
```
docker compose exec backend python manage.py createsuperuser
```

### Frontend
```
cd frontend
npm install
npm run dev -- --host --port 5173
```

If backend runs elsewhere:
```
VITE_API_BASE_URL=http://localhost:8000
```

---

## Environment Variables

Backend:

- `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`
- `REDIS_URL`
- `DJANGO_SETTINGS_MODULE`
- `EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_HOST_USER`, `EMAIL_HOST_PASSWORD`, `EMAIL_USE_TLS`, `DEFAULT_FROM_EMAIL`

Frontend:

- `VITE_API_BASE_URL`

---

## API Surface (Summary)

| Endpoint | Methods | Use |
| --- | --- | --- |
| `/api/auth/login` | POST | Sign in |
| `/api/auth/verify/send` | POST | Send verification code |
| `/api/auth/verify/confirm` | POST | Confirm verification |
| `/api/auth/forgot` | POST | Send reset code |
| `/api/auth/reset` | POST | Reset password |
| `/api/uploads/` | CRUD + `/retry/` | Uploads |
| `/api/job-runs/` | GET | Run history |
| `/api/jobs/` | CRUD + `/trigger/` | Schedules |
| `/api/incidents/` | CRUD + actions | Issues |
| `/api/reports/summary/` | GET | CSV/PDF downloads |
| `/api/health/` | GET | System health |

---

## Troubleshooting

- No admin styles in `/admin/`: set `DEBUG=1` in docker-compose and restart.
- No email received: check SMTP credentials and backend logs.
- Jobs not running: ensure `worker` and `scheduler` containers are running.
- Verification required: users/moderators must verify email before login.

---

BatchOps is designed to be both understandable for new readers and detailed enough for technical review. This README mirrors the current implementation across frontend, backend, workers, and automation.
