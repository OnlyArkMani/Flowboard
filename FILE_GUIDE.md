# BatchOps File Guide (Expanded, Simple Language)

This guide explains what each backend Python file and migration does, plus every frontend file. It also explains how batch schedules are created and executed. The explanations stay simple and beginner-friendly, but include enough detail for demos and documentation.

## Backend (Django + RQ)

### Entry point and configuration
- `backend/manage.py`
  - The main Django runner (migrate, runserver, createsuperuser).
  - Used when you run commands inside Docker or locally.

- `backend/requirements.txt`
  - List of Python libraries needed for the backend (Django, DRF, RQ, PDF tools, etc.).
  - Used by `pip install -r` in local setups and Docker builds.

- `backend/Dockerfile`
  - Builds the backend container image.
  - Installs Python dependencies and starts the backend in Docker.

- `backend/config/settings.py`
  - The global Django settings file.
  - Controls database connections, timezone, installed apps, email settings, static/media storage paths, and security flags.

- `backend/config/urls.py`
  - Top-level URL map for the backend server.
  - Routes `/api/...` endpoints to the `core` app, and exposes admin routes.

- `backend/config/wsgi.py`
  - Production entry point when running under a WSGI server.
  - Loads Django so the app can serve requests.

### Core application (backend/core)

- `backend/core/__init__.py`
  - Marks the `core` package.
  - No runtime logic.

- `backend/core/apps.py`
  - Django app configuration for `core`.
  - Loads the signals module so job schedules stay in sync.

- `backend/core/admin.py`
  - Registers models in Django admin.
  - Controls what tables show up in `/admin`.

- `backend/core/models.py`
  - Database models for the entire system:
    - `Upload`: uploaded files, status, and report output.
    - `Job`: batch schedules and job configuration.
    - `JobRun`: each execution of a job.
    - `Incident`: issues detected in the pipeline.
    - `Ticket`: ticket-style tracking tied to incidents.
    - `KnownError`: known incident patterns and fixes.
    - `DepartmentSource` / `DepartmentRecord`: simulated department data feeds.
    - `User`: custom user model with roles (admin/moderator/user).
  - Also includes helper methods (like adding timeline events).

- `backend/core/serializers.py`
  - Converts models to JSON for API responses.
  - Adds helpful computed fields (job name, upload filename, known error name).

- `backend/core/views.py`
  - The REST API implementation for uploads, jobs, runs, incidents, and tickets.
  - Handles key incident actions (assign, analyze, retry, resolve, archive).
  - Includes the dashboard metrics endpoints used by the frontend.

- `backend/core/urls.py`
  - URL routes for the `core` app.
  - Connects API endpoints to the viewsets/actions.

- `backend/core/permissions.py`
  - Role-based access control.
  - Decides what admins, moderators, and users can see or edit.

- `backend/core/metrics.py`
  - In-memory counters for basic metrics.
  - Exposes Prometheus-style metrics text for `/api/metrics`.

- `backend/core/signals.py`
  - Listens for changes to `Job` objects.
  - Auto-registers or removes cron schedules when jobs are saved/deleted.

### Queue and scheduling

- `backend/core/queues.py`
  - Connects to Redis.
  - Defines the RQ queue and scheduler used for background jobs.

- `backend/core/scheduler.py`
  - Registers cron schedules for `Job` records.
  - Enqueues jobs into the RQ queue when schedules fire.
  - Handles schedule canceling when jobs are deleted.

- `backend/core/workers.py`
  - Main batch engine.
  - Pipeline flow: standardize → validate → transform → generate summary → publish.
  - Handles CSV/PDF loading, aliasing columns, numeric detection, and report creation.
  - Creates incidents when failures occur.
  - Tries auto-remediation for known errors and auto-resolves when successful.

### Automation tasks

- `backend/core/automation/__init__.py`
  - Marks the automation package.

- `backend/core/automation/tasks.py`
  - Library of scheduled tasks used by jobs.
  - Includes:
    - Department ingest jobs (per department or all departments).
    - Attendance reminders (example scheduled job).
    - System status digest.
    - Backup simulation.
    - Cleanup/purge jobs.

### Management commands

- `backend/core/management/__init__.py`
  - Management package marker.

- `backend/core/management/commands/__init__.py`
  - Commands package marker.

- `backend/core/management/commands/rqworker.py`
  - Django command to start RQ workers.
  - Used inside Docker to run background jobs.

- `backend/core/management/commands/rqscheduler.py`
  - Django command to start RQ Scheduler.
  - This process fires cron schedules from Redis.

## Backend migrations (database history)

Each migration updates the database schema or seeds data.

- `backend/core/migrations/0001_initial.py`
  - Creates the base schema: users, uploads, jobs, job runs, incidents, tickets, known errors.
  - Adds indexes for performance (status, timestamps, foreign keys).

- `backend/core/migrations/0002_jobrun_details.py`
  - Adds a JSON `details` field on job runs.
  - Used to store per-step pipeline data.

- `backend/core/migrations/0003_upload_report_fields.py`
  - Adds report output metadata to uploads.
  - Stores report path and generation timestamp.

- `backend/core/migrations/0004_upload_report_storage.py`
  - Adds CSV report content and summary metadata as JSON.

- `backend/core/migrations/0005_incident_workflow_fields.py`
  - Adds fields for incident analysis: severity, category, impact summary, notes, timeline, retry counters, etc.

- `backend/core/migrations/0006_seed_default_jobs.py`
  - Seeds initial job templates.
  - Helps demo scheduling with real data.

- `backend/core/migrations/0007_upload_processing_plan.py`
  - Adds processing mode and config for uploads.
  - Supports transform, append, delete, and custom workflows.

- `backend/core/migrations/0008_upload_report_pdf.py`
  - Adds PDF report output storage.

- `backend/core/migrations/0009_department_sources.py`
  - Adds department sources and records tables.
  - Simulates departments as data providers (library, science, etc.).

- `backend/core/migrations/0010_seed_department_sources.py`
  - Seeds sample department sources.

- `backend/core/migrations/0011_seed_department_ingest_jobs.py`
  - Creates ingest jobs per department.

- `backend/core/migrations/0012_seed_all_departments_job.py`
  - Adds a job that ingests all departments into one dataset.

- `backend/core/migrations/0012_rename_core_deprecord_source_recorded_idx_core_depart_source__0ab2ee_idx_and_more.py`
  - Renames database indexes for department record performance.

- `backend/core/migrations/0013_merge_0012_branches.py`
  - Merge migration to reconcile two parallel 0012 branches.

- `backend/core/migrations/0014_password_reset_requests.py`
  - Adds a password reset request model.
  - Used for forgot-password flow.

- `backend/core/migrations/0015_email_verification_requests.py`
  - Adds email verification requests and `email_verified` field.

- `backend/core/migrations/0015_rename_core_passwo_user_id_d33f1f_idx_core_passwo_user_id_f12091_idx_and_more.py`
  - Renames indexes for password reset request table.

- `backend/core/migrations/0016_merge_0015_branches.py`
  - Merge migration for the two 0015 branches.

- `backend/core/migrations/0017_rename_core_emailv_user_id_3c2b2d_idx_core_emailv_user_id_63ceb9_idx_and_more.py`
  - Renames indexes for email verification table.

- `backend/core/migrations/0018_incident_resolved_at.py`
  - Adds `resolved_at` timestamp to incidents.
  - Used for resolution duration metrics.

## Frontend (React + Vite)

### Application entry and UI

- `frontend/index.html`
  - The HTML shell that loads the React app.
  - Contains the root `#app` element.

- `frontend/src/main.tsx`
  - Vite entry point.
  - Mounts React into the DOM and loads the main `App`.

- `frontend/src/App.tsx`
  - The main UI for all pages:
    - Batch Home (dashboard KPIs + role-based cards).
    - Batch Intake (uploads and queue).
    - Batch Runs (pipeline history and stage details).
    - Batch Issues (incidents, RCA workspace).
    - Batch Reports (CSV/PDF downloads).
    - Batch Schedules (cron job setup).
    - Auth and role-specific controls.
  - Contains major UI logic and API calls.

- `frontend/src/app.css`
  - Global styling for the entire UI.
  - Includes layout, cards, tables, modals, login page styling, and RCA workspace styling.

### Frontend utilities and configuration

- `frontend/src/lib/api.ts`
  - API helper for HTTP calls.
  - Handles token storage, base URLs, and wrappers for endpoints.

- `frontend/src/vite-env.d.ts`
  - TypeScript definitions for Vite.
  - Ensures TypeScript recognizes Vite environment variables.

- `frontend/package.json`
  - Frontend dependencies and scripts (`dev`, `build`, etc.).

- `frontend/package-lock.json`
  - Locked dependency tree for reproducible installs.

- `frontend/postcss.config.js`
  - PostCSS configuration used by Vite.

- `frontend/tailwind.config.js`
  - Tailwind configuration (if utilities are used).

## How batch schedules are created and executed (simple but detailed)

1) **Create a Job record**
   - Each schedule is stored in the `Job` table (`backend/core/models.py`).
   - The cron expression is saved in `schedule_cron`.

2) **Schedule registration**
   - When a job is saved, `backend/core/signals.py` triggers `register_cron_schedule`.
   - The schedule is registered into Redis by `backend/core/scheduler.py`.

3) **Scheduler process**
   - `rqscheduler` runs continuously.
   - It reads cron schedules and triggers jobs on time.

4) **Enqueue execution**
   - When the cron fires, the scheduler calls `enqueue_job_execution`.
   - That function enqueues `run_custom_job` into the Redis queue.

5) **Worker execution**
   - `rqworker` pulls jobs from Redis.
   - It runs the correct task: automation tasks, ingestion jobs, or custom jobs.

6) **Pipeline processing (for uploads)**
   - Uploads are processed by `job_chain_standardize` in `workers.py`.
   - Each stage updates job run logs and creates incidents on failure.

7) **Results and visibility**
   - Reports are saved (CSV + PDF).
   - The frontend reads job runs, incidents, and reports from API endpoints to display status.

### How scheduled data is enqueued into the pipeline (what “data” means)

1) **A schedule fires**
   - The cron expression stored on a `Job` is triggered by `rq-scheduler`.
   - `enqueue_job_execution` pushes the job into Redis.

2) **The worker executes the scheduled task**
   - The RQ worker runs `run_custom_job`, which calls a function from `backend/core/automation/tasks.py`.
   - Example: `schedule_file_ingest("Library")` or `schedule_all_department_ingest()`.

3) **The task creates an Upload**
   - The task queries `DepartmentRecord` rows from the database.
   - It writes those records into a new CSV file under `storage/uploads/<upload_id>/...`.
   - It creates a new `Upload` row in the database pointing to that CSV file.

4) **The Upload is enqueued for processing**
   - The task enqueues `job_chain_standardize(upload_id)` into Redis.
   - This is the real pipeline entry point (standardize → validate → transform → summary → publish).

So the “data” being enqueued is:
- A fresh CSV file built from department records.
- A matching Upload record that points to that file.

### Where you can see the ingested data

- **Frontend (preferred)**
  - Go to **Batch Intake** to see the new Upload record created by the schedule.
  - After processing finishes, go to **Batch Reports** to download the CSV/PDF output.

- **Database (raw source data)**
  - The scheduled ingest reads from the `DepartmentRecord` table.
  - You can inspect it in PostgreSQL:
    - `SELECT * FROM core_departmentrecord ORDER BY recorded_at DESC LIMIT 50;`

- **Filesystem (generated CSV)**
  - The scheduled task writes files to `backend/storage/uploads/<upload_id>/`.
  - The published report is saved under `backend/storage/exports/`.

## Notes
- This file documents code files only. Runtime storage folders and `node_modules` are excluded.
- If you add new backend or frontend files, add them here for completeness.
