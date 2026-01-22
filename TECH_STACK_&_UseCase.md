# BatchOps Tech Stack and Use Case

This document summarizes the technical stack used in BatchOps and the sample (hypothetical) use case the product demonstrates.

## Tech Stack

### Frontend
- React 18 (UI)
  - Component-based UI library that powers all screens (dashboard, intake, runs, issues, schedules).
  - Works with hooks to manage local state, forms, filters, and modal flows.
  - Enables reusable UI building blocks like cards, tables, and status pills.
- TypeScript (type safety)
  - Adds types to JavaScript to reduce runtime errors in the UI.
  - Helps ensure API data (uploads, incidents, runs) matches expected shapes.
  - Improves refactoring safety as the app grows.
- Vite (dev server and build)
  - Fast development server with hot module reloading.
  - Builds optimized production bundles for the frontend.
  - Handles environment variables and TypeScript out of the box.
- Axios (API client)
  - Consistent way to call backend endpoints (`/api/...`).
  - Manages default headers and auth tokens.
  - Simplifies error handling in async UI actions.
- Tailwind CSS + PostCSS (styling pipeline)
  - Tailwind gives utility classes for rapid layout and spacing.
  - PostCSS processes the final CSS (autoprefixing, compatibility).
  - Used alongside custom CSS for branding and layout polish.

### Backend / API
- Python 3.11+ (runtime)
  - The language used for the backend and data processing.
  - Strong data libraries (Pandas, CSV, PDF processing).
  - Fits well with task queues and automation workflows.
- Django 4.2 (web framework)
  - Core backend framework (models, database access, admin).
  - Provides authentication, permissions, and configuration.
  - Manages database migrations and ORM queries.
- Django REST Framework 3.15 (API layer)
  - Adds API endpoints on top of Django models.
  - Serializes data into JSON for the React frontend.
  - Supports viewsets, actions (assign, resolve), and pagination.
- django-cors-headers (CORS)
  - Enables browser calls from the frontend to the backend.
  - Controls which domains can access the API in development/production.

### Data and Storage
- PostgreSQL (primary database)
  - Stores uploads, job runs, incidents, users, and scheduling data.
  - Strong relational support for joins between uploads/runs/incidents.
  - Reliable for reporting and audit history.
- Redis (queue broker and cache)
  - In-memory store used by RQ queues and scheduler.
  - Keeps track of scheduled jobs and queued tasks.
  - Fast and lightweight for background processing.
- Local file storage for uploads and exports
  - Uploaded files saved to disk (`storage/uploads`).
  - Published CSV/PDF reports saved to `storage/exports`.
  - Simple for demos; can be replaced by S3 or cloud storage later.

### Processing / Automation
- RQ + rq-scheduler (background jobs and cron-like schedules)
  - RQ runs background tasks outside of HTTP requests.
  - rq-scheduler uses cron expressions to trigger jobs automatically.
  - This powers weekly/monthly department ingests and scheduled automation.
- Pandas + OpenPyXL (CSV/XLSX parsing and transforms)
  - Pandas loads CSV/XLSX into dataframes for validation and cleaning.
  - OpenPyXL supports Excel parsing for non-CSV sources.
  - Enables column normalization, type detection, and summaries.
- pdfplumber (PDF table extraction)
  - Extracts tables from PDFs into structured rows.
  - Used when departments upload PDF exports instead of CSV/Excel.
  - Integrates with column aliasing for PDF reliability.
- fpdf2 (PDF report generation)
  - Generates professional PDF reports after processing.
  - Supports table rendering, headers, and pagination.
  - Enables clean exports for non-technical users.

### Auth and Email
- Token authentication (DRF)
  - Uses token headers (`Authorization: Token <token>`) for API access.
  - The frontend stores the token after login and sends it on each request.
- Role-based access control (admin, moderator, user)
  - Admin: full access (schedules, incidents, system overview).
  - Moderator: incident analysis + resolution.
  - User: uploads + read-only visibility.
  - Permissions enforced in backend and reflected in UI.
- SMTP email support (verification + password reset)
  - Sends verification emails to new accounts.
  - Enables password reset flows with expiring codes.
  - Configurable for real SMTP providers or local Mailhog in dev.

### Dev / Ops
- Docker Compose for local services (backend, worker, db, redis, mailhog)
  - Launches the full stack with one command.
  - Includes Postgres, Redis, worker, scheduler, and email simulator.
  - Mimics production while remaining easy for demos.
- Prometheus-style metrics endpoint for health/monitoring
  - `/api/metrics` outputs counters for job runs and incidents.
  - Useful for health checks and dashboard insights.
  - Can be scraped by Prometheus or a monitoring tool later.

## Hypothetical Use Case (Schools/Colleges)

BatchOps is modeled as an operations hub for schools or colleges that run weekly or monthly data processing tasks across multiple departments.

### Example Scenario
- Departments (Admissions, Attendance, Fees, Library, Exams) export files weekly.
- Files are uploaded or scheduled for automatic ingestion.
- The pipeline standardizes, validates, and transforms the data.
- A clean report is published (CSV/PDF) and shared with staff.
- Issues trigger incidents with known-error matching and auto-remediation where possible.
- Moderators review and close incidents; admins manage schedules and assignments.

### What the demo highlights
- End-to-end batch automation from ingestion to publishing.
- Clear stage-by-stage visibility for processing runs.
- Incident workflows that reduce manual intervention.
- Role separation (admin vs moderator vs user) for governance.
