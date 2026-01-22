import uuid
from django.conf import settings
from django.db import models
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField
from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
    ROLE_CHOICES = [
        ("user", "User"),
        ("moderator", "Moderator"),
        ("admin", "Admin"),
    ]
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default="user")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    email_verified = models.BooleanField(default=False)

    class Meta:
        db_table = "core_user"

    def is_admin(self):
        return self.role == "admin" or self.is_superuser

    def is_moderator(self):
        return self.role in ["moderator", "admin"] or self.is_superuser

    def can_resolve_tickets(self):
        return self.is_moderator()

    def can_create_tickets(self):
        return True

    def can_assign_tickets(self):
        return self.is_moderator()


class PasswordResetRequest(models.Model):
    request_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="password_resets")
    code = models.CharField(max_length=12)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["user", "-created_at"]),
            models.Index(fields=["code"]),
        ]

    def __str__(self):
        return f"Password reset {self.request_id} ({self.user})"


class EmailVerificationRequest(models.Model):
    request_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="email_verifications")
    code = models.CharField(max_length=12)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["user", "-created_at"]),
            models.Index(fields=["code"]),
        ]

    def __str__(self):
        return f"Email verification {self.request_id} ({self.user})"


class Upload(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("processing", "Processing"),
        ("published", "Published"),
        ("failed", "Failed"),
    ]

    upload_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    department = models.CharField(max_length=100)
    filename = models.CharField(max_length=255)
    mime_type = models.CharField(max_length=100, blank=True, default="")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    received_at = models.DateTimeField(auto_now_add=True)
    notes = models.TextField(blank=True, null=True)
    file_path = models.CharField(max_length=500, blank=True, default="")
    report_path = models.CharField(max_length=500, blank=True, default="")
    report_generated_at = models.DateTimeField(null=True, blank=True)
    report_csv = models.TextField(blank=True, default="")
    report_meta = models.JSONField(default=dict, blank=True)
    report_pdf = models.TextField(blank=True, default="")
    process_mode = models.CharField(max_length=50, blank=True, default="transform_gradebook")
    process_config = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-received_at"]
        indexes = [
            models.Index(fields=["status", "department"]),
            models.Index(fields=["-received_at"]),
        ]

    def __str__(self):
        return f"{self.filename} ({self.upload_id})"


class DepartmentSource(models.Model):
    name = models.CharField(max_length=120, unique=True)
    code = models.CharField(max_length=50, unique=True)
    description = models.TextField(blank=True, default="")
    schedule_hint = models.CharField(max_length=120, blank=True, default="")
    active = models.BooleanField(default=True)
    last_ingested_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return f"{self.name} ({self.code})"


class DepartmentRecord(models.Model):
    source = models.ForeignKey(DepartmentSource, on_delete=models.CASCADE, related_name="records")
    student_id = models.CharField(max_length=40)
    student_name = models.CharField(max_length=120)
    class_name = models.CharField(max_length=50, blank=True, default="")
    score = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    attendance_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    status = models.CharField(max_length=40, blank=True, default="")
    recorded_at = models.DateTimeField(default=timezone.now)
    notes = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-recorded_at"]
        indexes = [
            models.Index(fields=["source", "-recorded_at"]),
            models.Index(fields=["student_id"]),
        ]

    def __str__(self):
        return f"{self.student_id} ({self.source.code})"


class Job(models.Model):
    JOB_TYPE_CHOICES = [
        ("shell", "Shell"),
        ("http", "HTTP"),
        ("python", "Python"),
    ]

    name = models.CharField(max_length=100, unique=True)
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default="python")
    config = models.JSONField(default=dict, blank=True)
    schedule_cron = models.CharField(max_length=100, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name


class JobRun(models.Model):
    STATUS_CHOICES = [
        ("queued", "Queued"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("retrying", "Retrying"),
    ]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="runs")
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE, related_name="job_runs", null=True, blank=True)

    run_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="queued")

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)

    exit_code = models.IntegerField(null=True, blank=True)
    logs = models.TextField(blank=True)
    details = models.JSONField(default=dict, blank=True)

    retry_count = models.IntegerField(default=0)
    max_retries = models.IntegerField(default=3)

    class Meta:
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["upload", "job"]),
            models.Index(fields=["status"]),
            models.Index(fields=["-started_at"]),
        ]

    def __str__(self):
        return f"{self.job.name} - {self.run_id}"


class KnownError(models.Model):
    """
    Known error library: regex pattern + fix payload
    """
    error_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=120, blank=True, default="")
    pattern = models.TextField(help_text="Regex pattern to match error")
    fix = models.JSONField(help_text="Fix config (type + params)", default=dict, blank=True)
    examples = ArrayField(models.TextField(), default=list, blank=True)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]

    def __str__(self):
        label = self.name or self.pattern
        return f"KnownError: {label[:50]}"


class Incident(models.Model):
    STATE_CHOICES = [
        ("open", "Open"),
        ("in_progress", "In Progress"),
        ("resolved", "Resolved"),
    ]
    SEVERITY_CHOICES = [
        ("low", "Low"),
        ("medium", "Medium"),
        ("high", "High"),
        ("critical", "Critical"),
    ]

    incident_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE, related_name="incidents")
    job_run = models.ForeignKey(JobRun, on_delete=models.SET_NULL, null=True, blank=True)

    # this is how we separate known vs unknown errors
    matched_known_error = models.ForeignKey(KnownError, on_delete=models.SET_NULL, null=True, blank=True)

    error = models.TextField()
    root_cause = models.TextField(blank=True, null=True)
    corrective_action = models.TextField(blank=True, null=True)
    impact_summary = models.TextField(blank=True, null=True)
    analysis_notes = models.TextField(blank=True, null=True)
    resolution_report = models.TextField(blank=True, null=True)

    severity = models.CharField(max_length=20, choices=SEVERITY_CHOICES, default="medium")
    category = models.CharField(max_length=100, blank=True, null=True)
    detection_source = models.CharField(max_length=100, blank=True, null=True)

    timeline = models.JSONField(default=list, blank=True)
    auto_retry_count = models.IntegerField(default=0)
    max_auto_retries = models.IntegerField(default=2)
    archived_at = models.DateTimeField(null=True, blank=True)
    resolved_by = models.CharField(max_length=100, blank=True, null=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    state = models.CharField(max_length=20, choices=STATE_CHOICES, default="open")
    assignee = models.CharField(max_length=100, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["state", "upload"]),
            models.Index(fields=["upload"]),
        ]

    def __str__(self):
        return f"Incident: {self.incident_id}"


class Ticket(models.Model):
    TICKET_STATUS_CHOICES = [
        ("open", "Open"),
        ("in_progress", "In Progress"),
        ("resolved", "Resolved"),
        ("closed", "Closed"),
    ]
    SOURCE_CHOICES = [
        ("system", "System"),
        ("manual", "Manual"),
    ]
    RESOLUTION_TYPE_CHOICES = [
        ("automatic", "Automatic"),
        ("manual", "Manual"),
    ]

    ticket_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    incident = models.ForeignKey(Incident, on_delete=models.CASCADE, related_name="tickets", null=True, blank=True)

    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default="system")
    status = models.CharField(max_length=20, choices=TICKET_STATUS_CHOICES, default="open")

    assignee = models.CharField(max_length=100, blank=True, null=True)
    created_by = models.CharField(max_length=100, blank=True, null=True)
    resolved_by = models.CharField(max_length=100, blank=True, null=True)

    resolution_type = models.CharField(max_length=20, choices=RESOLUTION_TYPE_CHOICES, blank=True, null=True)
    resolution_notes = models.TextField(blank=True, null=True)

    timeline = models.JSONField(default=list, blank=True)

    title = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "assignee"]),
            models.Index(fields=["source", "status"]),
        ]

    def __str__(self):
        title = self.title or f"Ticket {self.ticket_id}"
        return f"{title} ({self.ticket_id})"

    def resolve(self, resolved_by, resolution_type="manual", notes=""):
        self.status = "resolved"
        self.resolved_by = resolved_by
        self.resolution_type = resolution_type
        self.resolution_notes = notes
        self.resolved_at = timezone.now()

        tl = self.timeline or []
        tl.append(
            {
                "timestamp": timezone.now().isoformat(),
                "event": f"Ticket resolved by {resolved_by} ({resolution_type})",
                "actor": resolved_by,
                "notes": notes,
            }
        )
        self.timeline = tl

        if self.incident:
            self.incident.state = "resolved"
            self.incident.resolved_at = timezone.now()
            if notes:
                self.incident.corrective_action = notes
            self.incident.save()

        self.save()
