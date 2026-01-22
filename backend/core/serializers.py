from rest_framework import serializers
from .models import Upload, Job, JobRun, Incident, Ticket, KnownError


class UploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = Upload
        fields = "__all__"


class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = "__all__"


class JobRunSerializer(serializers.ModelSerializer):
    job_name = serializers.CharField(source="job.name", read_only=True)
    upload_filename = serializers.CharField(source="upload.filename", read_only=True)

    class Meta:
        model = JobRun
        fields = "__all__"


class KnownErrorSerializer(serializers.ModelSerializer):
    class Meta:
        model = KnownError
        fields = "__all__"


class IncidentSerializer(serializers.ModelSerializer):
    upload_filename = serializers.CharField(source="upload.filename", read_only=True)
    job_name = serializers.CharField(source="job_run.job.name", read_only=True)
    matched_known_error_name = serializers.CharField(source="matched_known_error.name", read_only=True)
    is_known = serializers.SerializerMethodField()
    suggested_fix = serializers.SerializerMethodField()

    class Meta:
        model = Incident
        fields = "__all__"

    def get_is_known(self, obj):
        return obj.matched_known_error_id is not None

    def get_suggested_fix(self, obj):
        ke = getattr(obj, "matched_known_error", None)
        if ke and isinstance(ke.fix, dict):
            return ke.fix
        return None


class TicketSerializer(serializers.ModelSerializer):
    incident_error = serializers.CharField(source="incident.error", read_only=True)

    class Meta:
        model = Ticket
        fields = "__all__"
