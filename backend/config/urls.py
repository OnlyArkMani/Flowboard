from django.contrib import admin
from django.http import HttpResponse
from django.urls import path, include
from rest_framework.routers import DefaultRouter

from core.views import (
    UploadViewSet,
    JobRunViewSet,
    JobViewSet,
    IncidentViewSet,
    TicketViewSet,
    auth_login,
    auth_forgot,
    auth_reset,
    auth_send_verification,
    auth_verify_email,
    auth_me,
    auth_logout,
    reports_summary,
    metrics_view,
    dashboard_metrics,
    api_health,
)

router = DefaultRouter()
router.register(r"uploads", UploadViewSet, basename="uploads")
router.register(r"job-runs", JobRunViewSet, basename="job-runs")
router.register(r"incidents", IncidentViewSet, basename="incidents")
router.register(r"tickets", TicketViewSet, basename="tickets")
router.register(r"jobs", JobViewSet, basename="jobs")


def index(request):
    return HttpResponse(
        "BatchOps backend is running. Use the React UI (Vite dev server) on port 5173, "
        "or call the API under /api/.",
        content_type="text/plain",
    )


urlpatterns = [
    path("", index),
    path("admin/", admin.site.urls),

    # API
    path("api/", include(router.urls)),
    path("api/auth/login", auth_login),
    path("api/auth/forgot", auth_forgot),
    path("api/auth/reset", auth_reset),
    path("api/auth/verify/send", auth_send_verification),
    path("api/auth/verify/confirm", auth_verify_email),
    path("api/auth/me", auth_me),
    path("api/auth/logout", auth_logout),
    path("api/health/", api_health),
    path("api/reports/summary", reports_summary),
    path("api/metrics", metrics_view),
    path("api/dashboard-metrics", dashboard_metrics),
    # alias to satisfy /api/dashboard/metrics/ shape from spec
    path("api/dashboard/metrics", dashboard_metrics),
]
