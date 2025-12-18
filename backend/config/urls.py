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
        "Flowboard backend is running. Use the React UI (Vite dev server) on port 5173, "
        "or call the API under /api/.",
        content_type="text/plain",
    )


urlpatterns = [
    path("", index),
    path("admin/", admin.site.urls),

    # API
    path("api/", include(router.urls)),
    path("api/health/", api_health),
    path("api/reports/summary", reports_summary),
    path("api/metrics", metrics_view),
    path("api/dashboard-metrics", dashboard_metrics),
    # alias to satisfy /api/dashboard/metrics/ shape from spec
    path("api/dashboard/metrics", dashboard_metrics),
]
