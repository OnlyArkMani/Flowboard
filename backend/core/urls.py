from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    UploadViewSet,
    JobRunViewSet,
    IncidentViewSet,
    TicketViewSet,
    api_health,
    api_metrics,
    reports_summary,
)

router = DefaultRouter()
router.register(r"uploads", UploadViewSet, basename="uploads")
router.register(r"job-runs", JobRunViewSet, basename="job-runs")
router.register(r"incidents", IncidentViewSet, basename="incidents")
router.register(r"tickets", TicketViewSet, basename="tickets")

urlpatterns = [
    path("", include(router.urls)),
    path("health/", api_health),
    path("metrics", api_metrics),  # NOTE: no trailing slash to match your frontend call
    path("reports/summary/", reports_summary),
]
