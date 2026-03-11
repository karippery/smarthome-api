from django.urls import path
from . import views

app_name = "sensor"

urlpatterns = [

    # ── RAW ──────────────────────────────────────────
    path(
        "readings/v1/raw/",
        views.RawReadingListView.as_view(),
        name="readings-raw",
    ),
    path(
        "readings/v1/processed/",
        views.ProcessedReadingListView.as_view(),
        name="readings-processed",
    ),
    path(
        "readings/v1/processed/with-issues/",
        views.ProcessedWithIssuesView.as_view(),
        name="readings-with-issues",
    ),
    path(
        "quality-logs/",
        views.QualityLogListView.as_view(),
        name="quality-logs",
    ),

    # ── DERIVED / AGGREGATIONS ────────────────────────
    path(
    "aggregations/v1/hourly/<str:sensor_id>/",
    views.HourlyAggregationView.as_view(),
    name="aggregations-hourly",
),
    path(
        "aggregations/v1/quality-summary/",
        views.QualitySummaryView.as_view(),
        name="aggregations-quality-summary",
    ),
    path(
        "aggregations/v1/sensor-health/",
        views.SensorHealthView.as_view(),
        name="aggregations-sensor-health",
    ),
]