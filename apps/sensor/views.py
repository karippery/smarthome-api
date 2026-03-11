

from datetime import timedelta
from collections import defaultdict

from django.core.cache import cache
from django.db.models import Avg, Min, Max, Count
from django.db.models.functions import TruncHour
from django.utils import timezone

from rest_framework import generics, filters
from rest_framework.views import APIView
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend

from apps.sensor.filters import DataQualityLogFilter, SensorReadingFilter
from .models import SensorReading, DataQualityLog
from .serializers import (
    SensorReadingRawSerializer,
    SensorReadingDerivedSerializer,
    DataQualityLogSerializer,
    QualitySummarySerializer,
    SensorInsightSerializer,
    HourlyAggregationSerializer,
)


# =============================================================================
# Module-level helper
# =============================================================================

def _r(v):
    """Round to 2dp or return None. Module-level — avoids re-definition in loops."""
    return round(v, 2) if v is not None else None


# =============================================================================
# 1.  GET /api/readings/v1/raw/
# =============================================================================

class RawReadingListView(generics.ListAPIView):
    """
    Returns every stored reading exactly as imported.
    No post-processing, no derived fields.
    Exposes all ingestion metadata: received_at, is_raw, processing_notes.

    Filters:  ?sensor_id=  ?start=  ?end=  ?has_flags=  ?min_quality=  ?max_quality=
    Ordering: ?ordering=-timestamp  (default) | quality_score | sensor_id_raw,-timestamp
    """
    serializer_class = SensorReadingRawSerializer
    filter_backends  = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class  = SensorReadingFilter
    ordering_fields  = [
        "timestamp", "received_at", "quality_score",
        "temperature", "humidity", "co2_ppm", "battery_level", "sensor_id_raw",
    ]
    ordering = ["-timestamp"]

    def get_queryset(self):
        # select_related avoids N+1 if serializer accesses sensor FK
        return SensorReading.objects.select_related("sensor").all()


# =============================================================================
# 2.  GET /api/readings/v1/processed/
# =============================================================================

class ProcessedReadingListView(generics.ListAPIView):
    """
    Readings enriched with computed quality annotations:
      has_issues  (bool) — True if any quality flag present
      issue_count (int)  — number of flags on this reading

    Omits raw ingestion internals (received_at, is_raw, processing_notes).
    """
    serializer_class = SensorReadingDerivedSerializer
    filter_backends  = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class  = SensorReadingFilter
    ordering_fields  = [
        "timestamp", "quality_score", "sensor_id_raw",
        "temperature", "humidity", "co2_ppm", "battery_level",
    ]
    ordering = ["-timestamp"]

    def get_queryset(self):
        return SensorReading.objects.all()


# =============================================================================
# 3.  GET /api/readings/v1/processed/with-issues/
# =============================================================================

class ProcessedWithIssuesView(generics.ListAPIView):
    """
    Pre-filtered shortcut: only readings with at least one quality flag.
    Avoids requiring callers to know about ?has_flags=true.
    """
    serializer_class = SensorReadingDerivedSerializer
    filter_backends  = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class  = SensorReadingFilter
    ordering_fields  = ["timestamp", "quality_score", "sensor_id_raw"]
    ordering         = ["-timestamp"]

    def get_queryset(self):
        return SensorReading.objects.exclude(quality_flags=[])


# =============================================================================
# 4.  GET /api/quality-logs/v1/
# =============================================================================

class QualityLogListView(generics.ListAPIView):
    """
    Full audit trail: every detection event logged by the quality pipeline.

    Filters:  ?sensor_id=  ?detection_type=stuck_at  ?severity=warning  ?start=  ?end=
    Ordering: ?ordering=-detected_at  (default)
    """
    serializer_class = DataQualityLogSerializer
    filter_backends  = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class  = DataQualityLogFilter
    ordering_fields  = ["detected_at", "severity", "detection_type", "sensor_id"]
    ordering         = ["-detected_at"]

    def get_queryset(self):
        # select_related avoids N+1 if serializer accesses reading FK
        return DataQualityLog.objects.select_related("reading").all()


# =============================================================================
# 5.  GET /api/aggregations/v1/hourly/<sensor_id>/
# =============================================================================

class HourlyAggregationView(APIView):
    """
    Hourly bucketed averages per sensor: temp, humidity, co2, battery.
    Includes reading_count and flagged_count per bucket.
    Optional filters: ?start=  ?end=
    sensor_id is path param: /hourly/LIVING_ROOM_01/
    """
    CACHE_TTL = 60 * 5

    def get(self, request, sensor_id: str):
        start = request.query_params.get("start", "")
        end   = request.query_params.get("end", "")

        # ── Cache ────────────────────────────────────────────────────────────
        cache_key = f"hourly:{sensor_id.upper()}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return Response(cached)

        # ── Queryset ─────────────────────────────────────────────────────────
        qs = SensorReading.objects.filter(sensor_id_raw__iexact=sensor_id)
        if start:
            qs = qs.filter(timestamp__gte=start)
        if end:
            qs = qs.filter(timestamp__lte=end)

        if not qs.exists():
            return Response(
                {"error": f"No readings found for sensor '{sensor_id}'"},
                status=404,
            )

        # ── Query 1: hourly averages ──────────────────────────────────────────
        buckets = (
            qs
            .annotate(hour=TruncHour("timestamp"))
            .values("hour")
            .annotate(
                avg_temperature   = Avg("temperature"),
                avg_humidity      = Avg("humidity"),
                avg_co2_ppm       = Avg("co2_ppm"),
                avg_battery       = Avg("battery_level"),
                avg_quality_score = Avg("quality_score"),
                min_quality_score = Min("quality_score"),
                reading_count     = Count("id"),
            )
            .order_by("hour")
        )

        flagged_by_hour = {
            r["hour"]: r["count"]
            for r in (
                qs
                .exclude(quality_flags=[])
                .annotate(hour=TruncHour("timestamp"))
                .values("hour")
                .annotate(count=Count("id"))
            )
        }

        results = [
            {
                "sensor_id":         sensor_id.upper(),
                "hour":              b["hour"],
                "reading_count":     b["reading_count"],
                "flagged_count":     flagged_by_hour.get(b["hour"], 0),
                "avg_temperature":   _r(b["avg_temperature"]),
                "avg_humidity":      _r(b["avg_humidity"]),
                "avg_co2_ppm":       _r(b["avg_co2_ppm"]),
                "avg_battery":       _r(b["avg_battery"]),
                "avg_quality_score": _r(b["avg_quality_score"]),
                "min_quality_score": _r(b["min_quality_score"]),
            }
            for b in buckets
        ]

        data = HourlyAggregationSerializer(results, many=True).data
        cache.set(cache_key, data, self.CACHE_TTL)
        return Response(data)


# =============================================================================
# 6.  GET /api/aggregations/v1/quality-summary/
# =============================================================================

class QualitySummaryView(APIView):
    """
    Per-sensor aggregated quality statistics.

    Why APIView?
    Result is one row per sensor (grouped), not one row per SensorReading.
    DjangoFilterBackend breaks on values() querysets.

    Optional: ?sensor_id=LIVING_ROOM_01  (omit for all sensors)

    Cache: 5 min TTL.

    Queries: 2
      1. Per-sensor totals (COUNT, AVG, MIN, MAX)
      2. All flagged readings across all sensors — grouped in Python
    """
    CACHE_TTL = 60 * 5

    def get(self, request):
        sensor_id_filter = request.query_params.get("sensor_id", "")

        # ── Cache ────────────────────────────────────────────────────────────
        cache_key = f"quality_summary:{sensor_id_filter.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return Response(cached)

        qs = SensorReading.objects.all()
        if sensor_id_filter:
            qs = qs.filter(sensor_id_raw__iexact=sensor_id_filter)

        # ── Query 1: per-sensor totals ────────────────────────────────────────
        agg_rows = (
            qs
            .values("sensor_id_raw")
            .annotate(
                total     = Count("id"),
                avg_score = Avg("quality_score"),
                min_score = Min("quality_score"),
                latest    = Max("timestamp"),
            )
            .order_by("sensor_id_raw")
        )


        flagged_counts  = defaultdict(int)
        flag_breakdowns = defaultdict(lambda: defaultdict(int))
        issue_types     = defaultdict(set)

        for row in qs.exclude(quality_flags=[]).values("sensor_id_raw", "quality_flags"):
            sid = row["sensor_id_raw"]
            flagged_counts[sid] += 1
            for flag in row["quality_flags"]:
                flag_breakdowns[sid][flag] += 1
                issue_types[sid].add(flag)

        # ── Build response ────────────────────────────────────────────────────
        results = [
            {
                "sensor_id":         row["sensor_id_raw"],
                "total_readings":    row["total"],
                "flagged_readings":  flagged_counts[row["sensor_id_raw"]],
                "clean_readings":    row["total"] - flagged_counts[row["sensor_id_raw"]],
                "avg_quality_score": _r(row["avg_score"] or 100.0),
                "min_quality_score": _r(row["min_score"] or 100.0),
                "flag_breakdown":    dict(flag_breakdowns[row["sensor_id_raw"]]),
                "latest_timestamp":  row["latest"],
                "issues":            sorted(issue_types[row["sensor_id_raw"]]),
            }
            for row in agg_rows
        ]

        data = QualitySummarySerializer(results, many=True).data
        cache.set(cache_key, data, self.CACHE_TTL)
        return Response(data)


# =============================================================================
# 7.  GET /api/aggregations/v1/sensor-health/
# =============================================================================

class SensorHealthView(APIView):
    """
    One-line health status per sensor.

    Health classification (avg quality score over lookback window):
      healthy   → avg_score >= 90
      degraded  → avg_score >= 70
      critical  → avg_score <  70  OR  no data received for > 2 hours

    Optional: ?sensor_id=OFFICE_01  (omit for all sensors)
    Optional: ?hours=48             (lookback window, default 24)

    Response sorted: critical → degraded → healthy.
    Cache: 2 min TTL (shorter — health should feel near-live).

    """
    CACHE_TTL = 60 * 2

    def get(self, request):
        sensor_id_filter = request.query_params.get("sensor_id", "")
        hours = int(request.query_params.get("hours", 24))

        # ── Cache ────────────────────────────────────────────────────────────
        cache_key = f"sensor_health:{sensor_id_filter.upper()}:{hours}"
        cached = cache.get(cache_key)
        if cached is not None:
            return Response(cached)

        since = timezone.now() - timedelta(hours=hours)
        qs = SensorReading.objects.filter(timestamp__gte=since)
        if sensor_id_filter:
            qs = qs.filter(sensor_id_raw__iexact=sensor_id_filter)

        agg_rows = list(
            qs
            .values("sensor_id_raw")
            .annotate(
                avg_score    = Avg("quality_score"),
                avg_temp     = Avg("temperature"),
                avg_humidity = Avg("humidity"),
                avg_co2      = Avg("co2_ppm"),
                latest       = Max("timestamp"),
                count        = Count("id"),
            )
        )
        sensor_ids = [row["sensor_id_raw"] for row in agg_rows]

        battery_map = {}
        for row in (
            SensorReading.objects
            .filter(sensor_id_raw__in=sensor_ids, battery_level__isnull=False)
            .order_by("sensor_id_raw", "-timestamp")
            .values("sensor_id_raw", "battery_level")
        ):
            # First occurrence per sensor_id_raw = most recent (ordered -timestamp)
            if row["sensor_id_raw"] not in battery_map:
                battery_map[row["sensor_id_raw"]] = row["battery_level"]

        recent_flags_map = defaultdict(set)
        for row in (
            qs
            .exclude(quality_flags=[])
            .values("sensor_id_raw", "quality_flags")
            .order_by("-timestamp")[:500]
        ):
            recent_flags_map[row["sensor_id_raw"]].update(row["quality_flags"])

        # ── Build response ────────────────────────────────────────────────────
        results = []
        for row in agg_rows:
            sid       = row["sensor_id_raw"]
            avg_score = row["avg_score"] or 100.0
            latest_ts = row["latest"]

            if avg_score >= 90:
                health = "healthy"
                reason = f"Avg quality score {avg_score:.1f}/100"
            elif avg_score >= 70:
                health = "degraded"
                reason = f"Quality issues detected — avg score {avg_score:.1f}/100"
            else:
                health = "critical"
                reason = f"Significant quality problems — avg score {avg_score:.1f}/100"

            gap_detected = False
            if latest_ts and (timezone.now() - latest_ts) > timedelta(hours=2):
                gap_detected = True
                health = "critical"
                hours_silent = int((timezone.now() - latest_ts).total_seconds() / 3600)
                reason = f"No data received for {hours_silent}h"

            results.append({
                "sensor_id":       sid,
                "health_status":   health,
                "health_reason":   reason,
                "avg_temperature": _r(row["avg_temp"]),
                "avg_humidity":    _r(row["avg_humidity"]),
                "avg_co2_ppm":     _r(row["avg_co2"]),
                "latest_battery":  _r(battery_map.get(sid)),
                "recent_flags":    sorted(recent_flags_map[sid]),
                "reading_count":   row["count"],
                "gap_detected":    gap_detected,
            })

        priority = {"critical": 0, "degraded": 1, "healthy": 2}
        results.sort(key=lambda r: priority[r["health_status"]])

        data = SensorInsightSerializer(results, many=True).data
        cache.set(cache_key, data, self.CACHE_TTL)
        return Response(data)