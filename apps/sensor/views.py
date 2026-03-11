from datetime import timedelta
from collections import defaultdict
from typing import Any, Dict, List, Optional

from django.core.cache import cache
from django.db.models import Avg, Min, Max, Count, Q
from django.db.models.functions import TruncHour
from django.utils import timezone
from django.http import Http404

from rest_framework import generics, filters, status
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
# Helpers & Mixins
# =============================================================================

def round_optional(value: Optional[float], decimals: int = 2) -> Optional[float]:
    """Round to N decimals or return None. Explicit naming for clarity."""
    return round(value, decimals) if value is not None else None


class CacheMixin:
    """
    Standardizes caching logic for APIViews.
    Prevents key collision and reduces boilerplate.
    """
    CACHE_PREFIX = "sensor_api"
    CACHE_TTL = 300  # Default 5 minutes

    def get_cache_key(self, identifier: str, **kwargs) -> str:
        parts = [self.CACHE_PREFIX, self.__class__.__name__, identifier]
        for k, v in sorted(kwargs.items()):
            parts.append(f"{k}:{v}")
        return ":".join(str(p) for p in parts)

    def get_cached_response(self, key: str) -> Optional[Response]:
        data = cache.get(key)
        if data is not None:
            return Response(data)
        return None

    def set_cache_response(self, key: str, data: List[Dict[str, Any]], ttl: Optional[int] = None) -> Response:
        cache.set(key, data, ttl or self.CACHE_TTL)
        return Response(data)


# =============================================================================
# Reading List Views
# =============================================================================

class RawReadingListView(generics.ListAPIView):
    """
    Returns every stored reading exactly as imported.
    Exposes all ingestion metadata.
    """
    serializer_class = SensorReadingRawSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class = SensorReadingFilter
    ordering_fields = [
        "timestamp", "received_at", "quality_score",
        "temperature", "humidity", "co2_ppm", "battery_level", "sensor_id_raw",
    ]
    ordering = ["-timestamp"]

    def get_queryset(self):
        # Optimization: Only select related if the serializer actually uses the FK object.
        # If sensor_id_raw is a charfield on Reading, select_related is unnecessary.
        # Assuming FK exists based on original code.
        return SensorReading.objects.select_related("sensor").all()


class ProcessedReadingListView(generics.ListAPIView):
    """
    Readings enriched with computed quality annotations.
    Omits raw ingestion internals.
    """
    serializer_class = SensorReadingDerivedSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class = SensorReadingFilter
    ordering_fields = [
        "timestamp", "quality_score", "sensor_id_raw",
        "temperature", "humidity", "co2_ppm", "battery_level",
    ]
    ordering = ["-timestamp"]

    def get_queryset(self):
        return SensorReading.objects.all()


class ProcessedWithIssuesView(generics.ListAPIView):
    """
    Pre-filtered shortcut: only readings with at least one quality flag.
    """
    serializer_class = SensorReadingDerivedSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class = SensorReadingFilter
    ordering_fields = ["timestamp", "quality_score", "sensor_id_raw"]
    ordering = ["-timestamp"]

    def get_queryset(self):
        # Database level filtering is more efficient than Python filtering
        return SensorReading.objects.exclude(quality_flags=[])


class QualityLogListView(generics.ListAPIView):
    """
    Full audit trail: every detection event logged by the quality pipeline.
    """
    serializer_class = DataQualityLogSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_class = DataQualityLogFilter
    ordering_fields = ["detected_at", "severity", "detection_type", "sensor_id"]
    ordering = ["-detected_at"]

    def get_queryset(self):
        return DataQualityLog.objects.select_related("reading").all()


# =============================================================================
# Aggregation Views
# =============================================================================

class HourlyAggregationView(CacheMixin, APIView):
    """
    Hourly bucketed averages per sensor.
    Uses conditional aggregation to count flagged readings in a single query.
    """
    CACHE_TTL = 60 * 5

    def get(self, request, sensor_id: str):
        start = request.query_params.get("start")
        end = request.query_params.get("end")

        cache_key = self.get_cache_key(sensor_id, start=start, end=end)
        cached_response = self.get_cached_response(cache_key)
        if cached_response:
            return cached_response

        qs = SensorReading.objects.filter(sensor_id_raw__iexact=sensor_id)
        if start:
            qs = qs.filter(timestamp__gte=start)
        if end:
            qs = qs.filter(timestamp__lte=end)

        # Trade-off: Return empty list instead of 404 for consistency with other aggregations.
        # 404 implies the resource (sensor) doesn't exist, not just lack of data.
        if not qs.exists():
            return Response([], status=status.HTTP_200_OK)

        # Optimization: Single query using conditional aggregation for flagged_count
        buckets = (
            qs
            .annotate(hour=TruncHour("timestamp"))
            .values("hour")
            .annotate(
                avg_temperature=Avg("temperature"),
                avg_humidity=Avg("humidity"),
                avg_co2_ppm=Avg("co2_ppm"),
                avg_battery=Avg("battery_level"),
                avg_quality_score=Avg("quality_score"),
                min_quality_score=Min("quality_score"),
                reading_count=Count("id"),
                # Count only rows where quality_flags is not empty
                flagged_count=Count("id", filter=~Q(quality_flags=[])),
            )
            .order_by("hour")
        )

        results = [
            {
                "sensor_id": sensor_id.upper(),
                "hour": b["hour"],
                "reading_count": b["reading_count"],
                "flagged_count": b["flagged_count"],
                "avg_temperature": round_optional(b["avg_temperature"]),
                "avg_humidity": round_optional(b["avg_humidity"]),
                "avg_co2_ppm": round_optional(b["avg_co2_ppm"]),
                "avg_battery": round_optional(b["avg_battery"]),
                "avg_quality_score": round_optional(b["avg_quality_score"]),
                "min_quality_score": round_optional(b["min_quality_score"]),
            }
            for b in buckets
        ]

        # Serialize to ensure field consistency/validation
        serializer = HourlyAggregationSerializer(results, many=True)
        return self.set_cache_response(cache_key, serializer.data, self.CACHE_TTL)


class QualitySummaryView(CacheMixin, APIView):
    """
    Per-sensor aggregated quality statistics.
    Optimized to perform counts in DB rather than Python loops.
    """
    CACHE_TTL = 60 * 5

    def get(self, request):
        sensor_id_filter = request.query_params.get("sensor_id", "")
        cache_key = self.get_cache_key("summary", sensor_id=sensor_id_filter)
        
        cached_response = self.get_cached_response(cache_key)
        if cached_response:
            return cached_response

        qs = SensorReading.objects.all()
        if sensor_id_filter:
            qs = qs.filter(sensor_id_raw__iexact=sensor_id_filter)

        # Optimization: DB aggregation for counts and scores
        agg_rows = (
            qs
            .values("sensor_id_raw")
            .annotate(
                total=Count("id"),
                flagged=Count("id", filter=~Q(quality_flags=[])),
                avg_score=Avg("quality_score"),
                min_score=Min("quality_score"),
                latest=Max("timestamp"),
            )
            .order_by("sensor_id_raw")
        )

        # Data Thinking: 
        # Calculating flag breakdowns for ALL history in Python is memory intensive.
        # We limit this to a sample or accept the cost for smaller datasets.
        # Here we fetch only flags for breakdown to avoid loading full rows.
        flag_breakdowns = defaultdict(lambda: defaultdict(int))
        issue_types = defaultdict(set)

        # Only fetch necessary fields for breakdown to reduce memory
        flag_qs = qs.exclude(quality_flags=[]).values("sensor_id_raw", "quality_flags")
        for row in flag_qs:
            sid = row["sensor_id_raw"]
            for flag in row["quality_flags"]:
                flag_breakdowns[sid][flag] += 1
                issue_types[sid].add(flag)

        results = [
            {
                "sensor_id": row["sensor_id_raw"],
                "total_readings": row["total"],
                "flagged_readings": row["flagged"],
                "clean_readings": row["total"] - row["flagged"],
                "avg_quality_score": round_optional(row["avg_score"], 2) or 100.0,
                "min_quality_score": round_optional(row["min_score"], 2) or 100.0,
                "flag_breakdown": dict(flag_breakdowns[row["sensor_id_raw"]]),
                "latest_timestamp": row["latest"],
                "issues": sorted(list(issue_types[row["sensor_id_raw"]])),
            }
            for row in agg_rows
        ]

        serializer = QualitySummarySerializer(results, many=True)
        return self.set_cache_response(cache_key, serializer.data, self.CACHE_TTL)


class SensorHealthView(CacheMixin, APIView):
    """
    One-line health status per sensor.
    Includes safe input parsing and optimized queries.
    """
    CACHE_TTL = 60 * 2

    def get(self, request):
        sensor_id_filter = request.query_params.get("sensor_id", "")
        
        # Safety: Handle bad input for 'hours' gracefully
        try:
            hours = int(request.query_params.get("hours", 24))
        except (ValueError, TypeError):
            hours = 24

        cache_key = self.get_cache_key("health", sensor_id=sensor_id_filter, hours=hours)
        cached_response = self.get_cached_response(cache_key)
        if cached_response:
            return cached_response

        since = timezone.now() - timedelta(hours=hours)
        qs = SensorReading.objects.filter(timestamp__gte=since)
        if sensor_id_filter:
            qs = qs.filter(sensor_id_raw__iexact=sensor_id_filter)

        # Optimization: Single aggregation query for metrics
        agg_rows = list(
            qs
            .values("sensor_id_raw")
            .annotate(
                avg_score=Avg("quality_score"),
                avg_temp=Avg("temperature"),
                avg_humidity=Avg("humidity"),
                avg_co2=Avg("co2_ppm"),
                latest=Max("timestamp"),
                count=Count("id"),
            )
        )
        
        if not agg_rows:
            return self.set_cache_response(cache_key, [], self.CACHE_TTL)

        sensor_ids = [row["sensor_id_raw"] for row in agg_rows]

        # Optimization: Fetch latest battery per sensor efficiently
        battery_map = {}
        battery_qs = (
            SensorReading.objects
            .filter(sensor_id_raw__in=sensor_ids, battery_level__isnull=False)
            .order_by("sensor_id_raw", "-timestamp")
            .values("sensor_id_raw", "battery_level")
        )
        for row in battery_qs:
            if row["sensor_id_raw"] not in battery_map:
                battery_map[row["sensor_id_raw"]] = row["battery_level"]

        # Optimization: Fetch recent flags only (limit scope to prevent memory overflow)
        recent_flags_map = defaultdict(set)
        flag_qs = (
            qs
            .exclude(quality_flags=[])
            .values("sensor_id_raw", "quality_flags")
            .order_by("-timestamp")
        )
        # Limiting to 500 total rows across all sensors for recent flags to ensure performance
        for row in flag_qs[:500]: 
            recent_flags_map[row["sensor_id_raw"]].update(row["quality_flags"])

        results = []
        now = timezone.now()
        for row in agg_rows:
            sid = row["sensor_id_raw"]
            avg_score = row["avg_score"] or 100.0
            latest_ts = row["latest"]

            # Health Logic
            if avg_score >= 90:
                health = "healthy"
                reason = f"Avg quality score {avg_score:.1f}/100"
            elif avg_score >= 70:
                health = "degraded"
                reason = f"Quality issues detected — avg score {avg_score:.1f}/100"
            else:
                health = "critical"
                reason = f"Significant quality problems — avg score {avg_score:.1f}/100"

            # Gap Detection
            gap_detected = False
            if latest_ts and (now - latest_ts) > timedelta(hours=2):
                gap_detected = True
                health = "critical"
                hours_silent = int((now - latest_ts).total_seconds() / 3600)
                reason = f"No data received for {hours_silent}h"

            results.append({
                "sensor_id": sid,
                "health_status": health,
                "health_reason": reason,
                "avg_temperature": round_optional(row["avg_temp"]),
                "avg_humidity": round_optional(row["avg_humidity"]),
                "avg_co2_ppm": round_optional(row["avg_co2"]),
                "latest_battery": round_optional(battery_map.get(sid)),
                "recent_flags": sorted(list(recent_flags_map[sid])),
                "reading_count": row["count"],
                "gap_detected": gap_detected,
            })

        # Sort by health priority
        priority = {"critical": 0, "degraded": 1, "healthy": 2}
        results.sort(key=lambda r: priority[r["health_status"]])

        serializer = SensorInsightSerializer(results, many=True)
        return self.set_cache_response(cache_key, serializer.data, self.CACHE_TTL)