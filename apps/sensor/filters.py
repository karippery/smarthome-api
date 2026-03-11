
import django_filters as df_filters
from django_filters.rest_framework import FilterSet

from .models import SensorReading, DataQualityLog



# =============================================================================
# Shared FilterSets
# =============================================================================

class SensorReadingFilter(FilterSet):
    """
    Reused across all reading endpoints.

    ?sensor_id=KITCHEN_01
    ?start=2024-05-01T00:00:00Z  &end=2024-05-01T23:59:59Z
    ?min_quality=0               &max_quality=80
    ?has_flags=true
    ?is_validated=false
    """
    sensor_id   = df_filters.CharFilter(field_name="sensor_id_raw", lookup_expr="iexact")
    start       = df_filters.DateTimeFilter(field_name="timestamp",   lookup_expr="gte")
    end         = df_filters.DateTimeFilter(field_name="timestamp",   lookup_expr="lte")
    min_quality = df_filters.NumberFilter(field_name="quality_score", lookup_expr="gte")
    max_quality = df_filters.NumberFilter(field_name="quality_score", lookup_expr="lte")
    is_validated = df_filters.BooleanFilter(field_name="is_validated")
    has_flags   = df_filters.BooleanFilter(method="filter_has_flags")

    def filter_has_flags(self, queryset, name, value):
        return queryset.exclude(quality_flags=[]) if value else queryset.filter(quality_flags=[])

    class Meta:
        model  = SensorReading
        fields = ["sensor_id", "start", "end", "min_quality", "max_quality", "is_validated"]


class DataQualityLogFilter(FilterSet):
    sensor_id      = df_filters.CharFilter(field_name="sensor_id",      lookup_expr="iexact")
    detection_type = df_filters.CharFilter(field_name="detection_type", lookup_expr="iexact")
    severity       = df_filters.CharFilter(field_name="severity",       lookup_expr="iexact")
    start          = df_filters.DateTimeFilter(field_name="detected_at", lookup_expr="gte")
    end            = df_filters.DateTimeFilter(field_name="detected_at", lookup_expr="lte")

    class Meta:
        model  = DataQualityLog
        fields = ["sensor_id", "detection_type", "severity"]

