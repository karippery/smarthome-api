
from rest_framework import serializers
from .models import Sensor, SensorReading, DataQualityLog


# =============================================================================
# Raw Serializers
# =============================================================================

class SensorSerializer(serializers.ModelSerializer):
    class Meta:
        model  = Sensor
        fields = [
            "id", "sensor_id", "name", "location", "sensor_type",
            "status", "installed_at", "last_seen_at", "metadata",
            "created_at", "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class SensorReadingRawSerializer(serializers.ModelSerializer):
    class Meta:
        model  = SensorReading
        fields = [
            "id", "sensor_id_raw", "timestamp", "received_at",
            "temperature", "humidity", "co2_ppm", "battery_level",
            "is_raw", "is_validated", "quality_score", "quality_flags",
            "processing_notes", "created_at", "updated_at",
        ]
        read_only_fields = fields


class DataQualityLogSerializer(serializers.ModelSerializer):
    class Meta:
        model  = DataQualityLog
        fields = [
            "id", "sensor_id", "detection_type", "severity",
            "description", "context", "detected_at", "resolved_at",
        ]
        read_only_fields = fields


# =============================================================================
# Derived Serializers
# =============================================================================

class SensorReadingDerivedSerializer(serializers.ModelSerializer):
    has_issues  = serializers.SerializerMethodField()
    issue_count = serializers.SerializerMethodField()

    class Meta:
        model  = SensorReading
        fields = [
            "id", "sensor_id_raw", "timestamp",
            "temperature", "humidity", "co2_ppm", "battery_level",
            "quality_score", "quality_flags", "has_issues", "issue_count",
            "is_validated",
        ]
        read_only_fields = fields

    def get_has_issues(self, obj) -> bool:
        return len(obj.quality_flags) > 0

    def get_issue_count(self, obj) -> int:
        return len(obj.quality_flags)


class HourlyAggregationSerializer(serializers.Serializer):

    sensor_id         = serializers.CharField()
    hour              = serializers.DateTimeField()
    reading_count     = serializers.IntegerField()
    flagged_count     = serializers.IntegerField()
    avg_temperature   = serializers.FloatField(allow_null=True)
    avg_humidity      = serializers.FloatField(allow_null=True)
    avg_co2_ppm       = serializers.FloatField(allow_null=True)
    avg_battery       = serializers.FloatField(allow_null=True)
    avg_quality_score = serializers.FloatField(allow_null=True)
    min_quality_score = serializers.FloatField(allow_null=True)


class QualitySummarySerializer(serializers.Serializer):
    sensor_id         = serializers.CharField()
    total_readings    = serializers.IntegerField()
    flagged_readings  = serializers.IntegerField()
    clean_readings    = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()
    min_quality_score = serializers.FloatField()
    flag_breakdown    = serializers.DictField(child=serializers.IntegerField())
    latest_timestamp  = serializers.DateTimeField(allow_null=True)
    issues            = serializers.ListField(child=serializers.CharField())


class SensorInsightSerializer(serializers.Serializer):
    sensor_id       = serializers.CharField()
    health_status   = serializers.ChoiceField(choices=["healthy", "degraded", "critical"])
    health_reason   = serializers.CharField()
    avg_temperature = serializers.FloatField(allow_null=True)
    avg_humidity    = serializers.FloatField(allow_null=True)
    avg_co2_ppm     = serializers.FloatField(allow_null=True)
    latest_battery  = serializers.FloatField(allow_null=True)
    recent_flags    = serializers.ListField(child=serializers.CharField())
    reading_count   = serializers.IntegerField()
    gap_detected    = serializers.BooleanField()