from django.db import models


class Sensor(models.Model):
    """
    Sensor metadata and configuration.
    Separated from readings to allow lifecycle management without 
    affecting historical data storage.
    """
    
    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        INACTIVE = "inactive", "Inactive"
        MAINTENANCE = "maintenance", "Maintenance"
    
    sensor_id = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
        help_text="Unique sensor identifier (e.g., LIVING_ROOM_01)"
    )
    name = models.CharField(
        max_length=128,
        blank=True,
        help_text="Human-readable name"
    )
    location = models.CharField(
        max_length=128,
        db_index=True,
        help_text="Physical location (e.g., Living Room, Kitchen)"
    )
    sensor_type = models.CharField(
        max_length=64,
        default="environmental",
        help_text="Type: environmental, air_quality, hvac, contact"
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.ACTIVE,
        help_text="Current operational status"
    )
    installed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Installation date"
    )
    last_seen_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last successful data receipt"
    )
    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Flexible sensor-specific config (e.g., calibration offsets)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ["location", "sensor_id"]
        indexes = [
            models.Index(fields=["status", "sensor_type"]),
        ]
    
    def __str__(self):
        return f"{self.sensor_id} ({self.location})"


class SensorReading(models.Model):
    """
    Individual sensor reading with data quality tracking.
    
    Design Principle: Store everything raw. Validate later.
    This ensures no data loss during ingestion spikes or sensor malfunctions.
    """
    
    # Decoupled FK: Allows ingestion before sensor registration
    sensor = models.ForeignKey(
        Sensor,
        on_delete=models.SET_NULL,  # Preserve history if sensor deleted
        related_name="readings",
        null=True,
        blank=True,
        help_text="Linked sensor (nullable for raw imports)"
    )
    sensor_id_raw = models.CharField(
        max_length=64,
        db_index=True,
        help_text="Raw sensor_id from import (persistent link)"
    )
    
    # Timestamp
    timestamp = models.DateTimeField(
        db_index=True,
        help_text="Sensor reading timestamp"
    )
    received_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When this record was ingested"
    )
    
    # Environmental metrics (FloatField for performance & noise tolerance)
    # No strict DB validators: allow bad data to be stored and flagged instead
    temperature = models.FloatField(
        null=True,
        blank=True,
        help_text="Temperature in Celsius"
    )
    humidity = models.FloatField(
        null=True,
        blank=True,
        help_text="Relative humidity percentage"
    )
    co2_ppm = models.FloatField(
        null=True,
        blank=True,
        help_text="CO2 concentration in ppm"
    )
    battery_level = models.FloatField(
        null=True,
        blank=True,
        help_text="Battery level percentage"
    )
    
    # Data Quality Flags
    is_validated = models.BooleanField(
        default=False,
        db_index=True,
        help_text="Has passed all validation checks"
    )
    quality_score = models.FloatField(
        default=100.0,
        help_text="Overall quality score (0-100)"
    )
    quality_flags = models.JSONField(
        default=list,
        blank=True,
        help_text="List of quality issues: ['outlier', 'stuck_at', 'range_violation']"
    )
    
    # Processing metadata
    is_raw = models.BooleanField(
        default=True,
        help_text="True if unprocessed, False if normalized/derived"
    )
    processing_notes = models.TextField(
        blank=True,
        help_text="Notes from data processing pipeline"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ["-timestamp"]
        indexes = [
            models.Index(fields=["sensor_id_raw", "-timestamp"]),
            models.Index(fields=["is_validated", "-timestamp"]),
            models.Index(fields=["timestamp", "sensor_id_raw"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["sensor_id_raw", "timestamp"],
                name="unique_sensor_reading_per_timestamp"
            )
        ]
    
    def __str__(self):
        return f"{self.sensor_id_raw} @ {self.timestamp}"
    
    def calculate_quality_score(self) -> float:
        """
        Calculate quality score based on detected issues.
        Penalties are applied per flag.
        """
        score = 100.0
        penalties = {
            "range_violation": 30.0,
            "outlier": 20.0,
            "stuck_at": 25.0,
            "missing_field": 15.0,
        }
        for flag in self.quality_flags:
            score -= penalties.get(flag, 10.0)
        return max(0.0, score)
    
    def mark_validated(self):
        """Mark reading as validated after quality checks."""
        self.is_validated = True
        self.quality_score = self.calculate_quality_score()
        self.save(update_fields=["is_validated", "quality_score", "updated_at"])


class DataQualityLog(models.Model):
    """
    Audit log for data quality detections.
    Provides observability into why data was flagged.
    """
    
    class DetectionType(models.TextChoices):
        RANGE_CHECK = "range_check", "Range Validation"
        STUCK_AT = "stuck_at", "Stuck-at Detection"
        OUTLIER = "outlier", "Statistical Outlier"
        GAP = "gap", "Time Gap Detection"
        DUPLICATE = "duplicate", "Duplicate Detection"
    
    reading = models.ForeignKey(
        SensorReading,
        on_delete=models.SET_NULL,
        related_name="quality_logs",
        null=True,
        blank=True,
        help_text="Associated reading"
    )
    sensor_id = models.CharField(
        max_length=64,
        db_index=True,
        help_text="Sensor identifier"
    )
    detection_type = models.CharField(
        max_length=32,
        choices=DetectionType.choices,
        help_text="Type of quality check"
    )
    severity = models.CharField(
        max_length=16,
        choices=[
            ("info", "Info"),
            ("warning", "Warning"),
            ("error", "Error"),
            ("critical", "Critical"),
        ],
        default="warning",
        help_text="Severity level"
    )
    description = models.TextField(
        help_text="Human-readable description of the issue"
    )
    context = models.JSONField(
        default=dict,
        blank=True,
        help_text="Additional context (e.g., expected_range, detected_value)"
    )
    detected_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When this issue was detected"
    )
    resolved_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When this issue was resolved/reviewed"
    )
    
    class Meta:
        ordering = ["-detected_at"]
        indexes = [
            models.Index(fields=["sensor_id", "detection_type"]),
            models.Index(fields=["severity", "-detected_at"]),
        ]
    
    def __str__(self):
        return f"[{self.severity}] {self.detection_type} on {self.sensor_id}"