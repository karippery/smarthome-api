from datetime import datetime
from typing import List, Dict, Any
from dataclasses import dataclass

import polars as pl
from django.db import transaction

from apps.sensor.models import Sensor, SensorReading, DataQualityLog


# Quality Detection Thresholds
# Assumption: These are reasonable bounds for indoor environmental sensors.
# Documented for future tuning based on actual sensor specs.
QUALITY_THRESHOLDS = {
    "temperature": {"min": -10.0, "max": 50.0},  # Celsius
    "humidity": {"min": 0.0, "max": 100.0},  # Percentage
    "co2_ppm": {"min": 0.0, "max": 5000.0},  # ppm (5000+ = dangerous/unrealistic)
    "battery_level": {"min": 0.0, "max": 100.0},  # Percentage
}

# Stuck-at Detection: Number of consecutive identical values to flag
# Assumption: 5+ identical readings indicates a frozen sensor
STUCK_AT_THRESHOLD = 5

# Bulk insert batch size
# Trade-off: Larger = fewer queries, more memory. 1000 is safe for MVP.
BULK_BATCH_SIZE = 1000

# Quality penalty mapping (base check type → points deducted)
QUALITY_PENALTIES = {
    "stuck_at": 25.0,
    "outlier": 20.0,
    "range_violation": 30.0,
    "missing_field": 15.0,
}


# =============================================================================
# Data Classes for Type Safety
# =============================================================================

@dataclass
class QualityCheckResult:
    """Result of a single quality check."""
    sensor_id: str
    timestamp: datetime
    check_type: str  # lowercase: "stuck_at", "outlier", etc.
    passed: bool
    details: Dict[str, Any]


@dataclass
class IngestionStats:
    """Statistics from ingestion run."""
    total_rows: int
    inserted_rows: int
    skipped_duplicates: int
    quality_issues_found: int
    sensors_seen: List[str]


# =============================================================================
# Core Service: Data Ingestion
# =============================================================================

def load_csv_with_polars(file_path: str) -> pl.DataFrame:
    """
    High-speed CSV loading with Polars.
    Trade-off: Extra dependency, but justified for ETL workloads.
    """
    df = pl.read_csv(
        file_path,
        schema_overrides={
            "timestamp": pl.Datetime(time_zone="UTC"),
            "sensor_id": pl.Utf8,
            "temperature": pl.Float64,
            "humidity": pl.Float64,
            "co2_ppm": pl.Float64,
            "battery_level": pl.Float64,
        },
        ignore_errors=True,  # Don't fail on bad rows; log them instead
    )
    return df


def normalize_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize data types and handle missing values.
    
    Normalization steps:
    1. Ensure timestamp is UTC
    2. Strip whitespace from sensor_id
    3. Convert nulls to None for Django compatibility
    
    Why normalize here? Keeps Django models clean; ETL handles data quirks.
    """
    return df.with_columns([
        pl.col("timestamp").dt.replace_time_zone("UTC").alias("timestamp"),
        pl.col("sensor_id").str.strip_chars().alias("sensor_id"),
    ])


def dataframe_to_reading_objects(df: pl.DataFrame) -> List[SensorReading]:
    """
    Convert Polars DataFrame to Django model instances.
    
    Trade-off: Creates Python objects (memory overhead) but enables
    Django's bulk_create with validation hooks.
    
    Alternative: Use `bulk_create` with dicts → faster but less type safety.
    """
    readings = []
    
    for row in df.iter_rows(named=True):
        reading = SensorReading(
            sensor_id_raw=row["sensor_id"],
            timestamp=row["timestamp"],
            temperature=row.get("temperature"),
            humidity=row.get("humidity"),
            co2_ppm=row.get("co2_ppm"),
            battery_level=row.get("battery_level"),
            is_raw=True,
            is_validated=False,
            quality_score=100.0,
            quality_flags=[],
        )
        readings.append(reading)
    
    return readings


@transaction.atomic
def bulk_insert_readings(readings: List[SensorReading]) -> Dict[str, int]:
    """
    Atomic bulk insert with duplicate handling.
    
    Why atomic? Prevents partial imports on failure.
    Why ignore_conflicts? Handles re-runs gracefully (idempotent).
    
    Trade-off: Can't retrieve created PKs, but we don't need them for MVP.
    """
    stats = {
        "total": len(readings),
        "inserted": 0,
        "skipped": 0,
    }
    
    # Batch insert for memory efficiency
    for i in range(0, len(readings), BULK_BATCH_SIZE):
        batch = readings[i:i + BULK_BATCH_SIZE]
        created = SensorReading.objects.bulk_create(
            batch,
            ignore_conflicts=True,  # Respects unique constraint
            batch_size=BULK_BATCH_SIZE,
        )
        stats["inserted"] += len(created)
        stats["skipped"] += len(batch) - len(created)
    
    return stats


# =============================================================================
# Core Service: Quality Checks
# =============================================================================

def detect_stuck_at_values(sensor_id: str, field: str, window_size: int = STUCK_AT_THRESHOLD) -> List[QualityCheckResult]:
    """
    DETECTION STRATEGY 1: Stuck-at Detection
    
    Logic: Check if the last N readings have zero variance.
    This indicates a frozen/broken sensor.
    
    Trade-off: May flag legitimate stable conditions (e.g., AC maintaining temp).
    Mitigation: Quality flag is warning, not error; human review possible.
    """
    results = []
    
    # Get recent readings for this sensor
    recent_readings = SensorReading.objects.filter(
        sensor_id_raw=sensor_id,
        **{f"{field}__isnull": False}
    ).order_by("-timestamp")[:window_size + 1]
    
    if len(recent_readings) < window_size:
        return results  # Not enough data yet
    
    # Extract values
    values = [getattr(r, field) for r in recent_readings]
    
    # Check variance (all values identical?)
    if len(set(values)) == 1:
        results.append(QualityCheckResult(
            sensor_id=sensor_id,
            timestamp=recent_readings[0].timestamp,
            check_type="stuck_at",  # lowercase, matches TextChoices
            passed=False,
            details={
                "field": field,
                "consecutive_identical": len(values),
                "value": values[0],
                "threshold": window_size,
            }
        ))
    
    return results


def detect_outliers(sensor_id: str, field: str) -> List[QualityCheckResult]:
    """
    DETECTION STRATEGY 2: Range-based Outlier Detection
    
    Logic: Flag values outside physically possible ranges.
    Example: CO2 > 5000 ppm is dangerous/unrealistic for indoor spaces.
    
    Trade-off: Less adaptive than statistical methods.
    Future: Add z-score detection once we have historical baselines.
    """
    results = []
    thresholds = QUALITY_THRESHOLDS.get(field)
    
    if not thresholds:
        return results  # No thresholds defined for this field
    
    # Get unvalidated readings with this field populated
    readings = SensorReading.objects.filter(
        sensor_id_raw=sensor_id,
        **{f"{field}__isnull": False},
        is_validated=False,
    ).order_by("-timestamp")[:100]  # Limit for performance
    
    for reading in readings:
        value = getattr(reading, field)
        if value < thresholds["min"] or value > thresholds["max"]:
            results.append(QualityCheckResult(
                sensor_id=sensor_id,
                timestamp=reading.timestamp,
                check_type="outlier",  # lowercase, matches TextChoices
                passed=False,
                details={
                    "field": field,
                    "value": value,
                    "expected_range": [thresholds["min"], thresholds["max"]],
                }
            ))
    
    return results


def run_quality_checks(sensor_ids: List[str] = None) -> Dict[str, Any]:
    """
    Run all quality checks and update readings.
    
    Process:
    1. Get all unique sensor IDs (or filter to provided list)
    2. Run each detection strategy
    3. Update readings with quality flags
    4. Log detections for audit trail
    
    """
    if sensor_ids is None:
        sensor_ids = SensorReading.objects.values_list(
            "sensor_id_raw", flat=True
        ).distinct()
    
    stats = {
        "sensors_checked": 0,
        "issues_found": 0,
        "readings_updated": 0,
    }
    
    fields_to_check = ["temperature", "humidity", "co2_ppm", "battery_level"]
    
    for sensor_id in sensor_ids:
        stats["sensors_checked"] += 1
        all_results = []
        
        # Run both detection strategies
        for field in fields_to_check:
            stuck_results = detect_stuck_at_values(sensor_id, field)
            outlier_results = detect_outliers(sensor_id, field)
            all_results.extend(stuck_results)
            all_results.extend(outlier_results)
        
        # Apply flags to readings
        if all_results:
            stats["issues_found"] += len(all_results)
            _apply_quality_flags(all_results)
    
    return stats


def _apply_quality_flags(results: List[QualityCheckResult]) -> None:
    """
    Apply quality flags to readings and create audit logs.

    """
    for result in results:
        if not result.passed:
            # Find and update the reading
            reading = SensorReading.objects.filter(
                sensor_id_raw=result.sensor_id,
                timestamp=result.timestamp,
            ).first()
            
            if reading:
                flag_key = result.check_type  # e.g., "stuck_at", not "stuck_at_temperature"
                if flag_key not in reading.quality_flags:
                    reading.quality_flags.append(flag_key)
                    reading.quality_score = reading.calculate_quality_score()
                    reading.save(update_fields=["quality_flags", "quality_score", "updated_at"])
                
                DataQualityLog.objects.create(
                    reading=reading,
                    sensor_id=result.sensor_id,
                    detection_type=result.check_type,  # lowercase, no .upper()
                    severity="warning",
                    description=f"{result.check_type} detected in {result.details.get('field')}",
                    context=result.details,
                )


# =============================================================================
# Orchestration: Full Import Pipeline
# =============================================================================

def import_sensor_data(file_path: str, skip_quality: bool = False) -> IngestionStats:
    """
    Main entry point for sensor data import.
    
    Pipeline:
    1. Load CSV with Polars (fast)
    2. Normalize data types
    3. Convert to Django objects
    4. Bulk insert (atomic)
    5. Run quality checks (optional, post-processing)
    
    """
    # Step 1: Load
    df = load_csv_with_polars(file_path)
    
    # Step 2: Normalize
    df = normalize_dataframe(df)
    
    # Step 3: Convert
    readings = dataframe_to_reading_objects(df)
    
    # Step 4: Persist
    insert_stats = bulk_insert_readings(readings)
    
    # Step 5: Quality checks (optional)
    quality_stats = {"issues_found": 0}
    if not skip_quality:
        sensor_ids = df["sensor_id"].unique().to_list()
        quality_stats = run_quality_checks(sensor_ids)
        
        # Step 6: Update sensor metadata (only if quality checks ran)
        _update_sensor_metadata(sensor_ids, df["timestamp"].max())
    
    return IngestionStats(
        total_rows=insert_stats["total"],
        inserted_rows=insert_stats["inserted"],
        skipped_duplicates=insert_stats["skipped"],
        quality_issues_found=quality_stats["issues_found"],
        sensors_seen=df["sensor_id"].unique().to_list(),
    )


def _update_sensor_metadata(sensor_ids: List[str], latest_timestamp: datetime) -> None:
    """
    Update or create Sensor records with latest metadata.
    
    """
    for sensor_id in sensor_ids:
        sensor, created = Sensor.objects.update_or_create(
            sensor_id=sensor_id,
            defaults={
                "last_seen_at": latest_timestamp,
                "status": Sensor.Status.ACTIVE,
            }
        )
        if created:
            # New sensor: set minimal defaults
            sensor.name = sensor_id  # Use sensor_id as display name
            sensor.location = "Unassigned"  # Explicitly mark as needing assignment
            sensor.save(update_fields=["name", "location"])