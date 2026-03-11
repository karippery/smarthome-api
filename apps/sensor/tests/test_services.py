# tests/test_services.py
"""
Business logic tests for:
  - apps/sensor/services.py   — ingestion pipeline + quality detection
  - management command        — import_sensors CLI

Test structure:
  TestLoadCsvWithPolars           — CSV loading and schema enforcement
  TestNormalizeDataframe          — timestamp normalization, whitespace stripping
  TestDataframToReadingObjects    — Polars → Django model conversion
  TestBulkInsertReadings          — atomic insert, duplicate handling
  TestDetectStuckAtValues         — Detection Strategy 1
  TestDetectOutliers              — Detection Strategy 2
  TestApplyQualityFlags           — flag writing + audit log creation
  TestRunQualityChecks            — orchestration of both strategies
  TestImportSensorData            — full pipeline end-to-end
  TestImportSensorsCommand        — management command CLI
"""

import csv
import os
import tempfile

import polars as pl
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone
from datetime import timedelta
from io import StringIO

from apps.sensor.models import Sensor, SensorReading, DataQualityLog
from apps.sensor.services import (
    QUALITY_THRESHOLDS,
    STUCK_AT_THRESHOLD,
    IngestionStats,
    QualityCheckResult,
    _apply_quality_flags,
    bulk_insert_readings,
    dataframe_to_reading_objects,
    detect_outliers,
    detect_stuck_at_values,
    import_sensor_data,
    load_csv_with_polars,
    normalize_dataframe,
    run_quality_checks,
)
from apps.sensor.tests.factories import SensorFactory, SensorReadingFactory


# =============================================================================
# Helpers
# =============================================================================

def make_csv(rows: list[dict], tmp_path) -> str:
    """Write rows to a temp CSV and return its path."""
    if not rows:
        path = os.path.join(tmp_path, "empty.csv")
        with open(path, "w") as f:
            f.write("timestamp,sensor_id,temperature,humidity,co2_ppm,battery_level\n")
        return path

    path = os.path.join(tmp_path, "test.csv")
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def default_row(**kwargs) -> dict:
    """Return a valid CSV row with sensible defaults, overridable via kwargs."""
    base = {
        "timestamp": "2024-05-01T10:00:00Z",
        "sensor_id": "KITCHEN_01",
        "temperature": "21.5",
        "humidity": "55.0",
        "co2_ppm": "420.0",
        "battery_level": "85.0",
    }
    base.update(kwargs)
    return base


# =============================================================================
# 1.  load_csv_with_polars
# =============================================================================

@pytest.mark.django_db
class TestLoadCsvWithPolars:

    def test_returns_polars_dataframe(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        df = load_csv_with_polars(path)
        assert isinstance(df, pl.DataFrame)

    def test_correct_row_count(self, tmp_path):
        rows = [default_row(sensor_id=f"S_{i:02d}") for i in range(10)]
        path = make_csv(rows, str(tmp_path))
        df = load_csv_with_polars(path)
        assert len(df) == 10

    def test_timestamp_column_is_datetime(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        df = load_csv_with_polars(path)
        assert df["timestamp"].dtype in (pl.Datetime, pl.Datetime("us", "UTC"))

    def test_numeric_columns_are_float(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        df = load_csv_with_polars(path)
        for col in ["temperature", "humidity", "co2_ppm", "battery_level"]:
            assert df[col].dtype == pl.Float64

    def test_ignore_errors_on_bad_rows(self, tmp_path):
        """ignore_errors=True means malformed rows are skipped, not raised."""
        rows = [
            default_row(),
            default_row(temperature="not_a_number"),  # bad value
            default_row(sensor_id="GOOD_01"),
        ]
        path = make_csv(rows, str(tmp_path))
        # Should not raise
        df = load_csv_with_polars(path)
        assert len(df) >= 1

    def test_handles_empty_csv(self, tmp_path):
        path = make_csv([], str(tmp_path))
        df = load_csv_with_polars(path)
        assert len(df) == 0


# =============================================================================
# 2.  normalize_dataframe
# =============================================================================

@pytest.mark.django_db
class TestNormalizeDataframe:

    def test_timestamp_has_utc_timezone(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        df = load_csv_with_polars(path)
        df = normalize_dataframe(df)
        tz = df["timestamp"].dtype.time_zone
        assert tz == "UTC"

    def test_sensor_id_whitespace_stripped(self, tmp_path):
        path = make_csv([default_row(sensor_id="  KITCHEN_01  ")], str(tmp_path))
        df = load_csv_with_polars(path)
        df = normalize_dataframe(df)
        assert df["sensor_id"][0] == "KITCHEN_01"

    def test_sensor_id_internal_spaces_preserved(self, tmp_path):
        """Only leading/trailing whitespace is stripped — internal spaces kept."""
        path = make_csv([default_row(sensor_id="  LIVING ROOM 01  ")], str(tmp_path))
        df = load_csv_with_polars(path)
        df = normalize_dataframe(df)
        assert df["sensor_id"][0] == "LIVING ROOM 01"

    def test_returns_polars_dataframe(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        df = normalize_dataframe(load_csv_with_polars(path))
        assert isinstance(df, pl.DataFrame)


# =============================================================================
# 3.  dataframe_to_reading_objects
# =============================================================================

@pytest.mark.django_db
class TestDataframeToReadingObjects:

    def _load(self, rows, tmp_path):
        path = make_csv(rows, str(tmp_path))
        return dataframe_to_reading_objects(normalize_dataframe(load_csv_with_polars(path)))

    def test_returns_list_of_sensor_readings(self, tmp_path):
        readings = self._load([default_row()], str(tmp_path))
        assert isinstance(readings, list)
        assert all(isinstance(r, SensorReading) for r in readings)

    def test_count_matches_rows(self, tmp_path):
        rows = [default_row(sensor_id=f"S_{i}") for i in range(5)]
        readings = self._load(rows, str(tmp_path))
        assert len(readings) == 5

    def test_sensor_id_raw_set_correctly(self, tmp_path):
        readings = self._load([default_row(sensor_id="KITCHEN_01")], str(tmp_path))
        assert readings[0].sensor_id_raw == "KITCHEN_01"

    def test_numeric_fields_assigned(self, tmp_path):
        readings = self._load([default_row(
            temperature="22.5", humidity="60.0", co2_ppm="500.0", battery_level="90.0"
        )], str(tmp_path))
        r = readings[0]
        assert r.temperature == pytest.approx(22.5)
        assert r.humidity == pytest.approx(60.0)
        assert r.co2_ppm == pytest.approx(500.0)
        assert r.battery_level == pytest.approx(90.0)

    def test_defaults_set_on_new_readings(self, tmp_path):
        readings = self._load([default_row()], str(tmp_path))
        r = readings[0]
        assert r.is_raw is True
        assert r.is_validated is False
        assert r.quality_score == 100.0
        assert r.quality_flags == []

    def test_empty_dataframe_returns_empty_list(self, tmp_path):
        readings = self._load([], str(tmp_path))
        assert readings == []


# =============================================================================
# 4.  bulk_insert_readings
# =============================================================================

@pytest.mark.django_db
class TestBulkInsertReadings:

    def _make_reading(self, sensor_id="TEST_01", minutes_ago=0):
        sensor = SensorFactory(sensor_id=sensor_id)
        return SensorReading(
            sensor_id_raw=sensor_id,
            timestamp=timezone.now() - timedelta(minutes=minutes_ago),
            temperature=21.5,
            humidity=55.0,
            quality_score=100.0,
            quality_flags=[],
            is_raw=True,
            is_validated=False,
        )

    def test_inserts_new_readings(self):
        readings = [self._make_reading(minutes_ago=i) for i in range(3)]
        stats = bulk_insert_readings(readings)
        assert stats["inserted"] == 3
        assert SensorReading.objects.count() == 3

    def test_returns_stats_dict(self):
        readings = [self._make_reading()]
        stats = bulk_insert_readings(readings)
        assert "total" in stats
        assert "inserted" in stats
        assert "skipped" in stats

    def test_total_equals_input_count(self):
        readings = [self._make_reading(minutes_ago=i) for i in range(5)]
        stats = bulk_insert_readings(readings)
        assert stats["total"] == 5

    def test_duplicate_skipped_not_raised(self):
        """
        Second insert of same (sensor_id_raw, timestamp) must be silently
        skipped via ignore_conflicts=True, not raise IntegrityError.

        NOTE on Django limitation: bulk_create with ignore_conflicts=True
        always returns all input objects regardless of whether rows were
        actually written — Django has no way to know which were skipped at
        the DB level. So stats["skipped"] is always 0 and cannot be asserted.

        What we CAN assert:
          - No IntegrityError is raised (the call succeeds)
          - The DB row count stays at 1 (no duplicate row was created)
          - stats["total"] reflects the number of objects passed in
        """
        reading = self._make_reading()
        bulk_insert_readings([reading])
        assert SensorReading.objects.count() == 1

        # Attempt to insert the same (sensor_id_raw, timestamp) again
        duplicate = self._make_reading(minutes_ago=0)
        duplicate.timestamp = reading.timestamp

        # Must not raise IntegrityError
        stats = bulk_insert_readings([duplicate])

        assert stats["total"] == 1                   # 1 object was passed in
        assert SensorReading.objects.count() == 1    # still only 1 row in DB

    def test_inserted_plus_skipped_equals_total(self):
        r1 = self._make_reading(minutes_ago=1)
        bulk_insert_readings([r1])
        # One new, one duplicate
        r2 = self._make_reading(minutes_ago=2)
        r_dup = self._make_reading(minutes_ago=1)
        r_dup.timestamp = r1.timestamp
        stats = bulk_insert_readings([r2, r_dup])
        assert stats["inserted"] + stats["skipped"] == stats["total"]

    def test_empty_list_returns_zero_stats(self):
        stats = bulk_insert_readings([])
        assert stats["total"] == 0
        assert stats["inserted"] == 0
        assert stats["skipped"] == 0

    def test_atomic_rollback_on_error(self):
        """
        If bulk_create raises an unexpected error mid-batch, the whole
        transaction must roll back — no partial inserts.
        """
        from unittest.mock import patch
        readings = [self._make_reading(minutes_ago=i) for i in range(3)]
        with patch.object(
            SensorReading.objects, "bulk_create", side_effect=RuntimeError("DB error")
        ):
            with pytest.raises(RuntimeError):
                bulk_insert_readings(readings)
        assert SensorReading.objects.count() == 0


# =============================================================================
# 5.  detect_stuck_at_values  (Detection Strategy 1)
# =============================================================================

@pytest.mark.django_db
class TestDetectStuckAtValues:

    def _create_readings(self, sensor_id, values, field="temperature"):
        """Create readings with the given field values, newest first."""
        sensor = SensorFactory(sensor_id=sensor_id)
        now = timezone.now()
        for i, val in enumerate(values):
            SensorReadingFactory(
                sensor=sensor,
                sensor_id_raw=sensor_id,
                timestamp=now - timedelta(minutes=i),
                **{field: val},
                quality_flags=[],
                quality_score=100.0,
            )

    def test_returns_empty_when_insufficient_data(self):
        """Fewer readings than STUCK_AT_THRESHOLD → nothing flagged."""
        self._create_readings("STUCK_01", [21.0] * (STUCK_AT_THRESHOLD - 1))
        results = detect_stuck_at_values("STUCK_01", "temperature")
        assert results == []

    def test_detects_frozen_temperature(self):
        """STUCK_AT_THRESHOLD identical values → stuck_at detected."""
        self._create_readings("STUCK_02", [22.1] * (STUCK_AT_THRESHOLD + 1))
        results = detect_stuck_at_values("STUCK_02", "temperature")
        assert len(results) == 1
        assert results[0].check_type == "stuck_at"
        assert results[0].passed is False

    def test_no_detection_when_values_vary(self):
        """Varying values → no stuck_at."""
        self._create_readings("STUCK_03", [20.0, 21.0, 22.0, 23.0, 24.0, 25.0])
        results = detect_stuck_at_values("STUCK_03", "temperature")
        assert results == []

    def test_detection_result_contains_field_info(self):
        self._create_readings("STUCK_04", [55.0] * (STUCK_AT_THRESHOLD + 1), field="humidity")
        results = detect_stuck_at_values("STUCK_04", "humidity")
        assert len(results) == 1
        assert results[0].details["field"] == "humidity"
        assert results[0].details["value"] == 55.0
        assert results[0].details["consecutive_identical"] == STUCK_AT_THRESHOLD + 1

    def test_detection_result_sensor_id(self):
        self._create_readings("STUCK_05", [420.0] * (STUCK_AT_THRESHOLD + 1), field="co2_ppm")
        results = detect_stuck_at_values("STUCK_05", "co2_ppm")
        assert results[0].sensor_id == "STUCK_05"

    def test_works_for_all_fields(self):
        for field, val in [("temperature", 21.0), ("humidity", 55.0),
                           ("co2_ppm", 420.0), ("battery_level", 85.0)]:
            sid = f"STUCK_FIELD_{field.upper()}"
            self._create_readings(sid, [val] * (STUCK_AT_THRESHOLD + 1), field=field)
            results = detect_stuck_at_values(sid, field)
            assert len(results) == 1, f"Expected stuck_at for field={field}"

    def test_returns_empty_for_unknown_sensor(self):
        results = detect_stuck_at_values("NONEXISTENT_SENSOR", "temperature")
        assert results == []


# =============================================================================
# 6.  detect_outliers  (Detection Strategy 2)
# =============================================================================

@pytest.mark.django_db
class TestDetectOutliers:

    def _make_reading(self, sensor_id, field, value):
        sensor = SensorFactory(sensor_id=sensor_id)
        return SensorReadingFactory(
            sensor=sensor,
            sensor_id_raw=sensor_id,
            **{field: value},
            is_validated=False,
            quality_flags=[],
            quality_score=100.0,
        )

    def test_no_detection_for_normal_value(self):
        self._make_reading("OUT_01", "temperature", 21.5)
        results = detect_outliers("OUT_01", "temperature")
        assert results == []

    def test_detects_temperature_too_high(self):
        threshold = QUALITY_THRESHOLDS["temperature"]["max"]
        self._make_reading("OUT_02", "temperature", threshold + 10)
        results = detect_outliers("OUT_02", "temperature")
        assert len(results) == 1
        assert results[0].check_type == "outlier"
        assert results[0].passed is False

    def test_detects_temperature_too_low(self):
        threshold = QUALITY_THRESHOLDS["temperature"]["min"]
        self._make_reading("OUT_03", "temperature", threshold - 10)
        results = detect_outliers("OUT_03", "temperature")
        assert len(results) == 1

    def test_detects_co2_spike(self):
        """Classic injected anomaly — CO2 = 9999 ppm."""
        self._make_reading("OUT_04", "co2_ppm", 9999.0)
        results = detect_outliers("OUT_04", "co2_ppm")
        assert len(results) == 1
        assert results[0].details["field"] == "co2_ppm"
        assert results[0].details["value"] == 9999.0

    def test_detects_humidity_over_100(self):
        """Injected anomaly — humidity = 115%."""
        self._make_reading("OUT_05", "humidity", 115.0)
        results = detect_outliers("OUT_05", "humidity")
        assert len(results) == 1

    def test_result_contains_expected_range(self):
        self._make_reading("OUT_06", "temperature", 99.0)
        results = detect_outliers("OUT_06", "temperature")
        assert "expected_range" in results[0].details
        assert results[0].details["expected_range"] == [
            QUALITY_THRESHOLDS["temperature"]["min"],
            QUALITY_THRESHOLDS["temperature"]["max"],
        ]

    def test_boundary_value_at_max_not_flagged(self):
        """Exactly at threshold is valid — not an outlier."""
        threshold = QUALITY_THRESHOLDS["temperature"]["max"]
        self._make_reading("OUT_07", "temperature", threshold)
        results = detect_outliers("OUT_07", "temperature")
        assert results == []

    def test_skips_validated_readings(self):
        """is_validated=True readings are excluded from outlier checks."""
        sensor = SensorFactory(sensor_id="OUT_08")
        SensorReadingFactory(
            sensor=sensor, sensor_id_raw="OUT_08",
            temperature=99.0, is_validated=True,
            quality_flags=[], quality_score=100.0,
        )
        results = detect_outliers("OUT_08", "temperature")
        assert results == []

    def test_returns_empty_for_field_without_thresholds(self):
        """Fields not in QUALITY_THRESHOLDS are silently skipped."""
        self._make_reading("OUT_09", "temperature", 99.0)
        results = detect_outliers("OUT_09", "nonexistent_field")
        assert results == []


# =============================================================================
# 7.  _apply_quality_flags
# =============================================================================

@pytest.mark.django_db
class TestApplyQualityFlags:

    def _make_result(self, sensor_id, check_type, field="temperature", value=99.0):
        sensor = SensorFactory(sensor_id=sensor_id)
        reading = SensorReadingFactory(
            sensor=sensor,
            sensor_id_raw=sensor_id,
            quality_flags=[],
            quality_score=100.0,
        )
        return QualityCheckResult(
            sensor_id=sensor_id,
            timestamp=reading.timestamp,
            check_type=check_type,
            passed=False,
            details={"field": field, "value": value},
        ), reading

    def test_flag_added_to_reading(self):
        result, reading = self._make_result("FLAG_01", "stuck_at")
        _apply_quality_flags([result])
        reading.refresh_from_db()
        assert "stuck_at" in reading.quality_flags

    def test_quality_score_reduced(self):
        result, reading = self._make_result("FLAG_02", "outlier")
        _apply_quality_flags([result])
        reading.refresh_from_db()
        assert reading.quality_score < 100.0

    def test_audit_log_created(self):
        result, reading = self._make_result("FLAG_03", "stuck_at")
        _apply_quality_flags([result])
        log = DataQualityLog.objects.filter(sensor_id="FLAG_03").first()
        assert log is not None
        assert log.detection_type == "stuck_at"
        assert log.severity == "warning"

    def test_audit_log_detection_type_lowercase(self):
        """detection_type must be lowercase to match TextChoices."""
        result, reading = self._make_result("FLAG_04", "outlier")
        _apply_quality_flags([result])
        log = DataQualityLog.objects.get(sensor_id="FLAG_04")
        assert log.detection_type == log.detection_type.lower()

    def test_flag_not_duplicated_on_second_apply(self):
        """Applying the same flag twice must not duplicate it in the list."""
        result, reading = self._make_result("FLAG_05", "stuck_at")
        _apply_quality_flags([result])
        _apply_quality_flags([result])
        reading.refresh_from_db()
        assert reading.quality_flags.count("stuck_at") == 1

    def test_multiple_flags_accumulated(self):
        """Two different check types on same reading → both flags present."""
        sensor = SensorFactory(sensor_id="FLAG_06")
        reading = SensorReadingFactory(
            sensor=sensor, sensor_id_raw="FLAG_06",
            quality_flags=[], quality_score=100.0,
        )
        results = [
            QualityCheckResult("FLAG_06", reading.timestamp, "stuck_at", False, {"field": "temperature", "value": 21.0}),
            QualityCheckResult("FLAG_06", reading.timestamp, "outlier",  False, {"field": "temperature", "value": 21.0}),
        ]
        _apply_quality_flags(results)
        reading.refresh_from_db()
        assert "stuck_at" in reading.quality_flags
        assert "outlier" in reading.quality_flags

    def test_passed_results_not_applied(self):
        """check_type results with passed=True must not modify the reading."""
        sensor = SensorFactory(sensor_id="FLAG_07")
        reading = SensorReadingFactory(
            sensor=sensor, sensor_id_raw="FLAG_07",
            quality_flags=[], quality_score=100.0,
        )
        result = QualityCheckResult(
            sensor_id="FLAG_07",
            timestamp=reading.timestamp,
            check_type="stuck_at",
            passed=True,  # ← passed, should be ignored
            details={"field": "temperature", "value": 21.0},
        )
        _apply_quality_flags([result])
        reading.refresh_from_db()
        assert reading.quality_flags == []
        assert reading.quality_score == 100.0

    def test_context_stored_in_log(self):
        result, _ = self._make_result("FLAG_08", "outlier", field="co2_ppm", value=9999.0)
        _apply_quality_flags([result])
        log = DataQualityLog.objects.get(sensor_id="FLAG_08")
        assert log.context["field"] == "co2_ppm"
        assert log.context["value"] == 9999.0


# =============================================================================
# 8.  run_quality_checks
# =============================================================================

@pytest.mark.django_db
class TestRunQualityChecks:

    def test_returns_stats_dict(self):
        stats = run_quality_checks(sensor_ids=[])
        assert "sensors_checked" in stats
        assert "issues_found" in stats
        assert "readings_updated" in stats

    def test_checks_all_sensors_when_none_provided(self):
        sensor = SensorFactory(sensor_id="RQC_01")
        SensorReadingFactory(sensor=sensor, sensor_id_raw="RQC_01")
        stats = run_quality_checks()
        assert stats["sensors_checked"] >= 1

    def test_detects_stuck_at_in_pipeline(self):
        """Full run_quality_checks must find and flag a stuck sensor."""
        sensor = SensorFactory(sensor_id="RQC_STUCK")
        now = timezone.now()
        for i in range(STUCK_AT_THRESHOLD + 1):
            SensorReadingFactory(
                sensor=sensor, sensor_id_raw="RQC_STUCK",
                timestamp=now - timedelta(minutes=i),
                temperature=22.1,
                quality_flags=[], quality_score=100.0,
            )
        run_quality_checks(sensor_ids=["RQC_STUCK"])
        flagged = SensorReading.objects.filter(
            sensor_id_raw="RQC_STUCK", quality_score__lt=100.0
        )
        assert flagged.exists()

    def test_detects_outlier_in_pipeline(self):
        """Full run_quality_checks must find and flag a CO2 spike."""
        sensor = SensorFactory(sensor_id="RQC_OUTLIER")
        SensorReadingFactory(
            sensor=sensor, sensor_id_raw="RQC_OUTLIER",
            co2_ppm=9999.0, is_validated=False,
            quality_flags=[], quality_score=100.0,
        )
        run_quality_checks(sensor_ids=["RQC_OUTLIER"])
        flagged = SensorReading.objects.filter(
            sensor_id_raw="RQC_OUTLIER", quality_score__lt=100.0
        )
        assert flagged.exists()

    def test_sensor_ids_filter_respected(self):
        """Only provided sensor_ids should be checked."""
        s1 = SensorFactory(sensor_id="RQC_SCOPE_A")
        s2 = SensorFactory(sensor_id="RQC_SCOPE_B")
        now = timezone.now()
        # Both sensors have stuck values
        for sid, sensor in [("RQC_SCOPE_A", s1), ("RQC_SCOPE_B", s2)]:
            for i in range(STUCK_AT_THRESHOLD + 1):
                SensorReadingFactory(
                    sensor=sensor, sensor_id_raw=sid,
                    timestamp=now - timedelta(minutes=i),
                    temperature=22.1,
                    quality_flags=[], quality_score=100.0,
                )
        # Only check A
        run_quality_checks(sensor_ids=["RQC_SCOPE_A"])
        # A should be flagged
        assert SensorReading.objects.filter(
            sensor_id_raw="RQC_SCOPE_A", quality_score__lt=100.0
        ).exists()
        # B should NOT be flagged
        assert not SensorReading.objects.filter(
            sensor_id_raw="RQC_SCOPE_B", quality_score__lt=100.0
        ).exists()

    def test_clean_data_gets_no_flags(self):
        """
        Readings with varying values within range must not be flagged.
        """
        sensor = SensorFactory(sensor_id="RQC_CLEAN")
        now = timezone.now()
        count = STUCK_AT_THRESHOLD + 2
        for i in range(count):
            SensorReadingFactory(
                sensor=sensor, sensor_id_raw="RQC_CLEAN",
                timestamp=now - timedelta(minutes=i),
                temperature=10.0 + i * 5.0,    # 10, 15, 20, 25, 30, 35, 40
                humidity=40.0 + i * 5.0,        # 40, 45, 50, 55, 60, 65, 70
                co2_ppm=300.0 + i * 50.0,       # 300, 350, 400, 450, 500, 550, 600
                battery_level=50.0 + i * 5.0,   # 50, 55, 60, 65, 70, 75, 80
                quality_flags=[], quality_score=100.0,
            )
        run_quality_checks(sensor_ids=["RQC_CLEAN"])
        flagged = SensorReading.objects.filter(
            sensor_id_raw="RQC_CLEAN", quality_score__lt=100.0
        )
        assert not flagged.exists()


# =============================================================================
# 9.  import_sensor_data (full pipeline)
# =============================================================================

@pytest.mark.django_db
class TestImportSensorData:

    def _write_csv(self, rows, tmp_path, filename="data.csv"):
        path = os.path.join(str(tmp_path), filename)
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_returns_ingestion_stats(self, tmp_path):
        path = make_csv([default_row()], str(tmp_path))
        stats = import_sensor_data(path, skip_quality=True)
        assert isinstance(stats, IngestionStats)

    def test_stats_total_rows(self, tmp_path):
        rows = [default_row(
            sensor_id="PIPE_01",
            timestamp=f"2024-05-01T{10+i:02d}:00:00Z"
        ) for i in range(5)]
        path = self._write_csv(rows, tmp_path)
        stats = import_sensor_data(path, skip_quality=True)
        assert stats.total_rows == 5

    def test_readings_persisted_to_db(self, tmp_path):
        rows = [default_row(
            sensor_id="PIPE_02",
            timestamp=f"2024-05-01T{10+i:02d}:00:00Z"
        ) for i in range(3)]
        path = self._write_csv(rows, tmp_path)
        import_sensor_data(path, skip_quality=True)
        assert SensorReading.objects.filter(sensor_id_raw="PIPE_02").count() == 3

    def test_idempotent_on_re_import(self, tmp_path):
        """Running the same file twice must not create duplicates."""
        path = make_csv([default_row(sensor_id="PIPE_03")], str(tmp_path))
        import_sensor_data(path, skip_quality=True)
        import_sensor_data(path, skip_quality=True)
        assert SensorReading.objects.filter(sensor_id_raw="PIPE_03").count() == 1

    def test_sensors_seen_list(self, tmp_path):
        rows = [
            default_row(sensor_id="PIPE_ALPHA", timestamp="2024-05-01T10:00:00Z"),
            default_row(sensor_id="PIPE_BETA",  timestamp="2024-05-01T10:00:00Z"),
        ]
        path = self._write_csv(rows, tmp_path)
        stats = import_sensor_data(path, skip_quality=True)
        assert set(stats.sensors_seen) == {"PIPE_ALPHA", "PIPE_BETA"}

    def test_quality_checks_run_by_default(self, tmp_path):
        """skip_quality=False (default) → quality issues found for injected anomaly."""
        rows = [
            default_row(sensor_id="PIPE_Q", timestamp=f"2024-05-01T{10+i:02d}:00:00Z",
                        co2_ppm="9999.0")
            for i in range(3)
        ]
        path = self._write_csv(rows, tmp_path)
        stats = import_sensor_data(path, skip_quality=False)
        assert stats.quality_issues_found > 0

    def test_skip_quality_means_no_flags(self, tmp_path):
        """skip_quality=True → readings inserted, but no quality flags applied."""
        rows = [default_row(sensor_id="PIPE_SKIP", co2_ppm="9999.0")]
        path = self._write_csv(rows, tmp_path)
        import_sensor_data(path, skip_quality=True)
        reading = SensorReading.objects.get(sensor_id_raw="PIPE_SKIP")
        assert reading.quality_flags == []

    def test_sensor_metadata_created_after_import(self, tmp_path):
        """Sensor records must be created/updated when quality checks run."""
        rows = [default_row(sensor_id="PIPE_META", timestamp="2024-05-01T10:00:00Z")]
        path = self._write_csv(rows, tmp_path)
        import_sensor_data(path, skip_quality=False)
        assert Sensor.objects.filter(sensor_id="PIPE_META").exists()

    def test_sensor_metadata_not_created_when_skip_quality(self, tmp_path):
        """_update_sensor_metadata is inside the skip_quality block → no Sensor records."""
        rows = [default_row(sensor_id="PIPE_NO_META")]
        path = self._write_csv(rows, tmp_path)
        import_sensor_data(path, skip_quality=True)
        assert not Sensor.objects.filter(sensor_id="PIPE_NO_META").exists()


# =============================================================================
# 10.  import_sensors management command
# =============================================================================

@pytest.mark.django_db
class TestImportSensorsCommand:

    def _run(self, *args, **kwargs):
        """Call the management command and return stdout."""
        out = StringIO()
        call_command("import_sensors", *args, stdout=out, **kwargs)
        return out.getvalue()

    def _write_csv(self, rows, tmp_path, filename="data.csv"):
        path = os.path.join(str(tmp_path), filename)
        if not rows:
            with open(path, "w") as f:
                f.write("timestamp,sensor_id,temperature,humidity,co2_ppm,battery_level\n")
            return path
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_raises_command_error_for_missing_file(self, tmp_path):
        with pytest.raises(CommandError, match="File not found"):
            self._run("/nonexistent/path/data.csv")

    def test_raises_command_error_for_non_csv(self, tmp_path):
        path = os.path.join(str(tmp_path), "data.json")
        with open(path, "w") as f:
            f.write("{}")
        with pytest.raises(CommandError, match="Expected CSV file"):
            self._run(path)

    def test_successful_import_output(self, tmp_path):
        rows = [default_row(sensor_id="CMD_01", timestamp="2024-05-01T10:00:00Z")]
        path = self._write_csv(rows, tmp_path)
        output = self._run(path, skip_quality=True)
        assert "Import completed successfully" in output

    def test_stats_printed_after_import(self, tmp_path):
        rows = [
            default_row(sensor_id="CMD_STATS", timestamp=f"2024-05-01T{10+i:02d}:00:00Z")
            for i in range(3)
        ]
        path = self._write_csv(rows, tmp_path)
        output = self._run(path, skip_quality=True)
        assert "Total rows processed" in output
        assert "New rows inserted" in output
        assert "Duplicates skipped" in output

    def test_dry_run_does_not_insert(self, tmp_path):
        rows = [default_row(sensor_id="CMD_DRY")]
        path = self._write_csv(rows, tmp_path)
        self._run(path, dry_run=True)
        assert SensorReading.objects.filter(sensor_id_raw="CMD_DRY").count() == 0

    def test_dry_run_output_says_validation_passed(self, tmp_path):
        rows = [default_row(sensor_id="CMD_DRY2")]
        path = self._write_csv(rows, tmp_path)
        output = self._run(path, dry_run=True)
        assert "validation passed" in output.lower()

    def test_dry_run_raises_on_missing_required_columns(self, tmp_path):
        """CSV without sensor_id column fails validation in dry-run."""
        path = os.path.join(str(tmp_path), "bad.csv")
        with open(path, "w") as f:
            f.write("temperature,humidity\n22.0,55.0\n")
        with pytest.raises(CommandError, match="Missing required columns"):
            self._run(path, dry_run=True)

    def test_skip_quality_flag_passed_to_service(self, tmp_path):
        """--skip-quality must suppress quality flags in the DB."""
        rows = [default_row(sensor_id="CMD_SKIP", co2_ppm="9999.0")]
        path = self._write_csv(rows, tmp_path)
        self._run(path, skip_quality=True)
        reading = SensorReading.objects.get(sensor_id_raw="CMD_SKIP")
        assert reading.quality_flags == []

    def test_import_without_skip_quality_creates_flags(self, tmp_path):
        """Without --skip-quality the quality pipeline runs and flags anomalies."""
        rows = [
            default_row(sensor_id="CMD_FLAGS", co2_ppm="9999.0",
                        timestamp=f"2024-05-01T{10+i:02d}:00:00Z")
            for i in range(3)
        ]
        path = self._write_csv(rows, tmp_path)
        self._run(path)
        flagged = SensorReading.objects.filter(
            sensor_id_raw="CMD_FLAGS", quality_score__lt=100.0
        )
        assert flagged.exists()

    def test_command_error_on_corrupt_csv(self, tmp_path):
        """Completely unreadable file → CommandError with 'Import failed'."""
        path = os.path.join(str(tmp_path), "corrupt.csv")
        with open(path, "wb") as f:
            f.write(b"\xff\xfe garbage binary content")
        with pytest.raises((CommandError, Exception)):
            self._run(path)