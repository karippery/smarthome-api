# tests/test_views.py
"""
API test suite — all 7 endpoints + their filters.

Fixes applied vs previous version:
  1. UNIQUE CONSTRAINT ERRORS (TestHourlyAggregationView)
     Tests that create multiple readings at the same timestamp now use
     explicit unique timestamps (now, now-1min, now-2min etc.).

  2. DATA LEAKAGE (TestQualitySummary, TestSensorHealth)
     Tests that assert exact counts/states now create their own isolated
     sensors with unique IDs instead of relying on shared fixtures.
     The 'multi_sensor_readings' fixture uses MULTI_* prefix IDs so it
     never collides with the 'sensor' fixture's KITCHEN_01.

  3. DATE RANGE FILTER (TestQualityLogListView::test_filter_by_date_range)
     DataQualityLogFactory.detected_at was auto_now_add in the model but
     the factory was passing it as a kwarg — ignored silently. Fixed by
     using update() after creation to set detected_at explicitly.

  4. HEALTH STATUS THRESHOLDS (TestSensorHealthView)
     Tests now create exact reading counts to hit the score thresholds:
       degraded: need avg 70–89  → 7×100 + 3×55 = avg 83.5 ✓
       critical: need avg < 70   → all readings at 50.0 ✓
     Readings use timestamps within the default 24h window.

  5. GAP DETECTION (TestSensorHealthView::test_gap_detected_for_silent_sensor)
     The sensor health view filters by timestamp__gte=since (last 24h).
     A reading 3h ago IS within 24h — it appears in the queryset but
     latest_ts is 3h ago → gap_detected=True. The test was correct;
     the issue was data leakage from other fixtures pushing latest_ts
     to now. Fixed by using an isolated sensor ID.

  6. RECENT FLAGS (TestSensorHealthView::test_recent_flags_populated)
     Same leakage issue — fixed with isolated sensor ID.
"""

import pytest
from django.utils import timezone
from datetime import timedelta
from rest_framework.test import APIClient

from apps.sensor.tests.factories import SensorFactory, SensorReadingFactory, DataQualityLogFactory
from apps.sensor.models import DataQualityLog


# =============================================================================
# Helpers
# =============================================================================

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_readings(sensor_id, count, quality_score=100.0, quality_flags=None, offset_minutes=0):
    """
    Create `count` readings for sensor_id with unique timestamps.
    offset_minutes: shifts all timestamps further into the past.
    """
    if quality_flags is None:
        quality_flags = []
    sensor = SensorFactory(sensor_id=sensor_id)
    now = timezone.now()
    return [
        SensorReadingFactory(
            sensor=sensor,
            sensor_id_raw=sensor_id,
            timestamp=now - timedelta(minutes=offset_minutes + i),
            quality_score=quality_score,
            quality_flags=quality_flags,
        )
        for i in range(count)
    ]


# =============================================================================
# 1.  GET /api/readings/v1/raw/
# =============================================================================

@pytest.mark.django_db
class TestRawReadingListView:
    url = "/api/readings/v1/raw/"

    def test_returns_200(self, api_client, clean_readings):
        assert api_client.get(self.url).status_code == 200

    def test_returns_all_readings(self, api_client, clean_readings):
        assert api_client.get(self.url).data["count"] == 5

    def test_response_contains_raw_fields(self, api_client, clean_readings):
        result = api_client.get(self.url).data["results"][0]
        assert "received_at" in result
        assert "is_raw" in result
        assert "processing_notes" in result

    def test_response_contains_quality_fields(self, api_client, clean_readings):
        result = api_client.get(self.url).data["results"][0]
        assert "quality_score" in result
        assert "quality_flags" in result
        assert "is_validated" in result

    # ── Filters ──────────────────────────────────────────────────────────────

    def test_filter_by_sensor_id(self, api_client, clean_readings):
        SensorReadingFactory(sensor_id_raw="OTHER_SENSOR")
        response = api_client.get(self.url, {"sensor_id": "KITCHEN_01"})
        assert response.data["count"] == 5
        assert all(r["sensor_id_raw"] == "KITCHEN_01" for r in response.data["results"])

    def test_filter_sensor_id_case_insensitive(self, api_client, clean_readings):
        assert api_client.get(self.url, {"sensor_id": "kitchen_01"}).data["count"] == 5

    def test_filter_by_start(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=2))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=30))
        response = api_client.get(self.url, {"start": iso(now - timedelta(hours=1))})
        assert response.data["count"] == 1

    def test_filter_by_end(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=2))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=30))
        response = api_client.get(self.url, {"end": iso(now - timedelta(hours=1))})
        assert response.data["count"] == 1

    def test_filter_by_start_and_end(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=3))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=2))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=1))
        response = api_client.get(self.url, {
            "start": iso(now - timedelta(hours=2, minutes=30)),
            "end":   iso(now - timedelta(minutes=30)),
        })
        assert response.data["count"] == 2

    def test_filter_has_flags_true(self, api_client, clean_readings, flagged_readings):
        response = api_client.get(self.url, {"has_flags": "true"})
        assert response.data["count"] == 3
        assert all(r["quality_flags"] != [] for r in response.data["results"])

    def test_filter_has_flags_false(self, api_client, clean_readings, flagged_readings):
        response = api_client.get(self.url, {"has_flags": "false"})
        assert response.data["count"] == 5
        assert all(r["quality_flags"] == [] for r in response.data["results"])

    def test_filter_min_quality(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=1), quality_score=100.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=2), quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=3), quality_score=50.0)
        response = api_client.get(self.url, {"min_quality": 76, "sensor_id": "KITCHEN_01"})
        assert response.data["count"] == 1
        assert response.data["results"][0]["quality_score"] == 100.0

    def test_filter_max_quality(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=1), quality_score=100.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=2), quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=3), quality_score=50.0)
        response = api_client.get(self.url, {"max_quality": 74, "sensor_id": "KITCHEN_01"})
        assert response.data["count"] == 1
        assert response.data["results"][0]["quality_score"] == 50.0

    def test_filter_is_validated(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=1), is_validated=False)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(minutes=2), is_validated=True)
        assert api_client.get(self.url, {"is_validated": "false", "sensor_id": "KITCHEN_01"}).data["count"] == 1

    # ── Ordering ─────────────────────────────────────────────────────────────

    def test_default_ordering_newest_first(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=2))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01", timestamp=now - timedelta(hours=1))
        response = api_client.get(self.url, {"sensor_id": "KITCHEN_01"})
        timestamps = [r["timestamp"] for r in response.data["results"]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_ordering_quality_score_asc(self, api_client, sensor):
        now = timezone.now()
        for i, score in enumerate([50.0, 100.0, 75.0]):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                                 timestamp=now - timedelta(minutes=i+1), quality_score=score)
        response = api_client.get(self.url, {"ordering": "quality_score", "sensor_id": "KITCHEN_01"})
        scores = [r["quality_score"] for r in response.data["results"]]
        assert scores == sorted(scores)

    def test_ordering_quality_score_desc(self, api_client, sensor):
        now = timezone.now()
        for i, score in enumerate([50.0, 100.0]):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                                 timestamp=now - timedelta(minutes=i+1), quality_score=score)
        response = api_client.get(self.url, {"ordering": "-quality_score", "sensor_id": "KITCHEN_01"})
        scores = [r["quality_score"] for r in response.data["results"]]
        assert scores == sorted(scores, reverse=True)

    def test_empty_result_returns_200(self, api_client):
        response = api_client.get(self.url, {"sensor_id": "NONEXISTENT"})
        assert response.status_code == 200
        assert response.data["count"] == 0


# =============================================================================
# 2.  GET /api/readings/v1/processed/
# =============================================================================

@pytest.mark.django_db
class TestProcessedReadingListView:
    url = "/api/readings/v1/processed/"

    def test_returns_200(self, api_client, clean_readings):
        assert api_client.get(self.url).status_code == 200

    def test_contains_derived_fields(self, api_client, clean_readings):
        result = api_client.get(self.url).data["results"][0]
        assert "has_issues" in result
        assert "issue_count" in result

    def test_does_not_contain_raw_fields(self, api_client, clean_readings):
        result = api_client.get(self.url).data["results"][0]
        assert "received_at" not in result
        assert "is_raw" not in result
        assert "processing_notes" not in result

    def test_has_issues_false_for_clean(self, api_client, clean_readings):
        response = api_client.get(self.url, {"sensor_id": "KITCHEN_01"})
        assert all(r["has_issues"] is False for r in response.data["results"])
        assert all(r["issue_count"] == 0 for r in response.data["results"])

    def test_has_issues_true_for_flagged(self, api_client, flagged_readings):
        response = api_client.get(self.url, {"sensor_id": "KITCHEN_01"})
        assert all(r["has_issues"] is True for r in response.data["results"])
        assert all(r["issue_count"] >= 1 for r in response.data["results"])

    def test_issue_count_matches_flag_length(self, api_client, sensor):
        SensorReadingFactory(
            sensor=sensor, sensor_id_raw="KITCHEN_01",
            quality_flags=["stuck_at", "outlier"], quality_score=55.0,
        )
        result = api_client.get(self.url, {"sensor_id": "KITCHEN_01"}).data["results"][0]
        assert result["issue_count"] == 2

    def test_filter_by_sensor_id(self, api_client, clean_readings):
        SensorReadingFactory(sensor_id_raw="OTHER_01")
        response = api_client.get(self.url, {"sensor_id": "KITCHEN_01"})
        assert all(r["sensor_id_raw"] == "KITCHEN_01" for r in response.data["results"])

    def test_filter_has_flags_true(self, api_client, clean_readings, flagged_readings):
        assert api_client.get(self.url, {"has_flags": "true"}).data["count"] == 3

    def test_filter_has_flags_false(self, api_client, clean_readings, flagged_readings):
        assert api_client.get(self.url, {"has_flags": "false"}).data["count"] == 5


# =============================================================================
# 3.  GET /api/readings/v1/processed/with-issues/
# =============================================================================

@pytest.mark.django_db
class TestProcessedWithIssuesView:
    url = "/api/readings/v1/processed/with-issues/"

    def test_returns_200(self, api_client, flagged_readings):
        assert api_client.get(self.url).status_code == 200

    def test_only_returns_flagged(self, api_client, clean_readings, flagged_readings):
        response = api_client.get(self.url)
        assert response.data["count"] == 3
        assert all(r["has_issues"] is True for r in response.data["results"])

    def test_clean_readings_excluded(self, api_client, clean_readings):
        assert api_client.get(self.url).data["count"] == 0

    def test_filter_by_sensor_id(self, api_client):
        s1 = SensorFactory(sensor_id="WI_SENSOR_A")
        s2 = SensorFactory(sensor_id="WI_SENSOR_B")
        now = timezone.now()
        for i in range(2):
            SensorReadingFactory(sensor=s1, sensor_id_raw="WI_SENSOR_A",
                                 timestamp=now - timedelta(minutes=i), quality_flags=["stuck_at"], quality_score=75.0)
        for i in range(3):
            SensorReadingFactory(sensor=s2, sensor_id_raw="WI_SENSOR_B",
                                 timestamp=now - timedelta(minutes=i), quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url, {"sensor_id": "WI_SENSOR_A"})
        assert response.data["count"] == 2

    def test_filter_by_date_range(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                             timestamp=now - timedelta(hours=3),
                             quality_flags=["stuck_at"], quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                             timestamp=now - timedelta(minutes=30),
                             quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url, {
            "start": iso(now - timedelta(hours=2)),
            "end":   iso(now),
        })
        assert response.data["count"] == 1

    def test_ordering_by_quality_score(self, api_client, sensor):
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                             timestamp=now - timedelta(minutes=1), quality_flags=["stuck_at"], quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                             timestamp=now - timedelta(minutes=2), quality_flags=["outlier"], quality_score=80.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="KITCHEN_01",
                             timestamp=now - timedelta(minutes=3), quality_flags=["stuck_at", "outlier"], quality_score=55.0)
        response = api_client.get(self.url, {"ordering": "quality_score"})
        scores = [r["quality_score"] for r in response.data["results"]]
        assert scores == sorted(scores)


# =============================================================================
# 4.  GET /api/quality-logs/
# =============================================================================

@pytest.mark.django_db
class TestQualityLogListView:
    url = "/api/quality-logs/"

    def test_returns_200(self, api_client):
        assert api_client.get(self.url).status_code == 200

    def test_returns_all_logs(self, api_client):
        DataQualityLogFactory.create_batch(4)
        assert api_client.get(self.url).data["count"] == 4

    def test_response_fields(self, api_client):
        DataQualityLogFactory()
        result = api_client.get(self.url).data["results"][0]
        for field in ["sensor_id", "detection_type", "severity", "description", "context", "detected_at"]:
            assert field in result

    def test_filter_by_sensor_id(self, api_client):
        r1 = SensorReadingFactory(sensor_id_raw="QL_KITCHEN_01")
        r2 = SensorReadingFactory(sensor_id_raw="QL_BEDROOM_01")
        DataQualityLogFactory(reading=r1, sensor_id="QL_KITCHEN_01")
        DataQualityLogFactory(reading=r2, sensor_id="QL_BEDROOM_01")
        response = api_client.get(self.url, {"sensor_id": "QL_KITCHEN_01"})
        assert response.data["count"] == 1
        assert response.data["results"][0]["sensor_id"] == "QL_KITCHEN_01"

    def test_filter_by_detection_type(self, api_client):
        reading = SensorReadingFactory()
        DataQualityLogFactory(reading=reading, detection_type="stuck_at")
        DataQualityLogFactory(reading=reading, detection_type="outlier")
        response = api_client.get(self.url, {"detection_type": "stuck_at"})
        assert response.data["count"] == 1
        assert response.data["results"][0]["detection_type"] == "stuck_at"

    def test_filter_by_severity(self, api_client):
        reading = SensorReadingFactory()
        DataQualityLogFactory(reading=reading, severity="warning")
        DataQualityLogFactory(reading=reading, severity="critical")
        DataQualityLogFactory(reading=reading, severity="critical")
        response = api_client.get(self.url, {"severity": "critical"})
        assert response.data["count"] == 2

    def test_filter_by_date_range(self, api_client):
        now = timezone.now()
        reading = SensorReadingFactory()

        log_old = DataQualityLogFactory(reading=reading)
        DataQualityLog.objects.filter(pk=log_old.pk).update(
            detected_at=now - timedelta(hours=3)
        )

        log_recent = DataQualityLogFactory(reading=reading)
        DataQualityLog.objects.filter(pk=log_recent.pk).update(
            detected_at=now - timedelta(minutes=30)
        )

        response = api_client.get(self.url, {
            "start": iso(now - timedelta(hours=2)),
            "end":   iso(now),
        })
        assert response.data["count"] == 1

    def test_filter_sensor_id_case_insensitive(self, api_client):
        reading = SensorReadingFactory(sensor_id_raw="QL_CASE_01")
        DataQualityLogFactory(reading=reading, sensor_id="QL_CASE_01")
        response = api_client.get(self.url, {"sensor_id": "ql_case_01"})
        assert response.data["count"] == 1

    def test_default_ordering_newest_first(self, api_client):
        """Uses update() to set detected_at since it is auto_now_add."""
        now = timezone.now()
        reading = SensorReadingFactory()
        log1 = DataQualityLogFactory(reading=reading)
        log2 = DataQualityLogFactory(reading=reading)
        DataQualityLog.objects.filter(pk=log1.pk).update(detected_at=now - timedelta(hours=2))
        DataQualityLog.objects.filter(pk=log2.pk).update(detected_at=now - timedelta(hours=1))
        response = api_client.get(self.url)
        timestamps = [r["detected_at"] for r in response.data["results"]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_ordering_by_severity(self, api_client):
        reading = SensorReadingFactory()
        DataQualityLogFactory(reading=reading, severity="warning")
        DataQualityLogFactory(reading=reading, severity="critical")
        DataQualityLogFactory(reading=reading, severity="info")
        response = api_client.get(self.url, {"ordering": "severity"})
        severities = [r["severity"] for r in response.data["results"]]
        assert severities == sorted(severities)


# =============================================================================
# 5.  GET /api/aggregations/v1/hourly/<sensor_id>/
# =============================================================================

@pytest.mark.django_db
class TestHourlyAggregationView:

    def url(self, sensor_id):
        return f"/api/aggregations/v1/hourly/{sensor_id}/"

    def test_returns_200(self, api_client):
        make_readings("HOURLY_K01", 3)
        assert api_client.get(self.url("HOURLY_K01")).status_code == 200

    def test_returns_empty_list_for_unknown_sensor(self, api_client):
        # View intentionally returns 200+[] instead of 404.
        # Comment in views.py: "404 implies the resource doesn't exist,
        # not just lack of data" — consistent with other aggregation endpoints.
        response = api_client.get(self.url("UNKNOWN_99"))
        assert response.status_code == 200
        assert response.data == []

    def test_response_fields(self, api_client):
        make_readings("HOURLY_K01", 2)
        bucket = api_client.get(self.url("HOURLY_K01")).data[0]
        for field in ["sensor_id", "hour", "reading_count", "flagged_count",
                      "avg_temperature", "avg_humidity", "avg_co2_ppm",
                      "avg_battery", "avg_quality_score", "min_quality_score"]:
            assert field in bucket

    def test_sensor_id_uppercased_in_response(self, api_client):
        make_readings("HOURLY_K01", 1)
        assert api_client.get(self.url("hourly_k01")).data[0]["sensor_id"] == "HOURLY_K01"

    def test_buckets_ordered_oldest_first(self, api_client):
        sensor = SensorFactory(sensor_id="HOURLY_ORD_01")
        now = timezone.now().replace(minute=0, second=0, microsecond=0)
        for i in range(3):
            SensorReadingFactory(
                sensor=sensor, sensor_id_raw="HOURLY_ORD_01",
                timestamp=now - timedelta(hours=i),
            )
        response = api_client.get(self.url("HOURLY_ORD_01"))
        hours = [b["hour"] for b in response.data]
        assert hours == sorted(hours)

    def test_flagged_count_correct(self, api_client):

        sensor = SensorFactory(sensor_id="HOURLY_FLAG_01")
        now = timezone.now().replace(minute=0, second=0, microsecond=0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_FLAG_01",
                             timestamp=now,                          quality_flags=[])
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_FLAG_01",
                             timestamp=now + timedelta(minutes=1),   quality_flags=[])
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_FLAG_01",
                             timestamp=now + timedelta(minutes=2),
                             quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url("HOURLY_FLAG_01"))
        assert response.data[0]["reading_count"] == 3
        assert response.data[0]["flagged_count"] == 1

    def test_filter_by_start(self, api_client):
        sensor = SensorFactory(sensor_id="HOURLY_ST_01")
        now = timezone.now().replace(minute=0, second=0, microsecond=0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_ST_01", timestamp=now - timedelta(hours=5))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_ST_01", timestamp=now - timedelta(hours=1))
        response = api_client.get(self.url("HOURLY_ST_01"), {"start": iso(now - timedelta(hours=2))})
        assert len(response.data) == 1

    def test_filter_by_end(self, api_client):
        sensor = SensorFactory(sensor_id="HOURLY_EN_01")
        now = timezone.now().replace(minute=0, second=0, microsecond=0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_EN_01", timestamp=now - timedelta(hours=5))
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_EN_01", timestamp=now - timedelta(hours=1))
        response = api_client.get(self.url("HOURLY_EN_01"), {"end": iso(now - timedelta(hours=3))})
        assert len(response.data) == 1

    def test_avg_temperature_calculated(self, api_client):

        sensor = SensorFactory(sensor_id="HOURLY_AVG_01")
        now = timezone.now().replace(minute=0, second=0, microsecond=0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_AVG_01",
                             timestamp=now,                        temperature=20.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="HOURLY_AVG_01",
                             timestamp=now + timedelta(minutes=1), temperature=22.0)
        response = api_client.get(self.url("HOURLY_AVG_01"))
        assert response.data[0]["avg_temperature"] == 21.0


# =============================================================================
# 6.  GET /api/aggregations/v1/quality-summary/
# =============================================================================

@pytest.mark.django_db
class TestQualitySummaryView:
    url = "/api/aggregations/v1/quality-summary/"

    def test_returns_200(self, api_client):
        make_readings("QS_TEST_01", 2)
        assert api_client.get(self.url).status_code == 200

    def test_returns_empty_list_no_data(self, api_client):
        # Filter by a sensor that definitely has no readings.
        # Asserting global response == [] is fragile — other tests in the
        # same session may have committed readings that bleed through.
        response = api_client.get(self.url, {"sensor_id": "QS_NONEXISTENT_99"})
        assert response.status_code == 200
        assert response.data == []

    def test_one_row_per_sensor(self, api_client, multi_sensor_readings):
        # Filter to only the MULTI_* sensors created by this fixture.
        # Asserting len(all results) == 3 is fragile if other tests left
        # data in the DB. The MULTI_ prefix is unique to this fixture.
        response = api_client.get(self.url)
        multi_rows = [r for r in response.data if r["sensor_id"].startswith("MULTI_")]
        sensor_ids = [r["sensor_id"] for r in multi_rows]
        assert len(sensor_ids) == 3
        assert len(set(sensor_ids)) == 3  # no duplicates

    def test_total_readings_count(self, api_client):
        sensor = SensorFactory(sensor_id="QS_COUNT_01")
        now = timezone.now()
        for i in range(5):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_COUNT_01",
                                 timestamp=now - timedelta(minutes=i), quality_flags=[])
        for i in range(3):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_COUNT_01",
                                 timestamp=now - timedelta(minutes=10+i),
                                 quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url, {"sensor_id": "QS_COUNT_01"})
        assert response.data[0]["total_readings"] == 8

    def test_flagged_and_clean_counts(self, api_client):
        sensor = SensorFactory(sensor_id="QS_SPLIT_01")
        now = timezone.now()
        for i in range(5):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_SPLIT_01",
                                 timestamp=now - timedelta(minutes=i), quality_flags=[])
        for i in range(3):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_SPLIT_01",
                                 timestamp=now - timedelta(minutes=10+i),
                                 quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url, {"sensor_id": "QS_SPLIT_01"})
        row = response.data[0]
        assert row["flagged_readings"] == 3
        assert row["clean_readings"] == 5
        assert row["flagged_readings"] + row["clean_readings"] == row["total_readings"]

    def test_flag_breakdown(self, api_client):

        sensor = SensorFactory(sensor_id="QS_BREAK_01")
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_BREAK_01",
                             timestamp=now - timedelta(minutes=1),
                             quality_flags=["stuck_at"], quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_BREAK_01",
                             timestamp=now - timedelta(minutes=2),
                             quality_flags=["stuck_at"], quality_score=75.0)
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_BREAK_01",
                             timestamp=now - timedelta(minutes=3),
                             quality_flags=["outlier"], quality_score=80.0)
        response = api_client.get(self.url, {"sensor_id": "QS_BREAK_01"})
        breakdown = response.data[0]["flag_breakdown"]
        assert breakdown["stuck_at"] == 2
        assert breakdown["outlier"] == 1

    def test_issues_list_sorted(self, api_client):
        sensor = SensorFactory(sensor_id="QS_SORT_01")
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_SORT_01",
                             quality_flags=["stuck_at", "outlier"], quality_score=55.0)
        response = api_client.get(self.url, {"sensor_id": "QS_SORT_01"})
        issues = response.data[0]["issues"]
        assert issues == sorted(issues)

    def test_filter_by_sensor_id(self, api_client, multi_sensor_readings):
        response = api_client.get(self.url, {"sensor_id": "MULTI_BEDROOM_01"})
        assert len(response.data) == 1
        assert response.data[0]["sensor_id"] == "MULTI_BEDROOM_01"

    def test_avg_quality_score_calculated(self, api_client):
        sensor = SensorFactory(sensor_id="QS_AVG_01")
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_AVG_01",
                             timestamp=now - timedelta(minutes=1),
                             quality_score=100.0, quality_flags=[])
        SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_AVG_01",
                             timestamp=now - timedelta(minutes=2),
                             quality_score=80.0, quality_flags=["outlier"])
        response = api_client.get(self.url, {"sensor_id": "QS_AVG_01"})
        assert response.data[0]["avg_quality_score"] == 90.0

    def test_clean_sensor_has_empty_flag_breakdown(self, api_client):

        sensor = SensorFactory(sensor_id="QS_CLEAN_01")
        now = timezone.now()
        for i in range(3):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="QS_CLEAN_01",
                                 timestamp=now - timedelta(minutes=i),
                                 quality_flags=[], quality_score=100.0)
        response = api_client.get(self.url, {"sensor_id": "QS_CLEAN_01"})
        assert response.data[0]["flag_breakdown"] == {}
        assert response.data[0]["issues"] == []


# =============================================================================
# 7.  GET /api/aggregations/v1/sensor-health/
# =============================================================================

@pytest.mark.django_db
class TestSensorHealthView:
    url = "/api/aggregations/v1/sensor-health/"

    def test_returns_200(self, api_client):
        make_readings("SH_TEST_01", 2)
        assert api_client.get(self.url).status_code == 200

    def test_returns_empty_list_no_recent_data(self, api_client):

        sensor = SensorFactory(sensor_id="SH_OLD_01")
        SensorReadingFactory(
            sensor=sensor, sensor_id_raw="SH_OLD_01",
            timestamp=timezone.now() - timedelta(hours=48),
        )
        response = api_client.get(self.url, {"sensor_id": "SH_OLD_01"})
        assert response.data == []

    def test_response_fields(self, api_client):
        make_readings("SH_FIELDS_01", 1)
        result = api_client.get(self.url, {"sensor_id": "SH_FIELDS_01"}).data[0]
        for field in ["sensor_id", "health_status", "health_reason", "avg_temperature",
                      "avg_humidity", "avg_co2_ppm", "latest_battery",
                      "recent_flags", "reading_count", "gap_detected"]:
            assert field in result

    def test_healthy_status_for_perfect_readings(self, api_client):
        make_readings("SH_HEALTHY_01", 5, quality_score=100.0, quality_flags=[])
        response = api_client.get(self.url, {"sensor_id": "SH_HEALTHY_01"})
        assert response.data[0]["health_status"] == "healthy"
        assert response.data[0]["gap_detected"] is False

    def test_degraded_status(self, api_client):

        sensor = SensorFactory(sensor_id="SH_DEG_01")
        now = timezone.now()
        for i in range(7):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_DEG_01",
                                 timestamp=now - timedelta(minutes=i),
                                 quality_score=100.0, quality_flags=[])
        for i in range(3):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_DEG_01",
                                 timestamp=now - timedelta(minutes=10+i),
                                 quality_score=55.0, quality_flags=["stuck_at", "outlier"])
        response = api_client.get(self.url, {"sensor_id": "SH_DEG_01"})
        assert response.data[0]["health_status"] == "degraded"

    def test_critical_status_for_low_score(self, api_client):

        sensor = SensorFactory(sensor_id="SH_CRIT_01")
        now = timezone.now()
        for i in range(5):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_CRIT_01",
                                 timestamp=now - timedelta(minutes=i),
                                 quality_score=50.0, quality_flags=["stuck_at", "outlier"])
        response = api_client.get(self.url, {"sensor_id": "SH_CRIT_01"})
        assert response.data[0]["health_status"] == "critical"

    def test_gap_detected_for_silent_sensor(self, api_client):

        sensor = SensorFactory(sensor_id="SH_GAP_01")
        SensorReadingFactory(
            sensor=sensor, sensor_id_raw="SH_GAP_01",
            timestamp=timezone.now() - timedelta(hours=3),
            quality_score=100.0, quality_flags=[],
        )
        response = api_client.get(self.url, {"sensor_id": "SH_GAP_01"})
        assert response.data[0]["gap_detected"] is True
        assert response.data[0]["health_status"] == "critical"

    def test_sorted_critical_first(self, api_client):
        now = timezone.now()
        s1 = SensorFactory(sensor_id="SH_SORT_GOOD")
        s2 = SensorFactory(sensor_id="SH_SORT_BAD")
        SensorReadingFactory(sensor=s1, sensor_id_raw="SH_SORT_GOOD",
                             timestamp=now - timedelta(minutes=1),
                             quality_score=100.0, quality_flags=[])
        for i in range(5):
            SensorReadingFactory(sensor=s2, sensor_id_raw="SH_SORT_BAD",
                                 timestamp=now - timedelta(minutes=i+1),
                                 quality_score=50.0, quality_flags=["stuck_at", "outlier"])
        response = api_client.get(self.url)
        statuses = [r["health_status"] for r in response.data]
        priority = {"critical": 0, "degraded": 1, "healthy": 2}
        assert statuses == sorted(statuses, key=lambda s: priority[s])

    def test_filter_by_sensor_id(self, api_client, multi_sensor_readings):
        response = api_client.get(self.url, {"sensor_id": "MULTI_LIVING_01"})
        assert len(response.data) == 1
        assert response.data[0]["sensor_id"] == "MULTI_LIVING_01"

    def test_hours_filter_respects_window(self, api_client):
        sensor = SensorFactory(sensor_id="SH_HOURS_01")
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_HOURS_01",
                             timestamp=now - timedelta(hours=1),
                             quality_score=100.0, quality_flags=[])
        SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_HOURS_01",
                             timestamp=now - timedelta(hours=10),
                             quality_score=50.0, quality_flags=["stuck_at"])
        response = api_client.get(self.url, {"sensor_id": "SH_HOURS_01", "hours": 5})
        assert response.data[0]["reading_count"] == 1
        assert response.data[0]["health_status"] == "healthy"

    def test_latest_battery_from_outside_window(self, api_client):
        """
        Battery reading outside the 1h window must still appear in latest_battery
        because battery_map uses an unrestricted queryset.
        """
        sensor = SensorFactory(sensor_id="SH_BAT_01")
        now = timezone.now()
        SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_BAT_01",
                             timestamp=now - timedelta(hours=5),
                             battery_level=42.0, quality_score=100.0, quality_flags=[])
        SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_BAT_01",
                             timestamp=now - timedelta(minutes=10),
                             battery_level=None, quality_score=100.0, quality_flags=[])
        response = api_client.get(self.url, {"sensor_id": "SH_BAT_01", "hours": 1})
        assert response.data[0]["latest_battery"] == 42.0

    def test_recent_flags_populated(self, api_client):

        sensor = SensorFactory(sensor_id="SH_FLAGS_01")
        now = timezone.now()
        for i in range(3):
            SensorReadingFactory(sensor=sensor, sensor_id_raw="SH_FLAGS_01",
                                 timestamp=now - timedelta(minutes=i),
                                 quality_flags=["stuck_at"], quality_score=75.0)
        response = api_client.get(self.url, {"sensor_id": "SH_FLAGS_01"})
        assert "stuck_at" in response.data[0]["recent_flags"]