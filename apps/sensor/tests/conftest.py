# tests/conftest.py
"""
Shared fixtures for sensor API tests.

Key design decisions:
  - All fixtures use unique sensor_ids to prevent cross-test data leakage.
  - multi_sensor_readings uses completely isolated sensors so QualitySummary
    tests that call without ?sensor_id= get exactly the expected row count.
  - @pytest.fixture(scope="function") is default — each test gets a clean DB.
"""

import pytest
from django.utils import timezone
from datetime import timedelta
from rest_framework.test import APIClient

from apps.sensor.tests.factories import SensorFactory, SensorReadingFactory, DataQualityLogFactory


@pytest.fixture(autouse=True)
def disable_throttling(settings):
    """
    Disable all DRF throttling for every test.
    Without this, tests hit 429 after the first few requests because
    the throttle cache persists across tests in the same session.
    """
    settings.REST_FRAMEWORK = {
        **settings.REST_FRAMEWORK,
        "DEFAULT_THROTTLE_CLASSES": [],
        "DEFAULT_THROTTLE_RATES": {},
    }


@pytest.fixture(autouse=True)
def clear_cache():
    """
    Clear Django cache before and after every test.

    Why both before AND after:
      - Before: a previous test may have cached a response (e.g. empty
        quality-summary) under the same key this test will use. Without
        clearing, the view returns stale cached data instead of querying
        the freshly created test fixtures.
      - After: prevents this test's cached data from leaking into the next.

    This affects QualitySummaryView, SensorHealthView, HourlyAggregationView
    which all use CacheMixin — they cache the serialized response and return
    it on subsequent requests with the same cache key.
    """
    from django.core.cache import cache
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def sensor():
    """Single KITCHEN_01 sensor. Used by tests that focus on one sensor."""
    return SensorFactory(sensor_id="KITCHEN_01", location="Kitchen")


@pytest.fixture
def clean_readings(sensor):
    """5 clean readings for KITCHEN_01, each 1 minute apart."""
    now = timezone.now()
    return [
        SensorReadingFactory(
            sensor=sensor,
            sensor_id_raw="KITCHEN_01",
            timestamp=now - timedelta(minutes=i),
            quality_score=100.0,
            quality_flags=[],
        )
        for i in range(5)
    ]


@pytest.fixture
def flagged_readings(sensor):
    """3 flagged readings for KITCHEN_01, each 1 minute apart after clean_readings."""
    now = timezone.now()
    return [
        SensorReadingFactory(
            sensor=sensor,
            sensor_id_raw="KITCHEN_01",
            timestamp=now - timedelta(minutes=10 + i),
            quality_flags=["stuck_at"],
            quality_score=75.0,
        )
        for i in range(3)
    ]


@pytest.fixture
def multi_sensor_readings():
    """
    Readings for 3 completely isolated sensors.
    Uses unique sensor_ids that won't collide with the 'sensor' fixture.
    """
    s1 = SensorFactory(sensor_id="MULTI_LIVING_01")
    s2 = SensorFactory(sensor_id="MULTI_BEDROOM_01")
    s3 = SensorFactory(sensor_id="MULTI_OFFICE_01")

    now = timezone.now()

    r1 = [
        SensorReadingFactory(
            sensor=s1, sensor_id_raw="MULTI_LIVING_01",
            timestamp=now - timedelta(minutes=i),
            quality_flags=[],
        )
        for i in range(4)
    ]
    r2 = [
        SensorReadingFactory(
            sensor=s2, sensor_id_raw="MULTI_BEDROOM_01",
            timestamp=now - timedelta(minutes=i),
            quality_flags=["stuck_at"], quality_score=75.0,
        )
        for i in range(3)
    ]
    r3 = [
        SensorReadingFactory(
            sensor=s3, sensor_id_raw="MULTI_OFFICE_01",
            timestamp=now - timedelta(minutes=i),
            quality_flags=["outlier"], quality_score=80.0,
            temperature=85.0,
        )
        for i in range(2)
    ]

    return {
        "MULTI_LIVING_01":  r1,
        "MULTI_BEDROOM_01": r2,
        "MULTI_OFFICE_01":  r3,
    }