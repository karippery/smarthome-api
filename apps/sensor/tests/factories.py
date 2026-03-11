# tests/factories.py
import factory
from factory.django import DjangoModelFactory
from django.utils import timezone
from datetime import timedelta

from apps.sensor.models import Sensor, SensorReading, DataQualityLog


class SensorFactory(DjangoModelFactory):
    class Meta:
        model = Sensor
        # Prevents duplicate sensor_id IntegrityError when same sensor_id
        # is created across fixtures in the same test
        django_get_or_create = ("sensor_id",)

    sensor_id    = factory.Sequence(lambda n: f"SENSOR_{n:02d}")
    name         = factory.LazyAttribute(lambda o: f"Sensor {o.sensor_id}")
    location     = factory.Iterator(["Living Room", "Kitchen", "Bedroom", "Office", "Bathroom"])
    sensor_type  = "environmental"
    status       = Sensor.Status.ACTIVE
    last_seen_at = factory.LazyFunction(timezone.now)


class SensorReadingFactory(DjangoModelFactory):
    class Meta:
        model = SensorReading

    sensor        = factory.SubFactory(SensorFactory)
    sensor_id_raw = factory.LazyAttribute(lambda o: o.sensor.sensor_id)

    timestamp     = factory.Sequence(
        lambda n: timezone.now() - timedelta(minutes=n)
    )

    temperature   = 21.5
    humidity      = 55.0
    co2_ppm       = 420.0
    battery_level = 85.0
    is_raw        = True
    is_validated  = False
    quality_score = 100.0
    quality_flags = factory.LazyFunction(list)
    processing_notes = ""

    class Params:
        flagged = factory.Trait(
            quality_flags=["stuck_at"],
            quality_score=75.0,
        )
        outlier = factory.Trait(
            quality_flags=["outlier"],
            quality_score=80.0,
            temperature=85.0,
        )
        degraded = factory.Trait(
            quality_flags=["stuck_at", "outlier"],
            quality_score=55.0,
        )


class DataQualityLogFactory(DjangoModelFactory):
    class Meta:
        model = DataQualityLog

    reading        = factory.SubFactory(SensorReadingFactory)
    sensor_id      = factory.LazyAttribute(lambda o: o.reading.sensor_id_raw)
    detection_type = DataQualityLog.DetectionType.STUCK_AT
    severity       = "warning"
    description    = "stuck_at detected in temperature"
    context        = factory.LazyFunction(dict)
    detected_at    = factory.Sequence(
        lambda n: timezone.now() - timedelta(minutes=n)
    )