# sensor/management/commands/import_sensors.py
"""
Django management command for importing sensor data.

Usage:
    python manage.py import_sensors path/to/data.csv
    python manage.py import_sensors path/to/data.csv --skip-quality

Design Decision: Keep command thin; delegate to services.py.
This makes the logic testable without Django's command framework.

FIX: Now properly passes --skip-quality flag to service layer.
"""

import os

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from apps.sensor.services import import_sensor_data, IngestionStats


class Command(BaseCommand):
    help = "Import sensor data from CSV file with quality checks"

    def add_arguments(self, parser):
        parser.add_argument(
            "file_path",
            type=str,
            help="Path to CSV file containing sensor data"
        )
        parser.add_argument(
            "--skip-quality",
            action="store_true",
            help="Skip quality checks (faster, for initial load)"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate file without inserting data"
        )

    def handle(self, *args, **options):
        file_path = options["file_path"]
        skip_quality = options["skip_quality"]  # FIX: Capture the flag
        dry_run = options["dry_run"]

        # Validate file exists
        if not os.path.exists(file_path):
            raise CommandError(f"File not found: {file_path}")

        if not file_path.endswith(".csv"):
            raise CommandError(f"Expected CSV file, got: {file_path}")

        self.stdout.write(f"Processing: {file_path}")
        self.stdout.write(f"Skip quality checks: {skip_quality}")  # FIX: Log the flag
        self.stdout.write(f"Dry run: {dry_run}")
        self.stdout.write("-" * 50)

        if dry_run:
            # Just validate the file can be read
            self._validate_file(file_path)
            self.stdout.write(self.style.SUCCESS("✓ File validation passed"))
            return

        # Run import
        try:
            # FIX: Pass skip_quality to service
            stats = import_sensor_data(file_path, skip_quality=skip_quality)
            self._print_stats(stats)
        except Exception as e:
            raise CommandError(f"Import failed: {str(e)}")

    def _validate_file(self, file_path: str) -> None:
        """Validate CSV can be read by Polars."""
        import polars as pl
        try:
            df = pl.read_csv(file_path, n_rows=10)
            required_cols = ["timestamp", "sensor_id"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise CommandError(f"Missing required columns: {missing}")
            self.stdout.write(f"✓ Found {len(df.columns)} columns")
            self.stdout.write(f"✓ Sample sensors: {df['sensor_id'].unique().to_list()[:3]}")
        except Exception as e:
            raise CommandError(f"File validation failed: {str(e)}")

    def _print_stats(self, stats: IngestionStats) -> None:
        """Print ingestion statistics."""
        self.stdout.write(self.style.SUCCESS("✓ Import completed successfully"))
        self.stdout.write("-" * 50)
        self.stdout.write(f"Total rows processed: {stats.total_rows}")
        self.stdout.write(f"New rows inserted:    {stats.inserted_rows}")
        self.stdout.write(f"Duplicates skipped:   {stats.skipped_duplicates}")
        self.stdout.write(f"Quality issues found: {stats.quality_issues_found}")
        self.stdout.write(f"Sensors detected:     {len(stats.sensors_seen)}")
        self.stdout.write("-" * 50)
        self.stdout.write(f"Sensors: {', '.join(stats.sensors_seen[:5])}")
        if len(stats.sensors_seen) > 5:
            self.stdout.write(f"         ... and {len(stats.sensors_seen) - 5} more")