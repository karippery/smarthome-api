#!/bin/sh
set -e

echo "Starting Django dev container..."

# Ensure Django uses the correct settings module
export DJANGO_SETTINGS_MODULE=${DJANGO_SETTINGS_MODULE:-config.settings.development}

echo "Starting Django dev container (using $DJANGO_SETTINGS_MODULE)..."
# Wait for Postgres
if [ -n "$DB_HOST" ]; then
  echo "Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."
  while ! nc -z "$DB_HOST" "$DB_PORT"; do
    sleep 1
  done
  echo "PostgreSQL is available"
fi

# Wait for Redis
if [ -n "$REDIS_HOST" ]; then
  echo "Waiting for Redis at $REDIS_HOST:$REDIS_PORT..."
  while ! nc -z "$REDIS_HOST" "$REDIS_PORT"; do
    sleep 1
  done
  echo "Redis is available"
fi

# Migrations
echo "Running migrations..."
python manage.py migrate --noinput



# Collect static files (optional for dev)
if [ "$DJANGO_COLLECTSTATIC" = "1" ]; then
  echo "Collecting static files..."
  python manage.py collectstatic --noinput --clear
fi

# Create superuser (optional)
if [ "$DJANGO_SUPERUSER_USERNAME" ]; then
  echo "Creating superuser if not exists..."
  python manage.py createsuperuser \
    --noinput \
    --username "$DJANGO_SUPERUSER_USERNAME" \
    --email "$DJANGO_SUPERUSER_EMAIL" || true
fi

echo "Starting development server..."
exec "$@"
