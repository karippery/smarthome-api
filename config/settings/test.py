# config/settings/test.py
from .development import *

# --- Disable Debug Toolbar in Tests ---
# Remove from INSTALLED_APPS
if "debug_toolbar" in INSTALLED_APPS:
    INSTALLED_APPS = [app for app in INSTALLED_APPS if app != "debug_toolbar"]

# Remove from MIDDLEWARE
MIDDLEWARE = [
    middleware for middleware in MIDDLEWARE if "debug_toolbar" not in middleware
]

# Override DEBUG if needed (optional)
DEBUG = False

# Keep test-specific settings
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

SECRET_KEY = "insecure-test-secret-key"

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
