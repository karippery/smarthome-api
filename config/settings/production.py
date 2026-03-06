from .base import *

# SECURITY WARNING: keep the secret key used in production secret!
# Already loaded from .env in base.py – ensure it's NEVER committed.
SECRET_KEY = env("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

# Hosts/domain names that this Django site can serve
# Required when DEBUG=False
ALLOWED_HOSTS = env.list("ALLOWED_HOSTS", default=[])

# Enforce HTTPS
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SECURE_HSTS_SECONDS = 31536000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

# Secure cookies
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_REFERRER_POLICY = "same-origin"

# Prevent clickjacking
X_FRAME_OPTIONS = "DENY"



# Logging: output JSON in production for better parsing
# (e.g., by Sentry or log aggregators)
LOGGING["handlers"]["console"]["formatter"] = "json"

# Disable development-only apps
# (Sentry, Whitenoise, Redis already handled – no debug/silk/drfspectacular in prod)

# Static files – WhiteNoise is already configured in base.py
# Ensure STATIC_ROOT exists and `collectstatic` is run during deployment

# REST Framework: disable browsable API in production (already done in base.py)

# CORS – configure if your frontend is on a different domain
# In production, never allow wildcards
CORS_ALLOWED_ORIGINS = env.list("CORS_ALLOWED_ORIGINS", default=[])
# If you must allow all (not recommended), use: CORS_ALLOW_ALL_ORIGINS = False

# Session engine is already set to Redis in base.py – good for production

# Security: disable directory listing and unnecessary features
# (Handled by middleware and web server –
#  Django doesn’t serve static/media in production)

# Ensure no development-only middleware (Silk, DebugToolbar)
#  is loaded – you’re already safe because
# production.py inherits from base.py, not development.py

# Site framework (needed for password resets, etc.)
SITE_ID = env.int("SITE_ID", default=1)

# Optional: set ADMINS for error emails (if using AdminEmailHandler)
ADMINS = env.list(
    "ADMINS", default=[], subcast=lambda x: tuple(x.split(":")) if ":" in x else x
)
# Format in .env: ADMINS="John:john@example.com,Mary:mary@example.com"

# Media files: ensure your reverse proxy (nginx, etc.) serves /media/ securely
# and never allows execution of uploaded files (e.g., block .php)

# Optional: tighten file upload limits
# DATA_UPLOAD_MAX_MEMORY_SIZE = 2621440  # already default
# FILE_UPLOAD_MAX_MEMORY_SIZE = 2621440

# Optional: add health check exemptions if you have a /health/ endpoint
# SECURE_REDIRECT_EXEMPT = [r"^health/$"]

# Ensure PostgreSQL connection uses health checks
DATABASES["default"]["CONN_HEALTH_CHECKS"] = True
DATABASES["default"]["CONN_MAX_AGE"] = 60  # persistent connections

# Explicitly disable browsable API
REST_FRAMEWORK["DEFAULT_RENDERER_CLASSES"] = ("rest_framework.renderers.JSONRenderer",)
