from .base import *

# Debug
DEBUG = True

ALLOWED_HOSTS = ["localhost", "127.0.0.1", "web", "0.0.0.0"]

INSTALLED_APPS += [
    "django_extensions",
    "drf_spectacular",
    "silk",
]

# Development middleware
MIDDLEWARE = [
    "silk.middleware.SilkyMiddleware",
] + MIDDLEWARE


# Swagger/OpenAPI settings for development
SPECTACULAR_SETTINGS = {
    "TITLE": "SmartHome API",
    "DESCRIPTION": "API documentation for SmartHome project",
    "VERSION": "1.0.0",
    "SERVE_INCLUDE_SCHEMA": False,
}

REST_FRAMEWORK.update(
    {
        "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
        "DEFAULT_RENDERER_CLASSES": [
            "rest_framework.renderers.JSONRenderer",
            "rest_framework.renderers.BrowsableAPIRenderer",  # Added, not replaced
        ],
    }
)

EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

# Swagger/OpenAPI settings for development
SWAGGER_SETTINGS = {
    "SECURITY_DEFINITIONS": {
        "Bearer": {"type": "apiKey", "name": "Authorization", "in": "header"}
    },
    "USE_SESSION_AUTH": False,
}

LOGGING["loggers"]["django.db.backends"] = {
    "handlers": ["console"],
    "level": "INFO",
    "propagate": False,
}


SILKY_PYTHON_PROFILER = True
SILKY_IGNORE_PATHS = ["/health/", "/silk/", "/admin/jsi18n/"]

CORS_ALLOWED_ORIGINS = []
