from django.conf import settings
from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse

from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularSwaggerView,
    SpectacularRedocView,
)


def health_check(request):
    return JsonResponse({"status": "ok"})


urlpatterns = [
    path("admin/", admin.site.urls),
    path("health/", health_check),
    # OpenAPI schema
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    # Swagger UI
    path(
        "api/swagger/",
        SpectacularSwaggerView.as_view(url_name="schema"),
        name="swagger-ui",
    ),
    # ReDoc
    path("api/redoc/", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
]

# URL Apps Api
urlpatterns += [
    path("api/", include("apps.sensor.urls", namespace="sensor")),
]

# Add Debug Toolbar URLs only in development
if settings.DEBUG:
    urlpatterns = [
        path("silk/", include("silk.urls", namespace="silk")),
    ] + urlpatterns
