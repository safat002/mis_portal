# django_mis_project/urls.py
"""
Main URL configuration for Django MIS Project
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.shortcuts import redirect

def root_redirect(request):
    """Redirect root URL to the MIS app."""
    return redirect('mis_app:root')

urlpatterns = [
    path('admin/', admin.site.urls),

    path('', include(('mis_app.urls', 'mis_app'))),
    path('intelligent-import/', include('intelligent_import.urls')),
]

# Serve media files during development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# Custom error handlers
handler404 = 'mis_app.views.custom_404'
handler500 = 'mis_app.views.custom_500'
