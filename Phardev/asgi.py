import os

from channels.routing import ProtocolTypeRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Phardev.settings")

# Initialize Django ASGI application before importing any other modules
django_asgi_app = get_asgi_application()

# Define the ASGI application
application = ProtocolTypeRouter({
    # HTTP requests handled by Django ASGI
    "http": django_asgi_app,
})
