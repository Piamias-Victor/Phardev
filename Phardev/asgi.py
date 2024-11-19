import os

from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Phardev.settings")

# Initialize Django ASGI application before importing any other modules
django_asgi_app = get_asgi_application()

# Define the ASGI application
application = ProtocolTypeRouter({
    # HTTP requests handled by Django ASGI
    "http": django_asgi_app,

    # Uncomment the below lines if WebSocket support is needed
    # "websocket": AuthMiddlewareStack(
    #     URLRouter(
    #         # Import and define WebSocket URL routes here
    #         # Example: websocket_urlpatterns
    #     )
    # )
})