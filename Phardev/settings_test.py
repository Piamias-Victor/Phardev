import os
from pathlib import Path

from dotenv import load_dotenv

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env
load_dotenv()

# Environment variables
SECRET_KEY = os.getenv("SECRET_KEY", "django-test-key-for-development-only-not-secure-12345")
ENV = os.getenv("ENV", "DEV")  # Default to 'DEV' if ENV is not set

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# Database and email credentials
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")

# AWS S3 Settings
AWS_STORAGE_BUCKET_NAME = "phardev"
AWS_S3_REGION_NAME = "eu-west-3"
AWS_S3_FILE_OVERWRITE = False
AWS_DEFAULT_ACL = None
AWS_S3_VERIFY = True
AWS_S3_CUSTOM_DOMAIN = f"{AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com"
AWS_S3_OBJECT_PARAMETERS = {"CacheControl": "max-age=86400"}
AWS_LOCATION = "static"
MEDIA_URL = f"https://{AWS_S3_CUSTOM_DOMAIN}/"

# Debug and static files settings based on environment
DEBUG = ENV == "DEV"
if DEBUG:
    STATIC_URL = "/staticfiles/"
    STATIC_ROOT = BASE_DIR / "staticfiles"

else:
    STATIC_URL = f"https://{AWS_S3_CUSTOM_DOMAIN}/{AWS_LOCATION}/"
    STORAGES = {
        "default": {
            "BACKEND": "Phardev.storage_backends.MediaStorage",
            "OPTIONS": {
                "access_key": AWS_ACCESS_KEY_ID,
                "secret_key": AWS_SECRET_ACCESS_KEY,
                "bucket_name": AWS_STORAGE_BUCKET_NAME,
                "region_name": AWS_S3_REGION_NAME,
            },
        },
        "staticfiles": {
            "BACKEND": "Phardev.storage_backends.StaticStorage",
            "OPTIONS": {
                "access_key": AWS_ACCESS_KEY_ID,
                "secret_key": AWS_SECRET_ACCESS_KEY,
                "bucket_name": AWS_STORAGE_BUCKET_NAME,
                "region_name": AWS_S3_REGION_NAME,
            },
        },
    }

    SECURE_SSL_REDIRECT = True
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

    LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
            },
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console"],
        },
        "loggers": {
            "django": {
                "handlers": ["console"],
                "level": "DEBUG",
                "propagate": False,
            },        'django.db.backends': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        },
    }

# Allowed hosts and CORS settings
ALLOWED_HOSTS = ['api.phardev.fr', 'localhost', '127.0.0.1']
CORS_ALLOWED_ORIGINS = [
    'https://api.phardev.fr',
    'http://localhost', 'http://127.0.0.1'
]
CSRF_TRUSTED_ORIGINS = CORS_ALLOWED_ORIGINS

# URL configurations
ROOT_URLCONF = "Phardev.urls"
# ASGI_APPLICATION = "Phardev.asgi.application"  # Désactivé pour tests (channels non installé)
WSGI_APPLICATION = "Phardev.wsgi.application"  # Utiliser WSGI pour tests

# Installed apps
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    # "daphne",  # Désactivé pour tests
    "django.contrib.staticfiles",
    # 'storages',  # Désactivé pour tests (pas besoin S3)
    "data",
]

# Middleware configuration
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",

]
INTERNAL_IPS = [
    'api.phardev.fr',
    "127.0.0.1",
    # ...
]

# Template settings
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            BASE_DIR / "templates",
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# ============================================================================
# DATABASE SETTINGS - MODIFIÉ POUR TEST AVEC SQLITE
# ============================================================================

# Configuration SQLite pour tests (plus simple et rapide)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db_test.sqlite3',  # Fichier séparé pour les tests
    }
}

# Configuration PostgreSQL originale (commentée pour tests)
# DATABASES = {
#     "default": {
#         "ENGINE": "django.db.backends.postgresql_psycopg2",
#         "NAME": "postgres",
#         "USER": "postgres",
#         "PASSWORD": DB_PASSWORD,
#         "HOST": DB_HOST,
#         "PORT": 5432,
#         # "OPTIONS": {"options": "-c search_path=dev"},
#     }
# }

# ============================================================================
# LOGGING CONFIGURATION - AJOUTÉ POUR DEBUG
# ============================================================================

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "file": {
            "level": "DEBUG",
            "class": "logging.FileHandler",
            "filename": BASE_DIR / "debug.log",
            "formatter": "verbose",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
        "data": {  # Logs spécifiques à notre app
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
        "data.services": {  # Logs pour nos services
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Europe/Paris"
USE_I18N = True
USE_TZ = True

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Data upload settings
DATA_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024  # 10 MB
DATA_UPLOAD_MAX_NUMBER_FIELDS = 5000

# ============================================================================
# CONFIGURATION SPÉCIFIQUE POUR TESTS
# ============================================================================

# Désactiver les vérifications CORS pour les tests locaux
if DEBUG:
    CORS_ALLOW_ALL_ORIGINS = True
    CORS_ALLOW_CREDENTIALS = True
    
# Timeout plus élevé pour les requêtes API
DEFAULT_TIMEOUT = 60

print(f"🔧 CONFIGURATION TEST:")
print(f"   📂 Database: SQLite ({DATABASES['default']['NAME']})")
print(f"   🐛 Debug: {DEBUG}")
print(f"   📝 Logs: Console + File")
print(f"   🌐 CORS: {'All origins' if DEBUG else 'Restricted'}")