# Utiliser l'image officielle Python 3.12 comme base
FROM python:3.12-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier le code source dans le conteneur
COPY . /app
COPY .env ./

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    cron \
    nano \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Mettre à jour pip et setuptools
RUN pip install --upgrade pip setuptools

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Collecter les fichiers statiques (désactivé ici, décommentez si nécessaire)
#RUN python manage.py collectstatic --noinput

# Exposer le port 8000 pour l'accès au serveur
EXPOSE 8000

# Commande de démarrage du conteneur
CMD ["daphne", "-u", "/tmp/daphne.sock", "-b", "0.0.0.0", "-p", "8000", "Phardev.asgi:application"]
