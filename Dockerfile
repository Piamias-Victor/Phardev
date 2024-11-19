# Utiliser l'image de base Ubuntu
FROM ubuntu:latest

# Set the working directory
WORKDIR /app

COPY . /app
COPY .env ./

# Install any needed packages specified in requirements.txt
RUN apt update && apt install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update && apt install -y build-essential libssl-dev libffi-dev python3.12 python3-pip python3.12-venv python3-dev cron
RUN apt install nano

RUN python3.12 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip3 install --upgrade pip setuptools
RUN pip3 install --no-cache-dir -r requirements.txt

# Collect static files
#RUN python3 manage.py collectstatic --noinput

# Make port 8000 available to the world outside this container
EXPOSE 8000
CMD ["daphne", "-u", "/tmp/daphne.sock", "-b", "0.0.0.0", "-p", "8000", "Phardev.asgi:application"]
