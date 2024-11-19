# Utiliser l'image de base Ubuntu
FROM ubuntu:latest

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Copie le fichier .env dans l'image
COPY .env ./

# Install any needed packages specified in requirements.txt
RUN apt update && apt install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update && apt install -y build-essential libssl-dev libffi-dev python3.12 python3-pip python3.12-venv python3-dev cron
RUN apt install nano
# Create a virtual environment and activate it
RUN python3.12 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Mettre à jour pip
RUN python3 -m pip install --upgrade pip

RUN pip3 install --upgrade setuptools

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Collect static files
#RUN python3 manage.py collectstatic --noinput

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Copy the entrypoint script into the image
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint to the script that will handle crons and daphne
ENTRYPOINT ["/app/entrypoint.sh"]
