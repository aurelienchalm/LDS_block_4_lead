FROM python:3.10-slim

# Dossier de travail dans le conteneur
WORKDIR /code/app

# Dépendances système
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copier l'ensemble du dossier app/ dans le conteneur
COPY . /code/app/

# Copier le .env depuis le dossier parent
COPY .env /code/.env

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 4000

# Définir le PYTHONPATH pour pouvoir importer app.*
ENV PYTHONPATH=/code

#CMD
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4000"]