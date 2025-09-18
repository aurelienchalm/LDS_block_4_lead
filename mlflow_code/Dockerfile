FROM python:3.10-slim

# Installer les dépendances système
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

# Créer le dossier de travail
WORKDIR /app

# Copier le contenu du projet
#COPY . /app
COPY . .

#COPY requirements.txt .
#RUN pip install --upgrade pip \
#    && pip install -r requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Point d'entrée
CMD ["python", "train.py"]