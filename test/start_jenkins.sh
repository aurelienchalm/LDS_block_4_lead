#!/bin/bash

set -e  # Arrête le script en cas d'erreur
set -u  # Erreur si une variable non définie est utilisée

# === Variables ===
NETWORK_NAME="jenkins"
DOCKER_DAEMON_CONTAINER="jenkins-docker"
JENKINS_CONTAINER="jenkins-blueocean"
IMAGE_NAME="myjenkins-blueocean:2.504.2-1"

# === Création du réseau s'il n'existe pas ===
if ! docker network ls --format '{{.Name}}' | grep -q "^$NETWORK_NAME$"; then
  echo "🔧 Création du réseau Docker : $NETWORK_NAME"
  docker network create $NETWORK_NAME
else
  echo "✅ Réseau Docker déjà existant : $NETWORK_NAME"
fi

# === Démarrage du daemon Docker DinD ===
if docker ps -a --format '{{.Names}}' | grep -q "^$DOCKER_DAEMON_CONTAINER$"; then
  echo "⚠️ Conteneur $DOCKER_DAEMON_CONTAINER déjà existant. Suppression..."
  docker rm -f $DOCKER_DAEMON_CONTAINER
fi

echo "🚀 Lancement du daemon Docker (DinD) pour Jenkins..."
docker run -d --name $DOCKER_DAEMON_CONTAINER \
  --privileged --network $NETWORK_NAME --network-alias docker \
  --env DOCKER_TLS_CERTDIR=/certs \
  --volume jenkins-docker-certs:/certs/client \
  --volume jenkins-data:/var/jenkins_home \
  --publish 2376:2376 \
  docker:dind --storage-driver overlay2

# === Lancement du conteneur Jenkins ===
if docker ps -a --format '{{.Names}}' | grep -q "^$JENKINS_CONTAINER$"; then
  echo "⚠️ Conteneur $JENKINS_CONTAINER déjà existant. Suppression..."
  docker rm -f $JENKINS_CONTAINER
fi

echo "🚀 Lancement de Jenkins sur http://localhost:8080 ..."
docker run -d --name $JENKINS_CONTAINER --restart=on-failure \
  --network $NETWORK_NAME \
  --env DOCKER_HOST=tcp://docker:2376 \
  --env DOCKER_CERT_PATH=/certs/client \
  --env DOCKER_TLS_VERIFY=1 \
  --publish 8080:8080 --publish 50000:50000 \
  --volume jenkins-data:/var/jenkins_home \
  --volume jenkins-docker-certs:/certs/client:ro \
  $IMAGE_NAME

# === Affichage du mot de passe initial Jenkins ===
echo "🔐 Mot de passe initial Jenkins (si première initialisation) :"
docker exec $JENKINS_CONTAINER cat /var/jenkins_home/secrets/initialAdminPassword 2>/dev/null || echo "🟡 Jenkins déjà initialisé"