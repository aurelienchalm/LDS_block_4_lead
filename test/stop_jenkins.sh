#!/bin/bash

set -e  # Arrête le script en cas d'erreur
set -u  # Erreur si une variable non définie est utilisée

# === Variables ===
JENKINS_CONTAINER="jenkins-blueocean"
DOCKER_DAEMON_CONTAINER="jenkins-docker"

# === Arrêt de Jenkins ===
echo "🛑 Arrêt du conteneur Jenkins ($JENKINS_CONTAINER)..."
if docker ps -a --format '{{.Names}}' | grep -q "^$JENKINS_CONTAINER$"; then
  docker stop $JENKINS_CONTAINER >/dev/null && echo "✅ Jenkins arrêté."
  docker rm $JENKINS_CONTAINER >/dev/null && echo "🧹 Jenkins supprimé."
else
  echo "⚠️  Jenkins n'était pas en cours d'exécution."
fi

# === Arrêt du daemon Docker (DinD) ===
echo "🛑 Arrêt du conteneur Docker DinD ($DOCKER_DAEMON_CONTAINER)..."
if docker ps -a --format '{{.Names}}' | grep -q "^$DOCKER_DAEMON_CONTAINER$"; then
  docker stop $DOCKER_DAEMON_CONTAINER >/dev/null && echo "✅ Docker DinD arrêté."
  docker rm $DOCKER_DAEMON_CONTAINER >/dev/null && echo "🧹 Docker DinD supprimé."
else
  echo "⚠️  Docker DinD n'était pas en cours d'exécution."
fi

echo "✅ Tous les conteneurs Jenkins du projet ont été arrêtés et nettoyés."