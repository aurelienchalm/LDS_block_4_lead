#!/bin/bash

echo "📦 Arrêt d'Airflow en cours..."
docker-compose down

echo "🔧 Rebuild des images si nécessaire..."
docker-compose build

echo "🚀 Redémarrage d'Airflow..."
docker-compose up

echo "✅ Airflow redémarré. Conteneurs actifs :"
docker ps
