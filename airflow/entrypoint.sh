#!/bin/bash
set -e

echo "📦 Installation des dépendances..."
pip install --no-cache-dir -r /requirements.txt

echo "🗄️ Initialisation de la base de données Airflow..."
airflow db migrate

echo "👤 Création de l'utilisateur admin..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || echo "User already exists"

echo "🚀 Démarrage d'Airflow..."
airflow webserver --port 8080 &
airflow scheduler
