#!/bin/bash

# ============================================
# SCRIPT DE LANCEMENT - SYSTÈME BIG DATA COMPLET
# Pipeline Smart City Traffic Analysis
# ============================================

echo "🚀 DÉMARRAGE DU SYSTÈME BIG DATA COMPLET"
echo "=========================================="
echo ""

# Fonction de vérification
check_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    echo "🔍 Vérification de $service..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s --max-time 5 "$url" > /dev/null 2>&1; then
            echo "✅ $service est opérationnel"
            return 0
        fi

        echo "⏳ Attente de $service (tentative $attempt/$max_attempts)..."
        sleep 5
        ((attempt++))
    done

    echo "❌ $service n'est pas accessible après $max_attempts tentatives"
    return 1
}

# 1. Arrêter les services existants
echo "🛑 Arrêt des services existants..."
docker compose -f docker-compose-airflow.yml down 2>/dev/null || true
docker compose down 2>/dev/null || true
echo "✅ Services arrêtés"
echo ""

# 2. Lancer tous les services
echo "🏗️ Construction et lancement de tous les services..."
echo "Cela peut prendre plusieurs minutes..."
echo ""

docker compose -f docker-compose-final.yml up -d --build

if [ $? -ne 0 ]; then
    echo "❌ Erreur lors du lancement des services"
    exit 1
fi

echo ""
echo "✅ Services en cours de démarrage..."
echo ""

# 3. Attendre que les services soient prêts
echo "⏳ Attente de l'initialisation complète des services..."
sleep 30

# 4. Vérifications des services
echo ""
echo "🔍 VÉRIFICATION DES SERVICES"
echo "============================"

services_ok=true

# Airflow
if check_service "Airflow Web UI" "http://localhost:8081/health"; then
    echo "🌐 Airflow: http://localhost:8081 (admin/admin)"
else
    services_ok=false
fi

# Grafana
if check_service "Grafana" "http://localhost:3000/api/health"; then
    echo "📊 Grafana: http://localhost:3000 (admin/admin)"
else
    services_ok=false
fi

# HDFS
if check_service "HDFS NameNode" "http://localhost:9870"; then
    echo "🗄️ HDFS NameNode: http://localhost:9870"
else
    services_ok=false
fi

# Spark
if check_service "Spark Master" "http://localhost:8090"; then
    echo "⚡ Spark Master: http://localhost:8090"
else
    services_ok=false
fi

echo ""
echo "📋 ÉTAT FINAL"
echo "============="

if [ "$services_ok" = true ]; then
    echo "🎉 TOUS LES SERVICES SONT OPÉRATIONNELS !"
    echo ""
    echo "🚀 INSTRUCTIONS D'UTILISATION :"
    echo "=============================="
    echo ""
    echo "1️⃣ Ouvrir Airflow: http://localhost:8081"
    echo "   → Login: admin/admin"
    echo "   → Cliquer sur 'smart_city_traffic_pipeline'"
    echo "   → Cliquer sur 'Trigger DAG'"
    echo ""
    echo "2️⃣ Observer l'exécution en temps réel"
    echo "   → 11 tâches séquentielles"
    echo "   → Durée: 5-8 minutes"
    echo ""
    echo "3️⃣ Voir les résultats dans Grafana:"
    echo "   → Heat Map Géographique"
    echo "   → Prédictions ML temps réel"
    echo "   → 23 panels de visualisation"
    echo ""
    echo "🎯 PIPELINE COMPLÈTE:"
    echo "   Générateur → Kafka → HDFS → Spark → MySQL → Grafana"
    echo ""
    echo "🏆 PROJET BIG DATA TERMINÉ AVEC SUCCÈS !"
else
    echo "⚠️ Certains services ne sont pas encore prêts"
    echo ""
    echo "🔧 COMMANDES DE DIAGNOSTIC :"
    echo "docker compose -f docker-compose-final.yml ps"
    echo "docker compose -f docker-compose-final.yml logs airflow-webserver"
    echo "docker compose -f docker-compose-final.yml logs grafana"
    echo ""
    echo "💡 Attendre encore quelques minutes puis relancer ce script"
fi

echo ""
echo "=========================================="
echo "FIN DU SCRIPT DE LANCEMENT"
