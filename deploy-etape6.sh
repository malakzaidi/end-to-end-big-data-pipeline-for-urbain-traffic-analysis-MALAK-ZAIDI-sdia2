#!/bin/bash
# deploy.sh

echo "=========================================="
echo "DÉPLOIEMENT DE LA STACK BIG DATA"
echo "=========================================="

# 1. Arrêter les services existants
echo "1. Arrêt des services existants..."
docker-compose down 2>/dev/null

# 2. Construire l'image Spark
echo "2. Construction de l'image Spark..."
docker build -t spark-hadoop:3.5.0 .

# 3. Démarrer les services
echo "3. Démarrage des services..."
docker-compose up -d

# 4. Attendre que les services soient prêts
echo "4. Attente du démarrage des services..."
sleep 30

# 5. Initialiser HDFS
echo "5. Initialisation de HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data/kpi
docker exec namenode hdfs dfs -chmod 777 /data

# 6. Exécuter le calcul des KPI
echo "6. Calcul des KPI..."
docker exec spark-master python3 /opt/spark/scripts/calculate_kpis_etape6.py

# 7. Vérifier les services
echo "7. Vérification des services..."
echo ""
echo " SERVICES DÉMARRÉS:"
echo "   - HDFS: http://localhost:9870"
echo "   - Spark UI: http://localhost:8090"
echo "   - Grafana: http://localhost:3000"
echo "   - Airflow: http://localhost:8081"
echo ""
echo " ACCÈS AUX INTERFACES:"
echo "   Grafana: http://localhost:3000 (admin/admin)"
echo "   Dashboard: 'Traffic Urbain Dashboard'"
echo ""
echo " COMMANDES UTILES:"
echo "   - Voir les logs: docker-compose logs -f"
echo "   - Recréer les KPI: docker exec spark-master python3 /opt/spark/scripts/calculate_kpis_etape6.py"
echo "   - Arrêter: docker-compose down"
echo ""
echo "=========================================="