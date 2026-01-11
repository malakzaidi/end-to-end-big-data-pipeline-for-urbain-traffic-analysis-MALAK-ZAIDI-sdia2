#!/usr/bin/env python3
"""
Script pour exécuter la pipeline Big Data complète manuellement

Ce script remplace Airflow temporairement et exécute toutes les étapes :
1. Génération des données
2. Ingestion Kafka
3. Stockage HDFS
4. Traitement Spark
5. Analytics
6. Export MySQL
7. Rapports
"""

import subprocess
import sys
import os
import time
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_command(command, description, timeout=300):
    """Exécute une commande avec timeout et logging"""
    logger.info(f"🚀 Démarrage: {description}")
    logger.info(f"Commande: {command}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode == 0:
            logger.info(f"✅ Succès: {description}")
            if result.stdout:
                logger.info(f"Output: {result.stdout[:500]}...")
            return True, result.stdout
        else:
            logger.error(f"❌ Échec: {description}")
            logger.error(f"Erreur: {result.stderr}")
            return False, result.stderr

    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Timeout: {description}")
        return False, "Timeout"
    except Exception as e:
        logger.error(f"💥 Exception: {description} - {e}")
        return False, str(e)

def step_1_generate_data():
    """Étape 1: Génération des données"""
    logger.info("🔄 ÉTAPE 1: GÉNÉRATION DES DONNÉES")

    # Créer le répertoire data
    os.makedirs('data', exist_ok=True)

    # Générer les données
    success, output = run_command(
        "python3 traffic_data_generator.py",
        "Génération des événements de trafic",
        timeout=60
    )

    if success:
        # Vérifier que le fichier a été créé
        if os.path.exists('traffic_events.json'):
            with open('traffic_events.json', 'r') as f:
                lines = sum(1 for _ in f)
            logger.info(f"📊 {lines} événements générés")
            return True
        else:
            logger.error("Fichier traffic_events.json non trouvé")
            return False

    return success

def step_2_kafka_ingestion():
    """Étape 2: Ingestion Kafka"""
    logger.info("🔄 ÉTAPE 2: INGESTION KAFKA")

    success, output = run_command(
        "python3 kafka-producer.py",
        "Ingestion des données dans Kafka",
        timeout=120
    )

    return success

def step_3_hdfs_storage():
    """Étape 3: Stockage HDFS"""
    logger.info("🔄 ÉTAPE 3: STOCKAGE HDFS")

    # Utiliser le script wrapper pour HDFS
    success, output = run_command(
        "python3 scripts/run_pipeline_step.py create_hdfs_dirs $(date +%Y-%m-%d)",
        "Création des répertoires HDFS",
        timeout=60
    )

    if not success:
        return False

    # Ingestion vers HDFS
    success, output = run_command(
        "python3 kafka-consumer.py --topic traffic-events --bootstrap kafka:9092 --hdfs-path /data/raw/traffic/$(date +%Y-%m-%d)/events.json --max-messages 1000",
        "Stockage des données dans HDFS",
        timeout=180
    )

    if success:
        # Vérification
        run_command(
            "python3 scripts/run_pipeline_step.py verify_hdfs $(date +%Y-%m-%d)",
            "Vérification du stockage HDFS",
            timeout=30
        )

    return success

def step_4_spark_processing():
    """Étape 4: Traitement Spark"""
    logger.info("🔄 ÉTAPE 4: TRAITEMENT SPARK")

    success, output = run_command(
        "python3 scripts/run_pipeline_step.py spark_processing",
        "Traitement analytique avec Spark",
        timeout=300
    )

    return success

def step_5_analytics():
    """Étape 5: Zone Analytics"""
    logger.info("🔄 ÉTAPE 5: ZONE ANALYTICS")

    success, output = run_command(
        "python3 scripts/run_pipeline_step.py spark_analytics",
        "Création de la zone analytics",
        timeout=300
    )

    return success

def step_6_mysql_export():
    """Étape 6: Export MySQL"""
    logger.info("🔄 ÉTAPE 6: EXPORT MYSQL")

    success, output = run_command(
        "python3 scripts/run_pipeline_step.py spark_export",
        "Export des résultats vers MySQL",
        timeout=300
    )

    if success:
        # Vérification
        run_command(
            "python3 scripts/run_pipeline_step.py verify_mysql",
            "Vérification de l'export MySQL",
            timeout=30
        )

    return success

def step_7_reports():
    """Étape 7: Génération de rapports"""
    logger.info("🔄 ÉTAPE 7: RAPPORTS")

    # Calcul des KPIs
    run_command(
        "python3 scripts/calculate_kpis_etape6.py",
        "Calcul des KPIs",
        timeout=60
    )

    # Génération du rapport
    run_command(
        "python3 scripts/visualization/generate_reports.py --date $(date +%Y-%m-%d) --output reports/traffic_report_$(date +%Y-%m-%d).pdf",
        "Génération du rapport PDF",
        timeout=60
    )

    return True

def main():
    """Fonction principale"""
    print("🚀 PIPELINE BIG DATA - EXÉCUTION MANUELLE")
    print("=" * 50)
    print(f"Début: {datetime.now()}")
    print("=" * 50)

    steps = [
        ("Génération des données", step_1_generate_data),
        ("Ingestion Kafka", step_2_kafka_ingestion),
        ("Stockage HDFS", step_3_hdfs_storage),
        ("Traitement Spark", step_4_spark_processing),
        ("Zone Analytics", step_5_analytics),
        ("Export MySQL", step_6_mysql_export),
        ("Rapports", step_7_reports)
    ]

    results = []

    for step_name, step_function in steps:
        print(f"\n▶️  {step_name}")
        print("-" * 30)

        start_time = time.time()
        success = step_function()
        end_time = time.time()

        duration = end_time - start_time
        status = "✅ RÉUSSI" if success else "❌ ÉCHEC"

        print(".1f")
        results.append((step_name, success, duration))

        if not success:
            logger.error(f"Pipeline arrêtée à l'étape: {step_name}")
            break

        # Petite pause entre les étapes
        time.sleep(2)

    # Résumé final
    print("\n" + "=" * 50)
    print("📊 RÉSUMÉ DE L'EXÉCUTION")
    print("=" * 50)

    total_time = sum(duration for _, _, duration in results)
    successful_steps = sum(1 for _, success, _ in results if success)

    for step_name, success, duration in results:
        status_icon = "✅" if success else "❌"
        print("5.1f")

    print(f"\n🎯 Résultat: {successful_steps}/{len(steps)} étapes réussies")
    print(".1f")

    if successful_steps == len(steps):
        print("\n🎉 PIPELINE COMPLÈTE AVEC SUCCÈS !")
        print("📊 Rendez-vous sur http://localhost:3000 pour voir les résultats")
        print("🗺️ La Heat Map géographique est maintenant disponible !")
    else:
        print(f"\n⚠️ Pipeline partiellement exécutée ({successful_steps}/{len(steps)} étapes)")
        print("Vérifiez les logs pour diagnostiquer les problèmes")

    print(f"\nFin: {datetime.now()}")

if __name__ == "__main__":
    main()
