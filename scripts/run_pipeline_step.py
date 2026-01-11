#!/usr/bin/env python3
"""
Pipeline Step Runner for Airflow

Ce script permet d'exécuter des commandes dans d'autres containers Docker
depuis Airflow, en gérant les interactions entre services.
"""

import subprocess
import sys
import os
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_docker_exec(container_name, command, cwd=None):
    """Exécute une commande dans un container Docker"""
    try:
        full_command = f"docker exec {container_name} {command}"
        logger.info(f"Exécution: {full_command}")

        result = subprocess.run(
            full_command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )

        if result.returncode == 0:
            logger.info("Commande exécutée avec succès")
            return True, result.stdout
        else:
            logger.error(f"Échec de la commande: {result.stderr}")
            return False, result.stderr

    except subprocess.TimeoutExpired:
        logger.error("Timeout de la commande")
        return False, "Timeout"
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution: {e}")
        return False, str(e)

def create_hdfs_directories(date_str):
    """Crée les répertoires HDFS nécessaires"""
    logger.info("Création des répertoires HDFS...")

    commands = [
        f"hdfs dfs -mkdir -p /data/raw/traffic/{date_str}",
        "hdfs dfs -mkdir -p /data/processed/traffic",
        "hdfs dfs -mkdir -p /data/analytics/traffic"
    ]

    for cmd in commands:
        success, output = run_docker_exec("namenode", cmd)
        if not success:
            return False

    return True

def run_spark_job(job_type, date_str=None):
    """Exécute un job Spark"""
    logger.info(f"Exécution du job Spark: {job_type}")

    # Configuration commune Spark
    spark_base = "/opt/spark/bin/spark-submit"
    spark_config = "--master spark://spark-master:7077 --deploy-mode client --driver-memory 1g --executor-memory 1g --total-executor-cores 2"

    if job_type == "processing":
        script = "/opt/spark/scripts/traffic_processor.py"
        packages = ""
    elif job_type == "analytics":
        script = "/opt/spark/scripts/analytics-processor.py"
        packages = ""
    elif job_type == "export":
        script = "/opt/spark/scripts/export_to_mysql.py"
        packages = "--packages mysql:mysql-connector-java:8.0.33"
    else:
        logger.error(f"Type de job inconnu: {job_type}")
        return False

    spark_command = f"{spark_base} {spark_config} {packages} --class main {script}"

    success, output = run_docker_exec("spark-master", spark_command)
    return success

def verify_hdfs_storage(date_str):
    """Vérifie que les données sont bien stockées dans HDFS"""
    logger.info("Vérification du stockage HDFS...")

    commands = [
        f"hdfs dfs -ls -h /data/raw/traffic/{date_str}/",
        f"hdfs dfs -du -h /data/raw/traffic/{date_str}/"
    ]

    for cmd in commands:
        success, output = run_docker_exec("namenode", cmd)
        if not success:
            return False

    return True

def verify_mysql_export():
    """Vérifie que les données sont bien exportées vers MySQL"""
    logger.info("Vérification de l'export MySQL...")

    commands = [
        "mysql -u grafana -pgrafana traffic_db -e 'SHOW TABLES;'",
        "mysql -u grafana -pgrafana traffic_db -e 'SELECT COUNT(*) FROM trafic_par_zone;'"
    ]

    for cmd in commands:
        success, output = run_docker_exec("mysql-traffic", cmd)
        if not success:
            return False

    return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline_step.py <action> [args...]")
        sys.exit(1)

    action = sys.argv[1]

    try:
        if action == "create_hdfs_dirs":
            if len(sys.argv) < 3:
                print("Usage: python run_pipeline_step.py create_hdfs_dirs <date>")
                sys.exit(1)
            date_str = sys.argv[2]
            success = create_hdfs_directories(date_str)

        elif action == "spark_processing":
            success = run_spark_job("processing")

        elif action == "spark_analytics":
            success = run_spark_job("analytics")

        elif action == "spark_export":
            success = run_spark_job("export")

        elif action == "verify_hdfs":
            if len(sys.argv) < 3:
                print("Usage: python run_pipeline_step.py verify_hdfs <date>")
                sys.exit(1)
            date_str = sys.argv[2]
            success = verify_hdfs_storage(date_str)

        elif action == "verify_mysql":
            success = verify_mysql_export()

        else:
            print(f"Action inconnue: {action}")
            print("Actions disponibles: create_hdfs_dirs, spark_processing, spark_analytics, spark_export, verify_hdfs, verify_mysql")
            sys.exit(1)

        if success:
            logger.info(f"Action '{action}' exécutée avec succès")
            sys.exit(0)
        else:
            logger.error(f"Échec de l'action '{action}'")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
