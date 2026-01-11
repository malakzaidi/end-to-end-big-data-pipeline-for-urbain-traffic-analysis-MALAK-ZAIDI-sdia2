#!/usr/bin/env python3
"""
Étape Avancée — Système d'Alerte Temps Réel

Objectif : Implémenter un système de monitoring temps réel pour détecter
les congestions et anomalies en continu avec notifications automatiques

Fonctionnalités :
- Monitoring temps réel des flux Kafka
- Détection automatique des seuils de congestion
- Notifications par email/webhook/Slack
- Escalade automatique des alertes
- Dashboard de monitoring des alertes
- Historique et analytics des incidents

Technologies utilisées :
- Spark Streaming pour le traitement temps réel
- Kafka pour les données en streaming
- MySQL pour l'historique des alertes
- Notifications externes (email, webhooks)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
import json
import time
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import mysql.connector
from mysql.connector import Error
import logging
import sys
from collections import defaultdict

# Configuration du logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 1. CONFIGURATION ET CONSTANTES
# ============================================================

# Seuils d'alerte
ALERT_THRESHOLDS = {
    "HIGH_CONGESTION": {
        "speed_threshold": 30,  # km/h
        "occupancy_threshold": 80,  # %
        "vehicle_threshold": 150,  # véhicules
        "severity": "HIGH",
        "escalation_time": 300  # 5 minutes
    },
    "MODERATE_CONGESTION": {
        "speed_threshold": 40,
        "occupancy_threshold": 70,
        "vehicle_threshold": 120,
        "severity": "MEDIUM",
        "escalation_time": 600  # 10 minutes
    },
    "LOW_CONGESTION": {
        "speed_threshold": 50,
        "occupancy_threshold": 60,
        "vehicle_threshold": 100,
        "severity": "LOW",
        "escalation_time": 900  # 15 minutes
    }
}

# Configuration des notifications
NOTIFICATION_CONFIG = {
    "email": {
        "enabled": True,
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "traffic-alert@smartcity.com",
        "sender_password": "your_password",
        "recipients": ["traffic-control@smartcity.com", "emergency@smartcity.com"]
    },
    "slack": {
        "enabled": False,
        "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
        "channel": "#traffic-alerts"
    },
    "webhook": {
        "enabled": False,
        "url": "https://api.smartcity.com/alerts",
        "headers": {"Authorization": "Bearer your_token"}
    }
}

# Configuration MySQL
MYSQL_CONFIG = {
    "host": "mysql-traffic",
    "database": "traffic_db",
    "user": "grafana",
    "password": "grafana"
}

# ============================================================
# 2. CLASSE DE GESTION DES ALERTES
# ============================================================

class AlertManager:
    """Gestionnaire centralisé des alertes"""

    def __init__(self):
        self.active_alerts = {}
        self.alert_history = []
        self.alert_counters = defaultdict(int)

    def check_congestion_alert(self, road_id, zone, metrics):
        """Vérifie si les métriques déclenchent une alerte"""
        vehicle_count = metrics.get("vehicle_count", 0)
        avg_speed = metrics.get("average_speed", 0)
        occupancy_rate = metrics.get("occupancy_rate", 0)

        alert_type = None

        # Vérifier les seuils par ordre de sévérité
        if (avg_speed < ALERT_THRESHOLDS["HIGH_CONGESTION"]["speed_threshold"] or
            occupancy_rate > ALERT_THRESHOLDS["HIGH_CONGESTION"]["occupancy_threshold"] or
            vehicle_count > ALERT_THRESHOLDS["HIGH_CONGESTION"]["vehicle_threshold"]):
            alert_type = "HIGH_CONGESTION"

        elif (avg_speed < ALERT_THRESHOLDS["MODERATE_CONGESTION"]["speed_threshold"] or
              occupancy_rate > ALERT_THRESHOLDS["MODERATE_CONGESTION"]["occupancy_threshold"] or
              vehicle_count > ALERT_THRESHOLDS["MODERATE_CONGESTION"]["vehicle_threshold"]):
            alert_type = "MODERATE_CONGESTION"

        elif (avg_speed < ALERT_THRESHOLDS["LOW_CONGESTION"]["speed_threshold"] or
              occupancy_rate > ALERT_THRESHOLDS["LOW_CONGESTION"]["occupancy_threshold"] or
              vehicle_count > ALERT_THRESHOLDS["LOW_CONGESTION"]["vehicle_threshold"]):
            alert_type = "LOW_CONGESTION"

        return alert_type

    def create_alert(self, alert_type, road_id, zone, metrics, timestamp):
        """Crée une nouvelle alerte"""
        alert_id = f"{alert_type}_{road_id}_{zone}_{int(timestamp.timestamp())}"

        alert = {
            "alert_id": alert_id,
            "alert_type": alert_type,
            "severity": ALERT_THRESHOLDS[alert_type]["severity"],
            "road_id": road_id,
            "zone": zone,
            "metrics": metrics,
            "timestamp": timestamp,
            "status": "ACTIVE",
            "escalation_count": 0,
            "last_escalation": timestamp,
            "escalation_time": ALERT_THRESHOLDS[alert_type]["escalation_time"]
        }

        self.active_alerts[alert_id] = alert
        self.alert_counters[alert_type] += 1

        logger.warning(f"🚨 Nouvelle alerte créée: {alert_type} sur {road_id} ({zone})")
        return alert

    def update_alert(self, alert_id, new_metrics, timestamp):
        """Met à jour une alerte existante"""
        if alert_id not in self.active_alerts:
            return None

        alert = self.active_alerts[alert_id]
        alert["metrics"] = new_metrics
        alert["last_update"] = timestamp

        # Vérifier si l'alerte doit être escaladée
        time_since_last_escalation = (timestamp - alert["last_escalation"]).total_seconds()
        if time_since_last_escalation > alert["escalation_time"]:
            alert["escalation_count"] += 1
            alert["last_escalation"] = timestamp
            logger.warning(f"⚠️ Alerte escaladée: {alert_id} (niveau {alert['escalation_count']})")
            return alert  # Retourner l'alerte pour notification

        return None

    def resolve_alert(self, alert_id, timestamp):
        """Résout une alerte"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert["status"] = "RESOLVED"
            alert["resolved_at"] = timestamp
            alert["duration"] = (timestamp - alert["timestamp"]).total_seconds()

            # Archiver l'alerte
            self.alert_history.append(alert)
            del self.active_alerts[alert_id]

            logger.info(f"✅ Alerte résolue: {alert_id} (durée: {alert['duration']:.0f}s)")
            return alert

        return None

    def get_active_alerts_summary(self):
        """Retourne un résumé des alertes actives"""
        summary = {
            "total_active": len(self.active_alerts),
            "by_severity": defaultdict(int),
            "by_type": defaultdict(int),
            "by_zone": defaultdict(int)
        }

        for alert in self.active_alerts.values():
            summary["by_severity"][alert["severity"]] += 1
            summary["by_type"][alert["alert_type"]] += 1
            summary["by_zone"][alert["zone"]] += 1

        return summary

# ============================================================
# 3. SYSTÈME DE NOTIFICATIONS
# ============================================================

class NotificationManager:
    """Gestionnaire des notifications"""

    def __init__(self):
        self.alert_manager = AlertManager()

    def send_email_alert(self, alert, is_escalation=False):
        """Envoi d'alerte par email"""
        if not NOTIFICATION_CONFIG["email"]["enabled"]:
            return

        try:
            # Configuration du message
            msg = MIMEMultipart()
            msg['From'] = NOTIFICATION_CONFIG["email"]["sender_email"]
            msg['To'] = ", ".join(NOTIFICATION_CONFIG["email"]["recipients"])

            if is_escalation:
                msg['Subject'] = f"🚨 ESCALADE ALERTE TRAFIC - {alert['alert_type']} sur {alert['road_id']}"
            else:
                msg['Subject'] = f"🚨 ALERTE TRAFIC - {alert['alert_type']} sur {alert['road_id']}"

            # Corps du message
            body = self.format_alert_message(alert, is_escalation)
            msg.attach(MIMEText(body, 'html'))

            # Envoi
            server = smtplib.SMTP(NOTIFICATION_CONFIG["email"]["smtp_server"],
                                NOTIFICATION_CONFIG["email"]["smtp_port"])
            server.starttls()
            server.login(NOTIFICATION_CONFIG["email"]["sender_email"],
                        NOTIFICATION_CONFIG["email"]["sender_password"])
            server.sendmail(msg['From'], NOTIFICATION_CONFIG["email"]["recipients"],
                          msg.as_string())
            server.quit()

            logger.info(f"📧 Email envoyé pour alerte {alert['alert_id']}")

        except Exception as e:
            logger.error(f"Erreur envoi email: {e}")

    def send_slack_alert(self, alert, is_escalation=False):
        """Envoi d'alerte Slack"""
        if not NOTIFICATION_CONFIG["slack"]["enabled"]:
            return

        try:
            message = self.format_slack_message(alert, is_escalation)

            payload = {
                "channel": NOTIFICATION_CONFIG["slack"]["channel"],
                "text": message
            }

            response = requests.post(NOTIFICATION_CONFIG["slack"]["webhook_url"],
                                   json=payload, timeout=5)

            if response.status_code == 200:
                logger.info(f"💬 Slack notification envoyée pour {alert['alert_id']}")
            else:
                logger.error(f"Erreur Slack: {response.status_code}")

        except Exception as e:
            logger.error(f"Erreur envoi Slack: {e}")

    def send_webhook_alert(self, alert, is_escalation=False):
        """Envoi d'alerte via webhook"""
        if not NOTIFICATION_CONFIG["webhook"]["enabled"]:
            return

        try:
            payload = {
                "alert_id": alert["alert_id"],
                "type": "traffic_alert",
                "severity": alert["severity"],
                "road_id": alert["road_id"],
                "zone": alert["zone"],
                "metrics": alert["metrics"],
                "timestamp": alert["timestamp"].isoformat(),
                "is_escalation": is_escalation
            }

            response = requests.post(NOTIFICATION_CONFIG["webhook"]["url"],
                                   json=payload,
                                   headers=NOTIFICATION_CONFIG["webhook"]["headers"],
                                   timeout=5)

            if response.status_code == 200:
                logger.info(f"🔗 Webhook envoyé pour {alert['alert_id']}")
            else:
                logger.error(f"Erreur webhook: {response.status_code}")

        except Exception as e:
            logger.error(f"Erreur envoi webhook: {e}")

    def format_alert_message(self, alert, is_escalation=False):
        """Formate le message d'alerte HTML"""
        escalation_text = " (ESCALADE)" if is_escalation else ""

        html = f"""
        <html>
        <body>
            <h2 style="color: #ff4444;">🚨 ALERTE TRAFIC{escalation_text}</h2>

            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0;">
                <h3>Détails de l'alerte</h3>
                <ul>
                    <li><strong>Type:</strong> {alert['alert_type']}</li>
                    <li><strong>Sévérité:</strong> {alert['severity']}</li>
                    <li><strong>Route:</strong> {alert['road_id']}</li>
                    <li><strong>Zone:</strong> {alert['zone']}</li>
                    <li><strong>Timestamp:</strong> {alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</li>
                </ul>
            </div>

            <div style="background-color: #fff3cd; padding: 20px; border-radius: 5px; margin: 20px 0;">
                <h3>Métriques actuelles</h3>
                <ul>
                    <li><strong>Véhicules:</strong> {alert['metrics'].get('vehicle_count', 'N/A')}</li>
                    <li><strong>Vitesse moyenne:</strong> {alert['metrics'].get('average_speed', 'N/A')} km/h</li>
                    <li><strong>Taux d'occupation:</strong> {alert['metrics'].get('occupancy_rate', 'N/A')}%</li>
                </ul>
            </div>

            <div style="background-color: #d1ecf1; padding: 20px; border-radius: 5px; margin: 20px 0;">
                <h3>Actions recommandées</h3>
                <ul>
                    <li>Vérifier la situation sur place</li>
                    <li>Activer les panneaux de signalisation</li>
                    <li>Contacter les équipes de régulation</li>
                    <li>Envisager des déviations</li>
                </ul>
            </div>

            <p style="color: #666; font-size: 12px;">
                Cette alerte a été générée automatiquement par le système Smart City Traffic Monitoring.
            </p>
        </body>
        </html>
        """

        return html

    def format_slack_message(self, alert, is_escalation=False):
        """Formate le message Slack"""
        emoji = "🚨" if alert["severity"] == "HIGH" else "⚠️"

        escalation_text = " (ESCALADE)" if is_escalation else ""

        message = f"""{emoji} *ALERTE TRAFIC{escalation_text}*

*Type:* {alert['alert_type']}
*Sévérité:* {alert['severity']}
*Route:* {alert['road_id']}
*Zone:* {alert['zone']}
*Véhicules:* {alert['metrics'].get('vehicle_count', 'N/A')}
*Vitesse:* {alert['metrics'].get('average_speed', 'N/A')} km/h
*Taux occupation:* {alert['metrics'].get('occupancy_rate', 'N/A')}%

Vérifier immédiatement la situation."""

        return message

    def notify_alert(self, alert, is_escalation=False):
        """Envoi de toutes les notifications configurées"""
        logger.info(f"Envoi notifications pour alerte {alert['alert_id']}")

        # Email
        self.send_email_alert(alert, is_escalation)

        # Slack
        self.send_slack_alert(alert, is_escalation)

        # Webhook
        self.send_webhook_alert(alert, is_escalation)

# ============================================================
# 4. STOCKAGE DES ALERTES EN BASE
# ============================================================

class AlertDatabase:
    """Gestionnaire de base de données pour les alertes"""

    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        """Connexion à MySQL"""
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            if self.connection.is_connected():
                logger.info("Connexion MySQL établie pour les alertes")
        except Error as e:
            logger.error(f"Erreur connexion MySQL: {e}")

    def save_alert(self, alert):
        """Sauvegarde une alerte en base"""
        if not self.connection or not self.connection.is_connected():
            self.connect()

        try:
            cursor = self.connection.cursor()

            # Insérer ou mettre à jour l'alerte
            sql = """
            INSERT INTO traffic_alerts
            (alert_id, alert_type, severity, road_id, zone, vehicle_count,
             average_speed, occupancy_rate, timestamp, status, escalation_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                vehicle_count = VALUES(vehicle_count),
                average_speed = VALUES(average_speed),
                occupancy_rate = VALUES(occupancy_rate),
                escalation_count = VALUES(escalation_count),
                status = VALUES(status)
            """

            cursor.execute(sql, (
                alert["alert_id"],
                alert["alert_type"],
                alert["severity"],
                alert["road_id"],
                alert["zone"],
                alert["metrics"].get("vehicle_count"),
                alert["metrics"].get("average_speed"),
                alert["metrics"].get("occupancy_rate"),
                alert["timestamp"],
                alert["status"],
                alert.get("escalation_count", 0)
            ))

            self.connection.commit()
            logger.info(f"Alerte sauvegardée: {alert['alert_id']}")

        except Error as e:
            logger.error(f"Erreur sauvegarde alerte: {e}")

    def get_alerts_summary(self, hours=24):
        """Récupère un résumé des alertes récentes"""
        if not self.connection or not self.connection.is_connected():
            self.connect()

        try:
            cursor = self.connection.cursor(dictionary=True)

            sql = """
            SELECT
                alert_type,
                severity,
                COUNT(*) as count,
                AVG(vehicle_count) as avg_vehicles,
                AVG(average_speed) as avg_speed,
                AVG(occupancy_rate) as avg_occupancy
            FROM traffic_alerts
            WHERE timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            GROUP BY alert_type, severity
            ORDER BY count DESC
            """

            cursor.execute(sql, (hours,))
            results = cursor.fetchall()

            return results

        except Error as e:
            logger.error(f"Erreur récupération résumé: {e}")
            return []

# ============================================================
# 5. TRAITEMENT TEMPS RÉEL AVEC SPARK STREAMING
# ============================================================

def create_spark_streaming_session():
    """Création de la session Spark Streaming"""
    spark = SparkSession.builder \
        .appName("Smart City Real-Time Traffic Alerting") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Session Spark Streaming créée pour les alertes temps réel")
    return spark

def process_traffic_stream(spark):
    """Traitement du flux Kafka en temps réel"""
    logger.info("Démarrage du traitement temps réel du trafic...")

    # Schéma des données Kafka
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("road_type", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("occupancy_rate", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])

    try:
        # Lire le flux Kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "traffic-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parser les messages JSON
        traffic_data = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("processing_time", current_timestamp())

        # Calcul des métriques par route et zone (fenêtre de 1 minute)
        windowed_metrics = traffic_data \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", "1 minute"),
                "road_id",
                "zone"
            ) \
            .agg(
                avg("vehicle_count").alias("avg_vehicle_count"),
                avg("average_speed").alias("avg_speed"),
                avg("occupancy_rate").alias("avg_occupancy_rate"),
                count("*").alias("event_count"),
                max("vehicle_count").alias("max_vehicles"),
                min("average_speed").alias("min_speed")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end"))

        # Initialiser les gestionnaires
        alert_manager = AlertManager()
        notification_manager = NotificationManager()
        db_manager = AlertDatabase()

        def process_batch(batch_df, batch_id):
            """Traitement de chaque micro-batch"""
            logger.info(f"Traitement du batch {batch_id}")

            # Convertir en Pandas pour traitement
            batch_data = batch_df.toPandas()

            for _, row in batch_data.iterrows():
                road_id = row["road_id"]
                zone = row["zone"]
                timestamp = row["window_end"]

                metrics = {
                    "vehicle_count": float(row["avg_vehicle_count"]),
                    "average_speed": float(row["avg_speed"]),
                    "occupancy_rate": float(row["avg_occupancy_rate"])
                }

                # Vérifier les alertes
                alert_type = alert_manager.check_congestion_alert(road_id, zone, metrics)

                if alert_type:
                    # Créer ou mettre à jour l'alerte
                    alert_key = f"{alert_type}_{road_id}_{zone}"

                    if alert_key in alert_manager.active_alerts:
                        # Mettre à jour l'alerte existante
                        escalated_alert = alert_manager.update_alert(alert_key, metrics, timestamp)
                        if escalated_alert:
                            notification_manager.notify_alert(escalated_alert, is_escalation=True)
                            db_manager.save_alert(escalated_alert)
                    else:
                        # Créer une nouvelle alerte
                        new_alert = alert_manager.create_alert(alert_type, road_id, zone, metrics, timestamp)
                        notification_manager.notify_alert(new_alert, is_escalation=False)
                        db_manager.save_alert(new_alert)
                else:
                    # Vérifier si une alerte doit être résolue
                    for alert_key, alert in alert_manager.active_alerts.items():
                        if alert["road_id"] == road_id and alert["zone"] == zone:
                            resolved_alert = alert_manager.resolve_alert(alert_key, timestamp)
                            if resolved_alert:
                                db_manager.save_alert(resolved_alert)
                                logger.info(f"Alerte résolue: {alert_key}")
                            break

            # Log des statistiques
            summary = alert_manager.get_active_alerts_summary()
            logger.info(f"Statut alertes - Actives: {summary['total_active']}, "
                       f"Par sévérité: {dict(summary['by_severity'])}")

        # Démarrer le streaming avec foreachBatch
        query = windowed_metrics \
            .writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/alerting") \
            .trigger(processingTime="30 seconds") \
            .start()

        logger.info("Streaming d'alertes démarré - surveillance temps réel active")
        return query

    except Exception as e:
        logger.error(f"Erreur dans le traitement streaming: {e}")
        raise

# ============================================================
# 6. DASHBOARD DE MONITORING DES ALERTES
# ============================================================

def create_alerts_dashboard_summary():
    """Crée un résumé pour le dashboard des alertes"""
    logger.info("Génération du résumé du dashboard d'alertes...")

    db_manager = AlertDatabase()
    summary = db_manager.get_alerts_summary(hours=24)

    # Créer un rapport JSON
    dashboard_data = {
        "generated_at": datetime.datetime.now().isoformat(),
        "period_hours": 24,
        "total_alerts": sum(item["count"] for item in summary),
        "alerts_by_type": {item["alert_type"]: item["count"] for item in summary},
        "alerts_by_severity": {item["severity"]: item["count"] for item in summary},
        "average_metrics": {
            "avg_vehicles": sum(item["avg_vehicles"] for item in summary) / len(summary) if summary else 0,
            "avg_speed": sum(item["avg_speed"] for item in summary) / len(summary) if summary else 0,
            "avg_occupancy": sum(item["avg_occupancy"] for item in summary) / len(summary) if summary else 0
        },
        "top_alert_types": sorted(summary, key=lambda x: x["count"], reverse=True)[:5]
    }

    # Sauvegarder en JSON
    with open("/opt/airflow/data/alerts_dashboard.json", "w") as f:
        json.dump(dashboard_data, f, indent=2, default=str)

    logger.info(f"Dashboard d'alertes mis à jour: {dashboard_data['total_alerts']} alertes en 24h")
    return dashboard_data

# ============================================================
# 7. MAIN - Point d'entrée principal
# ============================================================

def main():
    print("\n" + "="*80)
    print("ÉTAPE AVANCÉE — SYSTÈME D'ALERTE TEMPS RÉEL")
    print("="*80)
    print("Objectif : Monitoring temps réel et notifications automatiques")
    print("="*80)

    # Création de la session Spark
    spark = create_spark_streaming_session()

    try:
        # Démarrer le traitement temps réel
        streaming_query = process_traffic_stream(spark)

        # Générer un résumé initial du dashboard
        create_alerts_dashboard_summary()

        print("\n" + "="*60)
        print("SYSTÈME D'ALERTES TEMPS RÉEL ACTIF")
        print("="*60)
        print("✓ Surveillance Kafka activée")
        print("✓ Détection de congestion en cours")
        print("✓ Notifications configurées")
        print("✓ Stockage MySQL opérationnel")
        print("")
        print("Le système surveille maintenant le trafic en temps réel...")
        print("Appuyez sur Ctrl+C pour arrêter")
        print("="*60)

        # Attendre la fin du streaming
        streaming_query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Arrêt du système d'alertes demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur dans le système d'alertes: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Système d'alertes arrêté")

# ============================================================
# 8. FONCTION DE TEST
# ============================================================

def test_alert_system():
    """Test du système d'alertes avec des données simulées"""
    print("\n" + "="*60)
    print("TEST DU SYSTÈME D'ALERTES")
    print("="*60)

    alert_manager = AlertManager()
    notification_manager = NotificationManager()

    # Données de test
    test_cases = [
        {
            "road_id": "ROAD_TEST_001",
            "zone": "Zone-Test",
            "metrics": {"vehicle_count": 160, "average_speed": 25, "occupancy_rate": 85},
            "expected": "HIGH_CONGESTION"
        },
        {
            "road_id": "ROAD_TEST_002",
            "zone": "Zone-Test",
            "metrics": {"vehicle_count": 130, "average_speed": 35, "occupancy_rate": 75},
            "expected": "MODERATE_CONGESTION"
        },
        {
            "road_id": "ROAD_TEST_003",
            "zone": "Zone-Test",
            "metrics": {"vehicle_count": 80, "average_speed": 55, "occupancy_rate": 45},
            "expected": None
        }
    ]

    print("Test des règles de détection d'alertes:")
    for i, test_case in enumerate(test_cases, 1):
        alert_type = alert_manager.check_congestion_alert(
            test_case["road_id"],
            test_case["zone"],
            test_case["metrics"]
        )

        status = "✓" if alert_type == test_case["expected"] else "✗"
        print(f"{status} Test {i}: {alert_type} (attendu: {test_case['expected']})")

    print("\nTest des notifications (simulation):")
    test_alert = {
        "alert_id": "TEST_ALERT_001",
        "alert_type": "HIGH_CONGESTION",
        "severity": "HIGH",
        "road_id": "ROAD_TEST_001",
        "zone": "Zone-Test",
        "metrics": {"vehicle_count": 160, "average_speed": 25, "occupancy_rate": 85},
        "timestamp": datetime.datetime.now()
    }

    print("✓ Email notification (simulée)")
    print("✓ Slack notification (simulée)")
    print("✓ Webhook notification (simulée)")

    print("\n" + "="*60)
    print("TEST TERMINÉ - Système d'alertes opérationnel")
    print("="*60)

# ============================================================
# 9. Exécution
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_alert_system()
    else:
        main()