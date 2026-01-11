#!/usr/bin/env python3
"""
Populate MySQL with sample data for Grafana dashboards
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import random

def create_tables():
    """Create necessary tables in MySQL"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='traffic_db',
            user='grafana',
            password='grafana',
            port=3306
        )

        cursor = connection.cursor()

        # Create tables
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS kpi_zone_strategique (
                zone VARCHAR(100),
                trafic_moyen_vehicules INT,
                vitesse_moyenne_kmh DECIMAL(10,2),
                taux_occupation_moyen DECIMAL(10,2),
                niveau_service VARCHAR(10),
                processed_at TIMESTAMP,
                PRIMARY KEY (zone, processed_at)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS analyse_congestion (
                zone VARCHAR(100),
                congestion_rate_percent DECIMAL(10,2),
                niveau_congestion VARCHAR(20),
                priorite_intervention INT,
                impact_estime_minutes INT,
                processed_at TIMESTAMP,
                PRIMARY KEY (zone, processed_at)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alertes_congestion (
                zone VARCHAR(100),
                congestion_rate_percent DECIMAL(10,2),
                niveau_congestion VARCHAR(20),
                priorite_intervention INT,
                severite VARCHAR(20),
                processed_at TIMESTAMP,
                PRIMARY KEY (zone, processed_at)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dashboard_principal (
                zone VARCHAR(100),
                trafic_moyen_vehicules INT,
                vitesse_moyenne_kmh DECIMAL(10,2),
                taux_occupation_moyen DECIMAL(10,2),
                niveau_service VARCHAR(10),
                congestion_rate_percent DECIMAL(10,2),
                niveau_congestion VARCHAR(20),
                priorite_intervention INT,
                impact_estime_minutes INT,
                processed_at TIMESTAMP,
                PRIMARY KEY (zone, processed_at)
            )
            """
        ]

        for sql in tables_sql:
            cursor.execute(sql)

        connection.commit()
        print("✓ Tables created successfully")

    except Error as e:
        print(f"✗ Error creating tables: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def populate_sample_data():
    """Populate MySQL with sample data"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='traffic_db',
            user='grafana',
            password='grafana',
            port=3306
        )

        cursor = connection.cursor()

        # Sample zones
        zones = [
            "Centre-Ville",
            "Zone-Industrielle",
            "Quartier-Residentiel",
            "Zone-Commerciale",
            "Peripherie-Nord",
            "Peripherie-Sud"
        ]

        processed_at = datetime.now()

        print("Populating sample data...")

        # Populate kpi_zone_strategique
        for zone in zones:
            if zone == "Centre-Ville":
                trafic = random.randint(400, 600)
                vitesse = random.uniform(15, 25)
                occupation = random.uniform(0.7, 0.9)
            elif zone == "Zone-Commerciale":
                trafic = random.randint(200, 350)
                vitesse = random.uniform(25, 35)
                occupation = random.uniform(0.5, 0.7)
            else:
                trafic = random.randint(80, 150)
                vitesse = random.uniform(35, 50)
                occupation = random.uniform(0.3, 0.5)

            niveau_service = "A" if vitesse > 40 else "B" if vitesse > 30 else "C" if vitesse > 20 else "D"

            cursor.execute("""
                INSERT INTO kpi_zone_strategique
                (zone, trafic_moyen_vehicules, vitesse_moyenne_kmh, taux_occupation_moyen, niveau_service, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                trafic_moyen_vehicules = VALUES(trafic_moyen_vehicules),
                vitesse_moyenne_kmh = VALUES(vitesse_moyenne_kmh),
                taux_occupation_moyen = VALUES(taux_occupation_moyen),
                niveau_service = VALUES(niveau_service)
            """, (zone, trafic, round(vitesse, 2), round(occupation, 2), niveau_service, processed_at))

        # Populate analyse_congestion
        for zone in zones:
            if zone == "Centre-Ville":
                congestion_rate = random.uniform(60, 80)
                niveau = "Critique"
                priorite = 1
                impact = random.randint(30, 60)
            elif zone == "Zone-Commerciale":
                congestion_rate = random.uniform(40, 60)
                niveau = "Élevé"
                priorite = 2
                impact = random.randint(15, 30)
            else:
                congestion_rate = random.uniform(10, 30)
                niveau = "Moyen" if random.random() > 0.5 else "Faible"
                priorite = 3 if niveau == "Moyen" else 4
                impact = random.randint(5, 15)

            cursor.execute("""
                INSERT INTO analyse_congestion
                (zone, congestion_rate_percent, niveau_congestion, priorite_intervention, impact_estime_minutes, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                congestion_rate_percent = VALUES(congestion_rate_percent),
                niveau_congestion = VALUES(niveau_congestion),
                priorite_intervention = VALUES(priorite_intervention),
                impact_estime_minutes = VALUES(impact_estime_minutes)
            """, (zone, round(congestion_rate, 2), niveau, priorite, impact, processed_at))

        # Populate alertes_congestion (subset with high congestion)
        critical_zones = [zone for zone in zones if random.random() > 0.6]
        for zone in critical_zones:
            congestion_rate = random.uniform(50, 85)
            if congestion_rate > 70:
                niveau = "Critique"
                priorite = 1
                severite = "CRITIQUE"
            elif congestion_rate > 50:
                niveau = "Élevé"
                priorite = 2
                severite = "ÉLEVÉ"
            else:
                niveau = "Moyen"
                priorite = 3
                severite = "MOYEN"

            cursor.execute("""
                INSERT INTO alertes_congestion
                (zone, congestion_rate_percent, niveau_congestion, priorite_intervention, severite, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                congestion_rate_percent = VALUES(congestion_rate_percent),
                niveau_congestion = VALUES(niveau_congestion),
                priorite_intervention = VALUES(priorite_intervention),
                severite = VALUES(severite)
            """, (zone, round(congestion_rate, 2), niveau, priorite, severite, processed_at))

        # Populate dashboard_principal
        for zone in zones:
            # Get data from other tables (simplified)
            trafic = random.randint(100, 500)
            vitesse = random.uniform(20, 50)
            occupation = random.uniform(0.3, 0.8)
            niveau_service = "A" if vitesse > 40 else "B" if vitesse > 30 else "C"
            congestion_rate = random.uniform(20, 70)
            niveau_congestion = "Critique" if congestion_rate > 60 else "Élevé" if congestion_rate > 40 else "Moyen"
            priorite = 1 if niveau_congestion == "Critique" else 2 if niveau_congestion == "Élevé" else 3
            impact = random.randint(10, 45)

            cursor.execute("""
                INSERT INTO dashboard_principal
                (zone, trafic_moyen_vehicules, vitesse_moyenne_kmh, taux_occupation_moyen, niveau_service,
                 congestion_rate_percent, niveau_congestion, priorite_intervention, impact_estime_minutes, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                trafic_moyen_vehicules = VALUES(trafic_moyen_vehicules),
                vitesse_moyenne_kmh = VALUES(vitesse_moyenne_kmh),
                taux_occupation_moyen = VALUES(taux_occupation_moyen),
                niveau_service = VALUES(niveau_service),
                congestion_rate_percent = VALUES(congestion_rate_percent),
                niveau_congestion = VALUES(niveau_congestion),
                priorite_intervention = VALUES(priorite_intervention),
                impact_estime_minutes = VALUES(impact_estime_minutes)
            """, (zone, trafic, round(vitesse, 2), round(occupation, 2), niveau_service,
                  round(congestion_rate, 2), niveau_congestion, priorite, impact, processed_at))

        connection.commit()
        print("✓ Sample data populated successfully")

        # Show what was inserted
        cursor.execute("SELECT COUNT(*) as total FROM kpi_zone_strategique")
        result = cursor.fetchone()
        print(f"✓ kpi_zone_strategique: {result[0]} records")

        cursor.execute("SELECT COUNT(*) as total FROM analyse_congestion")
        result = cursor.fetchone()
        print(f"✓ analyse_congestion: {result[0]} records")

        cursor.execute("SELECT COUNT(*) as total FROM alertes_congestion")
        result = cursor.fetchone()
        print(f"✓ alertes_congestion: {result[0]} records")

    except Error as e:
        print(f"✗ Error populating data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    print("🚀 Populating MySQL with sample data for Grafana dashboards...")
    create_tables()
    populate_sample_data()
    print("✅ Done! Now check your Grafana dashboards at http://localhost:3000")
