#!/usr/bin/env python3
"""
Populate MySQL with CORRECT data for Grafana dashboards
Version corrigée pour correspondre aux tables utilisées dans Grafana
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime
import random

def create_tables():
    """Create the CORRECT tables that Grafana expects"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',  # Utiliser root au lieu de grafana
            password='root',
            port=3306
        )
        
        cursor = connection.cursor()
        
        # Créer la base de données
        cursor.execute("CREATE DATABASE IF NOT EXISTS traffic_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE traffic_db")
        
        print("✓ Database traffic_db created/verified")

        # Create tables that Grafana actually uses
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS trafic_par_zone (
                zone VARCHAR(50) PRIMARY KEY,
                nombre_mesures INT,
                trafic_moyen_vehicules_heure DECIMAL(10,2),
                trafic_total INT,
                pic_trafic INT,
                variabilite_trafic DECIMAL(10,2),
                niveau_trafic VARCHAR(20),
                processed_at DATETIME
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS zones_critiques (
                zone VARCHAR(50) PRIMARY KEY,
                niveau_congestion VARCHAR(20),
                processed_at DATETIME
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS taux_congestion (
                zone VARCHAR(50) PRIMARY KEY,
                taux_congestion_pourcentage DECIMAL(5,2),
                processed_at DATETIME
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS vitesse_moyenne (
                zone VARCHAR(50) PRIMARY KEY,
                vitesse_moyenne_kmh DECIMAL(5,2),
                processed_at DATETIME
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS evolution_trafic (
                id INT AUTO_INCREMENT PRIMARY KEY,
                zone VARCHAR(50),
                trafic_vehicules_heure DECIMAL(10,2),
                heure TIME,
                date DATE,
                processed_at DATETIME
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """
        ]

        for sql in tables_sql:
            try:
                cursor.execute(sql)
                print(f"✓ Table created/verified")
            except Error as e:
                print(f"✗ Error creating table: {e}")

        connection.commit()

    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def populate_sample_data():
    """Populate MySQL with sample data that matches Grafana queries"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='traffic_db',
            user='root',
            password='root',
            port=3306,
            charset='utf8mb4'  # Important pour l'encodage
        )
        
        cursor = connection.cursor()
        cursor.execute("SET NAMES utf8mb4")  # Forcer l'encodage UTF-8

        # Zones that match your Grafana queries
        zones = [
            "Centre_Ville",
            "Centre_Commercial", 
            "Périphérie_Nord",
            "Périphérie_Sud",
            "Quartier_Résidentiel",
            "Quartier_Est",
            "Zone_Industrielle",
            "Zone_Commerciale"
        ]

        processed_at = datetime.now()

        print("Populating sample data for Grafana...")

        # 1. Populate trafic_par_zone (table used in Grafana)
        print("\n📊 Populating trafic_par_zone...")
        for zone in zones:
            if zone == "Centre_Ville":
                trafic = random.uniform(250, 300)
                niveau = "Élevé"
            elif zone == "Centre_Commercial":
                trafic = random.uniform(170, 180)
                niveau = "Moyen"
            else:
                trafic = random.uniform(90, 110)
                niveau = "Faible"
            
            cursor.execute("""
                INSERT INTO trafic_par_zone 
                (zone, nombre_mesures, trafic_moyen_vehicules_heure, trafic_total, 
                 pic_trafic, variabilite_trafic, niveau_trafic, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                trafic_moyen_vehicules_heure = VALUES(trafic_moyen_vehicules_heure),
                niveau_trafic = VALUES(niveau_trafic),
                processed_at = VALUES(processed_at)
            """, (
                zone,
                random.randint(1200, 1400),
                round(trafic, 2),
                random.randint(120000, 150000),
                random.randint(100, 800),
                random.uniform(10, 150),
                niveau,
                processed_at
            ))
        print(f"✓ {len(zones)} zones in trafic_par_zone")

        # 2. Populate zones_critiques (table used in Grafana)
        print("\n🚨 Populating zones_critiques...")
        for zone in zones:
            if zone == "Centre_Ville":
                niveau = "Modéré"
            elif zone == "Centre_Commercial":
                niveau = "Élevé"
            else:
                niveau = "Faible"
            
            cursor.execute("""
                INSERT INTO zones_critiques (zone, niveau_congestion, processed_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                niveau_congestion = VALUES(niveau_congestion),
                processed_at = VALUES(processed_at)
            """, (zone, niveau, processed_at))
        print(f"✓ {len(zones)} zones in zones_critiques")

        # 3. Populate taux_congestion (table used in Grafana)
        print("\n🚦 Populating taux_congestion...")
        for zone in zones:
            if zone == "Centre_Ville":
                taux = random.uniform(40, 50)
            elif zone == "Centre_Commercial":
                taux = random.uniform(35, 40)
            else:
                taux = random.uniform(5, 15)
            
            cursor.execute("""
                INSERT INTO taux_congestion (zone, taux_congestion_pourcentage, processed_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                taux_congestion_pourcentage = VALUES(taux_congestion_pourcentage),
                processed_at = VALUES(processed_at)
            """, (zone, round(taux, 2), processed_at))
        print(f"✓ {len(zones)} zones in taux_congestion")

        # 4. Populate vitesse_moyenne (table used in Grafana)
        print("\n🚗 Populating vitesse_moyenne...")
        for zone in zones:
            if zone == "Centre_Ville":
                vitesse = random.uniform(20, 25)
            elif zone == "Centre_Commercial":
                vitesse = random.uniform(30, 35)
            else:
                vitesse = random.uniform(45, 65)
            
            cursor.execute("""
                INSERT INTO vitesse_moyenne (zone, vitesse_moyenne_kmh, processed_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                vitesse_moyenne_kmh = VALUES(vitesse_moyenne_kmh),
                processed_at = VALUES(processed_at)
            """, (zone, round(vitesse, 2), processed_at))
        print(f"✓ {len(zones)} zones in vitesse_moyenne")

        connection.commit()
        
        # Verification
        print("\n" + "="*50)
        print("✅ VERIFICATION:")
        
        cursor.execute("SELECT COUNT(*) FROM trafic_par_zone")
        print(f"• trafic_par_zone: {cursor.fetchone()[0]} records")
        
        cursor.execute("SELECT DISTINCT niveau_congestion FROM zones_critiques")
        niveaux = [row[0] for row in cursor.fetchall()]
        print(f"• Niveaux congestion: {', '.join(niveaux)}")
        
        cursor.execute("SELECT AVG(trafic_moyen_vehicules_heure) FROM trafic_par_zone")
        print(f"• Trafic moyen global: {cursor.fetchone()[0]:.1f} véhicules/h")
        
        cursor.execute("SELECT COUNT(*) FROM zones_critiques WHERE niveau_congestion != 'Faible'")
        print(f"• Zones critiques: {cursor.fetchone()[0]} zones")

    except Error as e:
        print(f"✗ Error populating data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def fix_encoding():
    """Fix encoding issues in existing data"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='traffic_db',
            user='root',
            password='root',
            port=3306
        )
        
        cursor = connection.cursor()
        
        print("\n🔧 Fixing encoding issues...")
        
        # Corriger les données existantes
        cursor.execute("""
            UPDATE zones_critiques 
            SET niveau_congestion = CASE
                WHEN niveau_congestion LIKE '%lev%' THEN 'Élevé'
                WHEN niveau_congestion LIKE '%Mod%' THEN 'Modéré'
                ELSE 'Faible'
            END
        """)
        
        connection.commit()
        print("✓ Encoding fixed")
        
    except Error as e:
        print(f"✗ Error fixing encoding: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    print("🚀 Populating MySQL with CORRECT data for Grafana dashboards...")
    print("="*60)
    
    # 1. Create correct tables
    create_tables()
    
    # 2. Populate with correct data
    populate_sample_data()
    
    # 3. Fix any encoding issues
    fix_encoding()
    
    print("="*60)
    print("✅ DONE! Your Grafana dashboards should now work correctly.")
    print("🌐 Access Grafana at: http://localhost:3000")
    print("👤 Login: admin / admin")
    print("\n📊 Expected data in Grafana:")
    print("   • Trafic Moyen Global: ~130-150 véhicules/h")
    print("   • Zones Critiques: 2 zones (Élevé + Modéré)")
    print("   • Congestion Moyenne: ~20-25%")
    print("   • Vitesse Moyenne Globale: ~45-50 km/h")
