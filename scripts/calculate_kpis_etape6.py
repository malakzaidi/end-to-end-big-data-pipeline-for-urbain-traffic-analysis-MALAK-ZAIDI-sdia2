"""
CALCUL DES KPI POUR L'ÉTAPE 6 - Exploitation et Visualisation
KPI à calculer:
1. Trafic par zone
2. Vitesse moyenne
3. Taux de congestion
4. Évolution du trafic
5. Zones critiques
"""

import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

def main():
    print("="*60)
    print("ÉTAPE 6 : CALCUL DES KPI DE MOBILITÉ URBAINE")
    print("="*60)
    
    # 1. Initialiser Spark (via spark-submit, la session existe déjà)
    spark = SparkSession.builder.appName("KPI-Calculator-Etape6").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("✓ Session Spark initialisée avec succès")
    
    # 2. Charger ou générer des données de test
    df = load_or_generate_data(spark)
    
    # 3. Calculer tous les KPI
    print("\n[1/5] Calcul du trafic par zone...")
    kpi_trafic_zone = calculate_traffic_by_zone(df)
    
    print("\n[2/5] Calcul de la vitesse moyenne...")
    kpi_vitesse = calculate_average_speed(df)
    
    print("\n[3/5] Calcul du taux de congestion...")
    kpi_congestion = calculate_congestion_rate(df)
    
    print("\n[4/5] Analyse de l'évolution temporelle...")
    kpi_evolution = calculate_temporal_evolution(df)
    
    print("\n[5/5] Identification des zones critiques...")
    kpi_zones_critiques = identify_critical_zones(kpi_trafic_zone, kpi_congestion)
    
    # 4. Sauvegarder dans HDFS
    save_to_hdfs(spark, {
        "kpi_trafic_zone": kpi_trafic_zone,
        "kpi_vitesse": kpi_vitesse,
        "kpi_congestion": kpi_congestion,
        "kpi_evolution": kpi_evolution,
        "kpi_zones_critiques": kpi_zones_critiques
    })
    
    # 5. Exporter vers MySQL pour Grafana
    export_to_mysql({
        "trafic_par_zone": kpi_trafic_zone,
        "vitesse_moyenne": kpi_vitesse,
        "taux_congestion": kpi_congestion,
        "evolution_trafic": kpi_evolution,
        "zones_critiques": kpi_zones_critiques
    })
    
    spark.stop()
    
    print("\n" + "="*60)
    print("ÉTAPE 6 TERMINÉE AVEC SUCCÈS !")
    print("="*60)
    print("\nAccès aux interfaces:")
    print("- Grafana: http://localhost:3000 (admin/admin)")
    print("- Superset: http://localhost:8088 (admin/admin)")
    print("- HDFS: http://localhost:9870")
    print("- Spark UI: http://localhost:8090")
    print("="*60)

def load_or_generate_data(spark):
    """Charge les données existantes ou génère des données de test"""
    try:
        # Essayer de lire depuis HDFS
        df = spark.read.parquet("hdfs://namenode:9000/data/traffic/test_data")
        print(f"✓ Données chargées depuis HDFS: {df.count()} enregistrements")
    except Exception as e:
        print(f"⚠ Impossible de charger depuis HDFS, génération de données de test...")
        df = generate_test_data(spark)
    
    return df

def generate_test_data(spark):
    """Génère des données de test réalistes pour le trafic urbain"""
    zones = ["Centre_Ville", "Quartier_Est", "Quartier_Ouest", 
             "Zone_Industrielle", "Zone_Residentielle", "Centre_Commercial",
             "Périphérie_Nord", "Périphérie_Sud"]
    
    data = []
    base_time = datetime.now() - timedelta(days=7)
    
    print("Génération de 10,000 points de données...")
    for i in range(10000):
        timestamp = base_time + timedelta(minutes=i*10)
        zone = random.choice(zones)
        hour = timestamp.hour
        
        # Modèle de trafic réaliste
        if zone == "Centre_Ville":
            base_traffic = 200
            if 8 <= hour <= 9 or 17 <= hour <= 18:
                traffic_multiplier = 3.0
            elif 12 <= hour <= 14:
                traffic_multiplier = 1.5
            else:
                traffic_multiplier = 1.0
        elif zone == "Centre_Commercial":
            base_traffic = 150
            if 10 <= hour <= 12 or 15 <= hour <= 19:
                traffic_multiplier = 2.5
            else:
                traffic_multiplier = 0.5
        else:
            base_traffic = 100
            traffic_multiplier = 1.0
        
        vehicle_count = int(base_traffic * traffic_multiplier * random.uniform(0.8, 1.2))
        speed = float(builtins.max(10, builtins.min(90, 70 - (vehicle_count / 5))))
        occupancy = float(builtins.min(1.0, vehicle_count / 250))
        
        if speed >= 60:
            service_level = "A"
        elif speed >= 50:
            service_level = "B"
        elif speed >= 40:
            service_level = "C"
        elif speed >= 30:
            service_level = "D"
        elif speed >= 20:
            service_level = "E"
        else:
            service_level = "F"
        
        data.append({
            "timestamp": timestamp,
            "zone": zone,
            "vehicle_count": vehicle_count,
            "speed": speed,
            "occupancy": occupancy,
            "service_level": service_level,
            "sensor_id": f"sensor_{random.randint(1, 100)}",
            "road_type": random.choice(["autoroute", "boulevard", "avenue", "rue"])
        })
    
    df = spark.createDataFrame(data)
    
    # Sauvegarder dans HDFS
    try:
        df.write.mode("overwrite").parquet("hdfs://namenode:9000/data/traffic/test_data")
        print("✓ Données sauvegardées dans HDFS")
    except Exception as e:
        print(f"⚠ Impossible de sauvegarder dans HDFS: {e}")
    
    print(f"✓ Données de test générées: {df.count()} enregistrements")
    return df

def calculate_traffic_by_zone(df):
    """Calcule le trafic moyen par zone"""
    result = df.groupBy("zone") \
        .agg(
            count("*").alias("nombre_mesures"),
            avg("vehicle_count").alias("trafic_moyen_vehicules_heure"),
            sum("vehicle_count").alias("trafic_total"),
            max("vehicle_count").alias("pic_trafic"),
            stddev("vehicle_count").alias("variabilite_trafic")
        ) \
        .withColumn("niveau_trafic",
                   when(col("trafic_moyen_vehicules_heure") > 200, "Élevé")
                   .when(col("trafic_moyen_vehicules_heure") > 120, "Moyen")
                   .otherwise("Faible")) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("trafic_moyen_vehicules_heure").desc())
    
    print(f"✓ Trafic calculé pour {result.count()} zones")
    result.show(truncate=False)
    return result

def calculate_average_speed(df):
    """Calcule la vitesse moyenne par zone"""
    result = df.groupBy("zone") \
        .agg(
            avg("speed").alias("vitesse_moyenne_kmh"),
            min("speed").alias("vitesse_minimale"),
            max("speed").alias("vitesse_maximale"),
            stddev("speed").alias("ecart_type_vitesse")
        ) \
        .withColumn("niveau_fluidite",
                   when(col("vitesse_moyenne_kmh") >= 60, "Excellente")
                   .when(col("vitesse_moyenne_kmh") >= 50, "Bonne")
                   .when(col("vitesse_moyenne_kmh") >= 40, "Moyenne")
                   .when(col("vitesse_moyenne_kmh") >= 30, "Faible")
                   .otherwise("Mauvaise")) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy("vitesse_moyenne_kmh")
    
    print(f"✓ Vitesse calculée pour {result.count()} zones")
    result.show(truncate=False)
    return result

def calculate_congestion_rate(df):
    """Calcule le taux de congestion par zone"""
    result = df.groupBy("zone") \
        .agg(
            count("*").alias("total_mesures"),
            sum(when(col("speed") < 30, 1).otherwise(0)).alias("mesures_congestionnees"),
            avg("occupancy").alias("taux_occupation_moyen"),
            avg("speed").alias("vitesse_moyenne")
        ) \
        .withColumn("taux_congestion_pourcentage",
                   (col("mesures_congestionnees") / col("total_mesures")) * 100) \
        .withColumn("niveau_congestion",
                   when(col("taux_congestion_pourcentage") >= 70, "Critique")
                   .when(col("taux_congestion_pourcentage") >= 50, "Élevé")
                   .when(col("taux_congestion_pourcentage") >= 30, "Modéré")
                   .otherwise("Faible")) \
        .withColumn("priorite_intervention",
                   when(col("taux_congestion_pourcentage") >= 70, 1)
                   .when(col("taux_congestion_pourcentage") >= 50, 2)
                   .when(col("taux_congestion_pourcentage") >= 30, 3)
                   .otherwise(4)) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("taux_congestion_pourcentage").desc())
    
    print(f"✓ Congestion calculée pour {result.count()} zones")
    result.show(truncate=False)
    return result

def calculate_temporal_evolution(df):
    """Analyse l'évolution du trafic dans le temps"""
    result = df.withColumn("heure", hour("timestamp")) \
        .withColumn("jour_semaine", date_format("timestamp", "EEEE")) \
        .groupBy("heure", "jour_semaine") \
        .agg(
            avg("vehicle_count").alias("trafic_horaire_moyen"),
            avg("speed").alias("vitesse_horaire_moyenne"),
            avg("occupancy").alias("occupation_horaire_moyenne"),
            count("*").alias("nombre_mesures")
        ) \
        .withColumn("periode_journee",
                   when((col("heure") >= 7) & (col("heure") <= 9), "Heure de pointe matin")
                   .when((col("heure") >= 16) & (col("heure") <= 19), "Heure de pointe soir")
                   .when((col("heure") >= 12) & (col("heure") <= 14), "Pause déjeuner")
                   .otherwise("Heure normale")) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy("heure")
    
    print(f"✓ Évolution temporelle calculée: {result.count()} périodes")
    return result

def identify_critical_zones(trafic_df, congestion_df):
    """Identifie les zones critiques basées sur trafic et congestion"""
    result = trafic_df.alias("t") \
        .join(congestion_df.alias("c"), "zone") \
        .select(
            col("t.zone"),
            col("t.trafic_moyen_vehicules_heure"),
            col("t.niveau_trafic"),
            col("c.taux_congestion_pourcentage"),
            col("c.niveau_congestion"),
            col("c.priorite_intervention"),
            when(
                (col("t.niveau_trafic") == "Élevé") & 
                (col("c.niveau_congestion").isin(["Critique", "Élevé"])),
                "Zone Critique - Action Immédiate"
            ).when(
                (col("t.niveau_trafic") == "Élevé") | 
                (col("c.niveau_congestion").isin(["Critique", "Élevé"])),
                "Zone à Surveiller"
            ).otherwise("Zone Normale").alias("statut_zone"),
            current_timestamp().alias("processed_at")
        ) \
        .orderBy(col("priorite_intervention").asc(), 
                col("trafic_moyen_vehicules_heure").desc())
    
    print(f"✓ Zones critiques identifiées: {result.count()} zones")
    result.show(truncate=False)
    return result

def save_to_hdfs(spark, dataframes):
    """Sauvegarde tous les DataFrames dans HDFS"""
    print("\nSauvegarde des KPI dans HDFS...")
    
    for name, df in dataframes.items():
        try:
            path = f"hdfs://namenode:9000/data/kpi/{name}"
            df.write.mode("overwrite").parquet(path)
            print(f"✓ {name} sauvegardé dans {path}")
        except Exception as e:
            print(f"⚠ Erreur: {e}")

def export_to_mysql(dataframes):
    """Exporte les KPI vers MySQL pour Grafana"""
    print("\nExport des KPI vers MySQL...")
    
    try:
        import mysql.connector
        from mysql.connector import Error
        
        # Créer d'abord les tables
        create_mysql_tables()
        
        conn = mysql.connector.connect(
            host="mysql-superset",
            user="superset",
            password="superset",
            database="superset",
            connect_timeout=10
        )
        
        for table_name, df in dataframes.items():
            try:
                pandas_df = df.toPandas()
                cursor = conn.cursor()
                
                for _, row in pandas_df.iterrows():
                    columns = ", ".join([f"`{col}`" for col in pandas_df.columns])
                    placeholders = ", ".join(["%s"] * len(pandas_df.columns))
                    values = tuple(row)
                    
                    sql = f"REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, values)
                
                conn.commit()
                cursor.close()
                print(f"✓ {table_name} exporté vers MySQL ({len(pandas_df)} lignes)")
            except Exception as e:
                print(f"⚠ Erreur {table_name}: {e}")
        
        conn.close()
            
    except Exception as e:
        print(f"⚠ MySQL non disponible: {e}")

def create_mysql_tables():
    """Crée les tables nécessaires dans MySQL"""
    import mysql.connector
    
    sql_statements = [
        """CREATE TABLE IF NOT EXISTS trafic_par_zone (
            zone VARCHAR(100) PRIMARY KEY,
            nombre_mesures INT,
            trafic_moyen_vehicules_heure DECIMAL(10,2),
            trafic_total BIGINT,
            pic_trafic INT,
            variabilite_trafic DECIMAL(10,2),
            niveau_trafic VARCHAR(20),
            processed_at TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS vitesse_moyenne (
            zone VARCHAR(100) PRIMARY KEY,
            vitesse_moyenne_kmh DECIMAL(10,2),
            vitesse_minimale DECIMAL(10,2),
            vitesse_maximale DECIMAL(10,2),
            ecart_type_vitesse DECIMAL(10,2),
            niveau_fluidite VARCHAR(20),
            processed_at TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS taux_congestion (
            zone VARCHAR(100) PRIMARY KEY,
            total_mesures INT,
            mesures_congestionnees INT,
            taux_occupation_moyen DECIMAL(10,2),
            vitesse_moyenne DECIMAL(10,2),
            taux_congestion_pourcentage DECIMAL(10,2),
            niveau_congestion VARCHAR(20),
            priorite_intervention INT,
            processed_at TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS evolution_trafic (
            heure INT,
            jour_semaine VARCHAR(20),
            trafic_horaire_moyen DECIMAL(10,2),
            vitesse_horaire_moyenne DECIMAL(10,2),
            occupation_horaire_moyenne DECIMAL(10,2),
            nombre_mesures INT,
            periode_journee VARCHAR(50),
            processed_at TIMESTAMP,
            PRIMARY KEY (heure, jour_semaine)
        )""",
        """CREATE TABLE IF NOT EXISTS zones_critiques (
            zone VARCHAR(100) PRIMARY KEY,
            trafic_moyen_vehicules_heure DECIMAL(10,2),
            niveau_trafic VARCHAR(20),
            taux_congestion_pourcentage DECIMAL(10,2),
            niveau_congestion VARCHAR(20),
            priorite_intervention INT,
            statut_zone VARCHAR(50),
            processed_at TIMESTAMP
        )"""
    ]
    
    try:
        conn = mysql.connector.connect(
            host="mysql-superset",
            user="superset",
            password="superset",
            database="superset",
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        for sql in sql_statements:
            cursor.execute(sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✓ Tables MySQL créées")
    except Exception as e:
        print(f"⚠ Tables MySQL: {e}")

if __name__ == "__main__":
    main()