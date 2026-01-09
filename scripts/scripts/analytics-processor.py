#!/usr/bin/env python3
"""
Étape 5 — Structuration analytique (Analytics Zone)
Objectif : préparer les données pour l'analyse avancée
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max,
    min as spark_min, when, current_timestamp,
    hour, date_format, stddev, round,
    dense_rank, lit
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import datetime

# ============================================================
# 1. Création de la Spark Session
# ============================================================
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Smart City Analytics Zone") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark Analytics créée avec succès")
    return spark

# ============================================================
# 2. Chargement des données traitées
# ============================================================
def load_processed_data(spark):
    print("\n" + "="*60)
    print("CHARGEMENT DES DONNEES TRAITEES")
    print("="*60)
    
    PROCESSED_BASE = "hdfs://namenode:9000/data/processed/traffic"
    
    try:
        print("Chargement des données depuis /data/processed/traffic...")
        
        traffic_by_zone = spark.read.parquet(f"{PROCESSED_BASE}/traffic_by_zone")
        speed_by_road = spark.read.parquet(f"{PROCESSED_BASE}/speed_by_road")
        congestion_by_zone = spark.read.parquet(f"{PROCESSED_BASE}/congestion_by_zone")
        hourly_traffic = spark.read.parquet(f"{PROCESSED_BASE}/hourly_analysis")
        
        print(f"1. Traffic par zone : {traffic_by_zone.count()} lignes")
        print(f"2. Vitesse par route : {speed_by_road.count()} lignes")
        print(f"3. Congestion par zone : {congestion_by_zone.count()} lignes")
        print(f"4. Analyse horaire : {hourly_traffic.count()} lignes")
        
        return {
            "traffic_by_zone": traffic_by_zone,
            "speed_by_road": speed_by_road,
            "congestion_by_zone": congestion_by_zone,
            "hourly_traffic": hourly_traffic
        }
        
    except Exception as e:
        print(f"Erreur lors du chargement : {e}")
        print("Vérifiez que l'étape 4 a bien été exécutée.")
        return None

# ============================================================
# 3. Création des vues analytiques
# ============================================================
def create_analytical_views(spark, data_dict):
    print("\n" + "="*60)
    print("CREATION DES VUES ANALYTIQUES")
    print("="*60)
    
    traffic_by_zone = data_dict["traffic_by_zone"]
    speed_by_road = data_dict["speed_by_road"]
    congestion_by_zone = data_dict["congestion_by_zone"]
    hourly_traffic = data_dict["hourly_traffic"]
    
    # 1. KPI par zone
    print("\n1. Création : KPI stratégiques par zone")
    
    strategic_zone_kpi = traffic_by_zone.groupBy("zone").agg(
        round(avg("avg_vehicle_count"), 2).alias("trafic_moyen_vehicules"),
        round(avg("avg_speed"), 2).alias("vitesse_moyenne_kmh"),
        round(avg("avg_occupancy_rate"), 2).alias("taux_occupation_moyen"),
        spark_sum("total_vehicles").alias("total_vehicules_zone"),
        spark_sum("event_count").alias("nombre_mesures"),
        spark_max("max_vehicles").alias("pic_trafic"),
        spark_min("min_speed").alias("vitesse_minimale"),
        round(stddev("avg_vehicle_count"), 2).alias("variabilite_trafic")
    ).withColumn("efficacite_circulation", 
                when(col("taux_occupation_moyen") > 0,
                     round(col("vitesse_moyenne_kmh") / col("taux_occupation_moyen"), 2))
                .otherwise(lit(0.0))) \
     .withColumn("niveau_service",
                when(col("vitesse_moyenne_kmh") > 70, "A - Excellent")
                .when(col("vitesse_moyenne_kmh") > 50, "B - Bon")
                .when(col("vitesse_moyenne_kmh") > 30, "C - Moyen")
                .when(col("vitesse_moyenne_kmh") > 20, "D - Faible")
                .otherwise("E - Critique")) \
     .withColumn("processed_at", current_timestamp())
    
    print("Exemple de KPI stratégiques:")
    strategic_zone_kpi.show(5, truncate=False)
    
    # 2. Performance des routes
    print("\n2. Création : Performance opérationnelle des routes")
    
    window_spec = Window.partitionBy("road_type").orderBy(col("avg_speed").desc())
    
    road_performance = speed_by_road.withColumn("rank", dense_rank().over(window_spec)) \
        .withColumn("performance_score",
                   when(col("avg_speed") > 80, 100)
                   .when(col("avg_speed") > 60, 80)
                   .when(col("avg_speed") > 40, 60)
                   .when(col("avg_speed") > 30, 40)
                   .otherwise(20)) \
        .withColumn("status",
                   when(col("avg_speed") < 30, "CRITIQUE - Intervention requise")
                   .when(col("avg_speed") < 50, "ALERTE - Surveillance renforcée")
                   .otherwise("NORMAL - Bonnes conditions")) \
        .withColumn("processed_at", current_timestamp())
    
    print("Exemple de performance routes:")
    road_performance.select("road_id", "road_type", "avg_speed", "performance_score", "status").show(5, truncate=False)
    
    # 3. Analyse de congestion
    print("\n3. Création : Analyse de congestion détaillée")
    
    congestion_analysis = congestion_by_zone.withColumn(
        "niveau_congestion",
        when(col("congestion_rate_%") > 50, "Élevée ( > 50%)")
        .when(col("congestion_rate_%") > 30, "Modérée (30-50%)")
        .when(col("congestion_rate_%") > 15, "Légère (15-30%)")
        .otherwise("Fluide ( < 15%)")
    ).withColumn("priorite_intervention",
                when(col("congestion_rate_%") > 50, 1)
                .when(col("congestion_rate_%") > 30, 2)
                .when(col("congestion_rate_%") > 15, 3)
                .otherwise(4)) \
     .withColumn("impact_estime_minutes",
                round(col("congestion_rate_%") * 1.5, 2)) \
     .withColumn("processed_at", current_timestamp())
    
    print("Exemple d'analyse congestion:")
    congestion_analysis.show(5, truncate=False)
    
    # 4. Analyse temporelle
    print("\n4. Création : Analyse temporelle et tendances")
    
    hourly_analysis = hourly_traffic.groupBy("hour").agg(
        round(avg("avg_vehicles"), 2).alias("trafic_horaire_moyen"),
        round(avg("avg_speed"), 2).alias("vitesse_horaire_moyenne"),
        round(avg("avg_occupancy"), 2).alias("occupation_horaire_moyenne"),
        spark_sum("event_count").alias("total_mesures_heure")
    ).withColumn("periode_journee",
                when(col("hour").between(6, 9), "Heures de pointe matin")
                .when(col("hour").between(16, 19), "Heures de pointe soir")
                .when(col("hour").between(10, 15), "Pleine journée")
                .when(col("hour").between(20, 23), "Soirée")
                .otherwise("Nuit")) \
     .withColumn("ratio_occupation_vitesse",
                when(col("vitesse_horaire_moyenne") > 0,
                     round(col("occupation_horaire_moyenne") / col("vitesse_horaire_moyenne"), 3))
                .otherwise(lit(0.0))) \
     .withColumn("processed_at", current_timestamp())
    
    print("Exemple d'analyse temporelle:")
    hourly_analysis.orderBy("hour").show(24, truncate=False)
    
    # 5. Tableau de bord exécutif - CORRIGÉ
    print("\n5. Création : Tableau de bord exécutif")
    
    try:
        # Calculer les métriques globales
        avg_congestion_df = congestion_by_zone.select(avg("congestion_rate_%"))
        avg_congestion_row = avg_congestion_df.collect()[0]
        avg_congestion_result = avg_congestion_row[0] if avg_congestion_row[0] is not None else 0.0
        
        total_vehicles_df = traffic_by_zone.select(spark_sum("total_vehicles"))
        total_vehicles_row = total_vehicles_df.collect()[0]
        total_vehicles_result = total_vehicles_row[0] if total_vehicles_row[0] is not None else 0
        
        avg_speed_df = speed_by_road.select(avg("avg_speed"))
        avg_speed_row = avg_speed_df.collect()[0]
        avg_speed_result = avg_speed_row[0] if avg_speed_row[0] is not None else 0.0
        
        zones_count = congestion_by_zone.count()
        
        # Préparer les valeurs Python (pas des colonnes Spark)
        avg_congestion_val = round(float(avg_congestion_result), 2)
        avg_speed_val = round(float(avg_speed_result), 2)
        total_vehicles_val = int(total_vehicles_result)
        current_time = datetime.datetime.now()
        
        # Préparer les données pour le DataFrame
        executive_data = [
            ("Rapport Trafic Urbain", 
             avg_congestion_val,
             avg_speed_val,
             total_vehicles_val,
             zones_count,
             current_time)
        ]
        
        print(f"Métriques calculées:")
        print(f"  - Congestion moyenne: {avg_congestion_val}%")
        print(f"  - Vitesse moyenne: {avg_speed_val} km/h")
        print(f"  - Total véhicules: {total_vehicles_val}")
        print(f"  - Zones surveillées: {zones_count}")
        
    except Exception as e:
        print(f"Erreur dans le calcul des métriques: {e}")
        current_time = datetime.datetime.now()
        executive_data = [
            ("Rapport Trafic Urbain", 
             0.0,
             0.0,
             0,
             0,
             current_time)
        ]
    
    # Définir le schéma
    executive_schema = StructType([
        StructField("titre_rapport", StringType(), False),
        StructField("taux_congestion_moyen", DoubleType(), False),
        StructField("vitesse_moyenne_globale", DoubleType(), False),
        StructField("total_vehicules_jour", LongType(), False),
        StructField("zones_surveillees", LongType(), False),
        StructField("date_generation", TimestampType(), False)
    ])
    
    # Créer le DataFrame
    executive_dashboard = spark.createDataFrame(executive_data, executive_schema)
    
    print("\nTableau de bord exécutif:")
    executive_dashboard.show(truncate=False)
    
    return {
        "strategic_zone_kpi": strategic_zone_kpi,
        "road_performance": road_performance,
        "congestion_analysis": congestion_analysis,
        "hourly_analysis": hourly_analysis,
        "executive_dashboard": executive_dashboard
    }

# ============================================================
# 4. Sauvegarde dans la zone analytique
# ============================================================
def save_to_analytics_zone(spark, analytics_views):
    print("\n" + "="*60)
    print("SAUVEGARDE DANS LA ZONE ANALYTIQUE")
    print("="*60)
    
    ANALYTICS_BASE = "hdfs://namenode:9000/data/analytics/traffic"
    
    try:
        print(f"Création du répertoire analytique : {ANALYTICS_BASE}")
        
        print("\n1. Sauvegarde : KPI Stratégiques par zone")
        output_path = f"{ANALYTICS_BASE}/kpi_strategique_zone"
        analytics_views["strategic_zone_kpi"].write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        print(f"   Chemin : {output_path}")
        
        print("\n2. Sauvegarde : Performance des routes")
        output_path = f"{ANALYTICS_BASE}/performance_routes"
        analytics_views["road_performance"].write \
            .mode("overwrite") \
            .partitionBy("road_type") \
            .option("compression", "snappy") \
            .parquet(output_path)
        print(f"   Chemin : {output_path} (partitionné par road_type)")
        
        print("\n3. Sauvegarde : Analyse de congestion")
        output_path = f"{ANALYTICS_BASE}/analyse_congestion"
        analytics_views["congestion_analysis"].write \
            .mode("overwrite") \
            .partitionBy("niveau_congestion") \
            .option("compression", "snappy") \
            .parquet(output_path)
        print(f"   Chemin : {output_path} (partitionné par niveau_congestion)")
        
        print("\n4. Sauvegarde : Analyse temporelle")
        output_path = f"{ANALYTICS_BASE}/analyse_temporelle"
        analytics_views["hourly_analysis"].write \
            .mode("overwrite") \
            .partitionBy("periode_journee") \
            .option("compression", "snappy") \
            .parquet(output_path)
        print(f"   Chemin : {output_path} (partitionné par periode_journee)")
        
        print("\n5. Sauvegarde : Tableau de bord exécutif")
        output_path = f"{ANALYTICS_BASE}/tableau_bord_executif"
        analytics_views["executive_dashboard"].write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        print(f"   Chemin : {output_path}")
        
        print("\n" + "="*60)
        print("SAUVEGARDE TERMINEE AVEC SUCCES")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"Erreur lors de la sauvegarde : {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================
# 5. Vérification des fichiers
# ============================================================
def verify_analytics_files(spark):
    print("\n" + "="*60)
    print("VERIFICATION DES FICHIERS ANALYTIQUES")
    print("="*60)
    
    ANALYTICS_BASE = "hdfs://namenode:9000/data/analytics/traffic"
    
    datasets = [
        ("KPI Stratégiques Zone", "kpi_strategique_zone"),
        ("Performance Routes", "performance_routes"),
        ("Analyse Congestion", "analyse_congestion"),
        ("Analyse Temporelle", "analyse_temporelle"),
        ("Tableau Bord Exécutif", "tableau_bord_executif")
    ]
    
    print("\nDétail des fichiers créés dans HDFS:")
    print("-" * 70)
    
    for dataset_name, dataset_path in datasets:
        full_path = f"{ANALYTICS_BASE}/{dataset_path}"
        
        try:
            df = spark.read.parquet(full_path)
            row_count = df.count()
            col_count = len(df.columns)
            
            print(f"\n{dataset_name}:")
            print(f"   Chemin HDFS: {full_path}")
            print(f"   Nombre de lignes: {row_count}")
            print(f"   Nombre de colonnes: {col_count}")
            
            if row_count > 0 and row_count <= 5:
                print(f"   Données complètes:")
                df.show(row_count, truncate=False)
            elif row_count > 0:
                print(f"   Aperçu (3 premières lignes):")
                df.show(3, truncate=False)
            
        except Exception as e:
            print(f"\n{dataset_name}: ERREUR de lecture - {e}")
    
    print("\n" + "="*60)
    print("COMMANDES HDFS POUR VERIFICATION:")
    print("="*60)
    
    print("\n1. Liste des répertoires:")
    print("   hdfs dfs -ls /data/analytics/traffic/")
    
    print("\n2. Structure des partitions:")
    print("   hdfs dfs -ls -R /data/analytics/traffic/performance_routes/")
    
    print("\n3. Taille des données:")
    print("   hdfs dfs -du -h /data/analytics/traffic/")
    
    print("\n" + "="*60)
    print("VERIFICATION TERMINEE")
    print("="*60)
    
    return True

# ============================================================
# 6. Justification du format Parquet
# ============================================================
def justify_parquet_format():
    print("\n" + "="*60)
    print("JUSTIFICATION DU FORMAT PARQUET")
    print("="*60)
    
    justification = """
    JUSTIFICATION DU CHOIX DU FORMAT PARQUET :

    1. PERFORMANCE OPTIMALE :
       - Format colonnaire : lecture selective des colonnes seulement
       - Compression Snappy : reduction de 70-80% du stockage
       - Predicats push-down : filtrage au niveau du stockage

    2. COMPATIBILITE MAXIMALE :
       - Support natif par Spark, Hive, Impala, Presto
       - Compatible avec Tableau, PowerBI, Qlik
       - Schema auto-documente (metadonnees incluses)

    3. OPTIMISATION ANALYTIQUE :
       - Partitionnement intelligent (road_type, niveau_congestion, periode_journee)
       - Statistiques integrees (min/max par bloc)
       - Schema preserve avec types de donnees

    4. GOUVERNANCE DES DONNEES :
       - Schema fige et versionne
       - Metadonnees de provenance (processed_at)
       - Traçabilite complete des transformations

    STRUCTURE CREEE DANS /data/analytics/traffic/ :

    kpi_strategique_zone/      - Indicateurs globaux par zone
    performance_routes/        - Performance par type de route
        road_type=autoroute/   - Partition pour les autoroutes
        road_type=avenue/      - Partition pour les avenues  
        road_type=rue/         - Partition pour les rues
    analyse_congestion/        - Analyse de congestion
        niveau_congestion=Élevée/
        niveau_congestion=Modérée/
        niveau_congestion=Légère/
    analyse_temporelle/        - Tendances horaires
        periode_journee=Heures de pointe matin/
        periode_journee=Pleine journée/
        periode_journee=Soirée/
        periode_journee=Nuit/
    tableau_bord_executif/     - Vue executive

    AVANTAGES CONCRETS :
    - Temps de requete reduits de 10x vs CSV/JSON
    - Stockage 4x plus efficace
    - Maintenance simplifiee
    - Compatible avec tous les outils d'analyse
    """
    
    print(justification)

# ============================================================
# 7. Instructions pour vérifier dans l'interface web HDFS
# ============================================================
def show_hdfs_web_interface_instructions():
    print("\n" + "="*60)
    print("INSTRUCTIONS POUR L'INTERFACE WEB HDFS")
    print("="*60)
    
    instructions = """
    POUR VERIFIER DANS L'INTERFACE WEB HDFS :

    1. ACCES A L'INTERFACE :
       - Ouvrez votre navigateur
       - Allez à : http://localhost:9870
       - Ou : http://[IP-NAMENODE]:9870

    2. NAVIGATION :
       - Cliquez sur 'Utilities' dans le menu
       - Sélectionnez 'Browse the file system'
       - Dans le champ de chemin, entrez : /data/analytics/traffic/
       - Cliquez sur 'Go'

    3. VERIFICATION VISUELLE :
       - Vous devriez voir 5 dossiers principaux
       - Cliquez sur 'performance_routes/' pour voir les partitions
       - Cliquez sur 'analyse_congestion/' pour voir les niveaux
       - Les fichiers .parquet sont visibles dans chaque dossier

    4. TELECHARGEMENT (optionnel) :
       - Cliquez sur un fichier .parquet
       - Cliquez sur 'Download' pour l'analyser localement
       - Utilisez 'parquet-tools' pour inspecter le contenu

    5. METADONNEES :
       - L'interface affiche la taille, le nombre de blocs
       - Les permissions et dates de modification
       - Le chemin HDFS complet pour référence
    """
    
    print(instructions)

# ============================================================
# 8. MAIN - Point d'entrée principal
# ============================================================
def main():
    print("\n" + "="*80)
    print("ETAPE 5 — STRUCTURATION ANALYTIQUE (ANALYTICS ZONE)")
    print("="*80)
    print("Objectif : préparer les données pour l'analyse avancée")
    print("="*80)
    
    # 1. Création de la session Spark
    spark = create_spark_session()
    
    # 2. Chargement des données traitées
    processed_data = load_processed_data(spark)
    
    if not processed_data:
        print("ERREUR : Impossible de charger les données traitées.")
        print("Veuillez exécuter d'abord l'étape 4 (traffic_processor.py).")
        spark.stop()
        sys.exit(1)
    
    # 3. Création des vues analytiques
    analytics_views = create_analytical_views(spark, processed_data)
    
    # 4. Sauvegarde dans la zone analytique
    success = save_to_analytics_zone(spark, analytics_views)
    
    if not success:
        print("ERREUR : Échec de la sauvegarde dans la zone analytique.")
        spark.stop()
        sys.exit(1)
    
    # 5. Vérification des fichiers
    verify_analytics_files(spark)
    
    # 6. Justification du format
    justify_parquet_format()
    
    # 7. Instructions interface web
    show_hdfs_web_interface_instructions()
    
    # 8. Résumé final
    print("\n" + "="*80)
    print("RESUME FINAL - ZONE ANALYTIQUE")
    print("="*80)
    
    print("\nSTRUCTURATION ANALYTIQUE TERMINEE AVEC SUCCES")
    
    print("\nEmplacement HDFS :")
    print("   hdfs://namenode:9000/data/analytics/traffic/")
    
    print("\nDatasets créés :")
    print("   1. /kpi_strategique_zone   - KPI par zone")
    print("   2. /performance_routes     - Performance routes (partitionné)")
    print("   3. /analyse_congestion     - Analyse congestion (partitionné)")
    print("   4. /analyse_temporelle     - Tendances horaires (partitionné)")
    print("   5. /tableau_bord_executif  - Vue executive")
    
    print("\nCommandes de vérification :")
    print("   hdfs dfs -ls /data/analytics/traffic/")
    print("   hdfs dfs -ls -R /data/analytics/traffic/performance_routes/")
    print("   hdfs dfs -du -h /data/analytics/traffic/")
    
    print("\nInterface web HDFS :")
    print("   http://localhost:9870")
    print("   Naviguez vers: /data/analytics/traffic/")
    
    print("\n" + "="*80)
    print("OBJECTIF ATTEINT : Données prêtes pour l'analyse avancée")
    print("="*80)
    
    spark.stop()

# ============================================================
# 9. Exécution
# ============================================================
if __name__ == "__main__":
    main()