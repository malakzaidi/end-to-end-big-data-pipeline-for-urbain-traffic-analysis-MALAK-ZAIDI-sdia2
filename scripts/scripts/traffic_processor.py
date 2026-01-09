from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max,
    min as spark_min, when, current_timestamp,
    to_timestamp, hour, date_format, regexp_replace, from_json, coalesce, lit,
    udf, trim
)
from pyspark.sql.types import *
import json
import sys

# ============================================================
# 1. Création de la Spark Session
# ============================================================
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Smart City Traffic Processor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark créée avec succès")
    return spark


# ============================================================
# 2. Fonction pour convertir le JSON non standard
# ============================================================
def convert_non_standard_json(json_str):
    """Convertit un JSON non standard (sans guillemets) en JSON standard"""
    if not json_str or json_str.strip() == '':
        return None
    
    try:
        # Supprimer les accolades
        content = json_str.strip()[1:-1]
        
        # Séparer par virgule
        pairs = content.split(',')
        
        # Reconstruire avec guillemets
        standard_pairs = []
        for pair in pairs:
            if ':' in pair:
                key, value = pair.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # Ajouter les guillemets manquants
                if not key.startswith('"'):
                    key = f'"{key}"'
                
                # Gérer les valeurs de chaîne
                if value.startswith('"') and value.endswith('"'):
                    # Déjà une chaîne entre guillemets
                    pass
                elif value.replace('.', '', 1).isdigit() or value.replace('.', '', 1).replace('-', '', 1).isdigit():
                    # C'est un nombre
                    pass
                elif value in ['true', 'false', 'null']:
                    # Valeurs JSON spéciales
                    pass
                else:
                    # C'est une chaîne sans guillemets
                    value = f'"{value}"'
                
                standard_pairs.append(f'{key}:{value}')
        
        # Reconstruire l'objet JSON
        standard_json = '{' + ','.join(standard_pairs) + '}'
        
        # Valider le JSON
        json.loads(standard_json)
        return standard_json
        
    except Exception as e:
        return None


# ============================================================
# 3. Chargement des données JSON non standard
# ============================================================
def load_raw_data(spark, input_path):
    print(f"Chargement des données depuis : {input_path}")
    
    try:
        # Étape 1: Lire comme texte brut
        print("1. Lecture des fichiers en texte brut...")
        raw_text_df = spark.read.text(input_path)
        
        total_lines = raw_text_df.count()
        print(f"   Total des lignes brutes : {total_lines}")
        
        # Étape 2: Filtrer les lignes vides
        print("2. Filtrage des lignes vides...")
        filtered_df = raw_text_df.filter(col("value").isNotNull() & (trim(col("value")) != ""))
        filtered_count = filtered_df.count()
        print(f"   Lignes après filtrage : {filtered_count}")
        
        if filtered_count == 0:
            print("Aucune donnée valide trouvée !")
            return None
        
        # Étape 3: Définir le schéma attendu
        schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("road_id", StringType(), True),
            StructField("road_type", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("vehicle_count", LongType(), True),
            StructField("average_speed", DoubleType(), True),
            StructField("occupancy_rate", DoubleType(), True),
            StructField("event_time", StringType(), True)
        ])
        
        # Étape 4: Convertir le JSON non standard en JSON standard
        print("\n3. Conversion du JSON non standard...")
        
        # Définir une UDF pour la conversion
        convert_json_udf = udf(lambda x: convert_non_standard_json(x), StringType())
        
        # Appliquer la conversion
        converted_df = filtered_df.withColumn("json_str", convert_json_udf(col("value")))
        
        # Filtrer les conversions réussies
        valid_json_df = converted_df.filter(col("json_str").isNotNull())
        valid_count = valid_json_df.count()
        print(f"   Conversions réussies : {valid_count}/{filtered_count}")
        
        if valid_count == 0:
            print("Aucune conversion JSON réussie !")
            print("Tentative d'utilisation de regex pour la conversion...")
            
            # Tentative avec regex
            fixed_json_df = filtered_df.withColumn(
                "json_str",
                regexp_replace(
                    regexp_replace(col("value"), 
                        r'(\w+):', 
                        r'"\1":'
                    ),
                    r':\s*([^",\{\}\[\]]+?)\s*(?=[,\}])',
                    r':"\1"'
                )
            )
            
            valid_json_df = fixed_json_df.filter(col("json_str").isNotNull())
            valid_count = valid_json_df.count()
            print(f"   Conversions regex réussies : {valid_count}")
            
            if valid_count == 0:
                return None
        
        # Étape 5: Parser le JSON standard
        print("\n4. Parsing du JSON standard...")
        parsed_df = valid_json_df.select(
            from_json(col("json_str"), schema).alias("data")
        ).select("data.*")
        
        parsed_count = parsed_df.count()
        print(f"   Lignes parsées avec succès : {parsed_count}")
        
        # Étape 6: Nettoyage et transformation
        print("\n5. Nettoyage et transformation des données...")
        
        # Convertir event_time en timestamp
        df = parsed_df.withColumn(
            "event_time",
            coalesce(
                to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")
            )
        )
        
        # Filtrer les event_time invalides
        initial_count = df.count()
        df = df.filter(col("event_time").isNotNull())
        filtered_time_count = df.count()
        
        if initial_count != filtered_time_count:
            print(f"   Lignes avec event_time invalide supprimées : {initial_count - filtered_time_count}")
        
        # Ajouter les colonnes temporelles
        df = df.withColumn("hour", hour("event_time")) \
               .withColumn("date_str", date_format("event_time", "yyyy-MM-dd"))
        
        # Étape 7: Validation
        print("\n6. Validation des données :")
        
        # Compter les valeurs nulles
        from pyspark.sql.functions import isnan, when, count
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        print("\n   Valeurs nulles par colonne :")
        null_counts.show()
        
        # Statistiques de base
        stats_df = df.select(
            count("*").alias("total_rows"),
            count("sensor_id").alias("sensor_ok"),
            count("vehicle_count").alias("vehicle_ok"),
            count("average_speed").alias("speed_ok"),
            count("event_time").alias("time_ok"),
            spark_min("event_time").alias("min_time"),
            spark_max("event_time").alias("max_time")
        )
        print("\n   Statistiques de base :")
        stats_df.show(truncate=False)
        
        # Échantillon final
        print("\n7. Échantillon des données finales :")
        df.select("sensor_id", "road_id", "road_type", "zone", 
                 "vehicle_count", "average_speed", "occupancy_rate", 
                 "event_time", "hour").show(10, truncate=False)
        
        return df
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        import traceback
        traceback.print_exc()
        
        return None


# ============================================================
# 4. Analyses
# ============================================================
def calculate_traffic_by_zone(df):
    print("\n" + "="*50)
    print("Calcul : Trafic moyen par zone et heure")
    print("="*50)
    
    result = df.groupBy("zone", "hour", "road_type").agg(
        avg("vehicle_count").alias("avg_vehicle_count"),
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy_rate"),
        spark_sum("vehicle_count").alias("total_vehicles"),
        count("*").alias("event_count"),
        spark_max("vehicle_count").alias("max_vehicles"),
        spark_min("average_speed").alias("min_speed")
    ).orderBy(col("avg_vehicle_count").desc())
    
    print("Aperçu des résultats :")
    result.show(10, truncate=False)
    return result


def calculate_speed_by_road(df):
    print("\n" + "="*50)
    print("Calcul : Vitesse moyenne par route")
    print("="*50)
    
    result = df.groupBy("road_id", "road_type", "zone").agg(
        avg("average_speed").alias("avg_speed"),
        avg("vehicle_count").alias("avg_vehicles"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("*").alias("measurement_count"),
        spark_max("average_speed").alias("max_speed"),
        spark_min("average_speed").alias("min_speed")
    ).orderBy(col("avg_speed").desc())
    
    print("Top 10 des routes les plus rapides :")
    result.show(10, truncate=False)
    return result


def calculate_congestion_rate(df):
    print("\n" + "="*50)
    print("Calcul du taux de congestion par zone")
    print("="*50)

    congestion_df = df.withColumn(
        "is_congested",
        when((col("road_type") == "autoroute") & 
             ((col("average_speed") < 50) | (col("occupancy_rate") > 30)), True)
        .when((col("road_type") == "avenue") & 
              ((col("average_speed") < 30) | (col("occupancy_rate") > 30)), True)
        .when((col("road_type") == "rue") & 
              ((col("average_speed") < 25) | (col("occupancy_rate") > 30)), True)
        .otherwise(False)
    )

    result = congestion_df.groupBy("zone").agg(
        count("*").alias("total_events"),
        spark_sum(when(col("is_congested"), 1).otherwise(0)).alias("congested_events"),
        avg("occupancy_rate").alias("avg_occupancy"),
        avg("average_speed").alias("avg_speed")
    ).withColumn(
        "congestion_rate_%",
        (col("congested_events") / col("total_events") * 100)
    ).orderBy(col("congestion_rate_%").desc())
    
    print("Taux de congestion par zone :")
    result.show(truncate=False)
    return result


def calculate_hourly_analysis(df):
    print("\n" + "="*50)
    print("Analyse horaire globale")
    print("="*50)
    
    result = df.groupBy("hour", "zone").agg(
        avg("vehicle_count").alias("avg_vehicles"),
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("*").alias("event_count")
    ).orderBy("hour", "zone")
    
    print("Distribution horaire :")
    result.show(24, truncate=False)
    return result


# ============================================================
# 5. Sauvegarde en Parquet
# ============================================================
def save_processed_data(spark, df, output_path, partition_by=None):
    print(f"Sauvegarde Parquet -> {output_path}")
    
    if df is None or df.count() == 0:
        print("Aucune donnée à sauvegarder !")
        return
    
    # Ajouter un timestamp de traitement
    df = df.withColumn("processed_at", current_timestamp())
    
    # Configurer l'écriture
    writer = df.write.mode("overwrite") \
               .option("compression", "snappy")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.parquet(output_path)
    
    # Vérification
    try:
        saved_df = spark.read.parquet(output_path)
        count = saved_df.count()
        print(f"Données sauvegardées avec succès : {count} lignes")
        
        if partition_by:
            partitions = saved_df.select(partition_by).distinct().count()
            print(f"Nombre de partitions : {partitions}")
            
    except Exception as e:
        print(f"Impossible de vérifier les données sauvegardées : {e}")
    
    print("Sauvegarde terminée\n")


# ============================================================
# 6. MAIN
# ============================================================
def main():
    INPUT_PATH = "hdfs://namenode:9000/data/raw/traffic"
    OUTPUT_BASE = "hdfs://namenode:9000/data/processed/traffic"
    
    spark = create_spark_session()
    
    print("\n" + "="*60)
    print("DEBUT DU PIPELINE DE TRAITEMENT DU TRAFIC")
    print("="*60)
    
    # Chargement avec gestion du JSON non standard
    df = load_raw_data(spark, INPUT_PATH)
    
    if df is None or df.count() == 0:
        print("\nERREUR : Aucune donnée valide chargée !")
        print("Veuillez vérifier :")
        print("1. Le format JSON de vos données (doit avoir des guillemets)")
        print("2. Le chemin HDFS : " + INPUT_PATH)
        print("3. Les permissions d'accès")
        spark.stop()
        sys.exit(1)
    
    print(f"\nDonnées chargées avec succès : {df.count()} lignes à traiter")
    
    # Analyses et sauvegardes
    print("\n" + "="*60)
    print("DEBUT DES ANALYSES")
    print("="*60)
    
    try:
        traffic_by_zone = calculate_traffic_by_zone(df)
        save_processed_data(spark, traffic_by_zone, f"{OUTPUT_BASE}/traffic_by_zone", ["zone", "hour"])
        
        speed_by_road = calculate_speed_by_road(df)
        save_processed_data(spark, speed_by_road, f"{OUTPUT_BASE}/speed_by_road", ["zone"])
        
        congestion_by_zone = calculate_congestion_rate(df)
        save_processed_data(spark, congestion_by_zone, f"{OUTPUT_BASE}/congestion_by_zone")
        
        hourly_traffic = calculate_hourly_analysis(df)
        save_processed_data(spark, hourly_traffic, f"{OUTPUT_BASE}/hourly_analysis", ["hour", "zone"])
        
    except Exception as e:
        print(f"\nErreur lors des analyses : {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    
    print("\n" + "="*60)
    print("RESUME FINAL")
    print("="*60)
    
    try:
        print("\nStatistiques finales :")
        print(f"1. Traffic par zone : {traffic_by_zone.count()} lignes")
        print(f"2. Vitesse par route : {speed_by_road.count()} lignes")
        print(f"3. Congestion par zone : {congestion_by_zone.count()} lignes")
        print(f"4. Analyse horaire : {hourly_traffic.count()} lignes")
        
        # Afficher les 3 zones les plus congestionnées
        print("\nTop 3 des zones les plus congestionnées :")
        congestion_by_zone.limit(3).show(truncate=False)
        
    except Exception as e:
        print(f"Impossible d'afficher les statistiques finales : {e}")
    
    print("\n" + "="*60)
    print("PIPELINE TERMINE AVEC SUCCES !")
    print("="*60)
    
    spark.stop()


if __name__ == "__main__":
    main()