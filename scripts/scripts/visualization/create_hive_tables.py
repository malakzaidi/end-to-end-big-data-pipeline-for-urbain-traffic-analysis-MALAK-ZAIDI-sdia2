"""
Création des tables Hive - Version compatible spark-submit
"""

from pyspark.sql import SparkSession

def create_hive_tables():
    """
    Crée des tables Hive externes pointant vers les données analytiques
    """
    # Récupérer la session Spark existante ou en créer une nouvelle
    spark = SparkSession.builder.getOrCreate()
    
    print("SparkSession initialisée avec succès!")
    print(f"Version Spark: {spark.version}")
    
    # Configuration Hive
    try:
        spark.sql("SET hive.exec.dynamic.partition = true")
        spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        print("Configuration Hive appliquée")
    except Exception as e:
        print(f"Note: Configuration Hive non appliquée - {e}")
    
    # Liste des tables à créer
    tables_config = [
        {
            "name": "kpi_zone_strategique",
            "sql": """
                CREATE EXTERNAL TABLE IF NOT EXISTS kpi_zone_strategique (
                    zone STRING,
                    trafic_moyen_vehicules DOUBLE,
                    vitesse_moyenne_kmh DOUBLE,
                    taux_occupation_moyen DOUBLE,
                    total_vehicules_zone BIGINT,
                    nombre_mesures BIGINT,
                    pic_trafic BIGINT,
                    vitesse_minimale DOUBLE,
                    variabilite_trafic DOUBLE,
                    efficacite_circulation DOUBLE,
                    niveau_service STRING,
                    processed_at TIMESTAMP
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000/data/analytics/traffic/kpi_strategique_zone'
            """
        },
        {
            "name": "performance_routes",
            "sql": """
                CREATE EXTERNAL TABLE IF NOT EXISTS performance_routes (
                    road_id STRING,
                    road_type STRING,
                    zone STRING,
                    avg_speed DOUBLE,
                    avg_vehicles DOUBLE,
                    avg_occupancy DOUBLE,
                    measurement_count BIGINT,
                    max_speed DOUBLE,
                    min_speed DOUBLE,
                    rank INT,
                    performance_score INT,
                    status STRING,
                    processed_at TIMESTAMP
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000/data/analytics/traffic/performance_routes'
            """
        },
        {
            "name": "analyse_congestion",
            "sql": """
                CREATE EXTERNAL TABLE IF NOT EXISTS analyse_congestion (
                    zone STRING,
                    total_events BIGINT,
                    congested_events BIGINT,
                    avg_occupancy DOUBLE,
                    avg_speed DOUBLE,
                    congestion_rate_percent DOUBLE,
                    niveau_congestion STRING,
                    priorite_intervention INT,
                    impact_estime_minutes DOUBLE,
                    processed_at TIMESTAMP
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000/data/analytics/traffic/analyse_congestion'
            """
        }
    ]
    
    print("\n" + "="*60)
    print("CRÉATION DES TABLES HIVE")
    print("="*60)
    
    success_count = 0
    for table in tables_config:
        print(f"\nCréation de la table: {table['name']}")
        try:
            # Supprimer la table si elle existe
            spark.sql(f"DROP TABLE IF EXISTS {table['name']}")
            print(f"  - Table existante supprimée (si elle existait)")
            
            # Créer la nouvelle table
            spark.sql(table['sql'])
            print(f"✓ Table {table['name']} créée avec succès")
            
            # Vérifier que la table existe
            result = spark.sql(f"DESCRIBE FORMATTED {table['name']}")
            row_count = result.count()
            print(f"  - Schéma vérifié ({row_count} lignes de métadonnées)")
            success_count += 1
            
        except Exception as e:
            print(f"✗ Erreur lors de la création de {table['name']}: {str(e)}")
    
    # Vérification finale
    print("\n" + "="*60)
    print("VÉRIFICATION DES TABLES CRÉÉES")
    print("="*60)
    
    try:
        tables = spark.sql("SHOW TABLES").collect()
        print(f"\nNombre de tables trouvées: {len(tables)}")
        for table in tables:
            print(f"  - {table.database}.{table.tableName}")
    except Exception as e:
        print(f"Erreur lors de la vérification: {e}")
    
    # Afficher le schéma de chaque table
    print("\n" + "="*60)
    print("SCHÉMAS DES TABLES")
    print("="*60)
    
    for table in tables_config:
        try:
            print(f"\n--- Table: {table['name']} ---")
            spark.sql(f"DESCRIBE {table['name']}").show(truncate=False)
        except Exception as e:
            print(f"Erreur lors de l'affichage du schéma de {table['name']}: {e}")
    
    print("\n" + "="*60)
    print(f"CRÉATION TERMINÉE: {success_count}/{len(tables_config)} tables créées")
    print("="*60)
    
    return success_count == len(tables_config)

if __name__ == "__main__":
    import sys
    try:
        success = create_hive_tables()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)