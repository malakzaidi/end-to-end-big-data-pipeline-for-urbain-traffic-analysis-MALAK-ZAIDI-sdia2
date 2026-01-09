"""
Correction des schémas Hive pour correspondre aux fichiers Parquet
"""

from pyspark.sql import SparkSession

def fix_hive_tables():
    """
    Recrée les tables Hive avec les schémas corrects
    """
    spark = SparkSession.builder.getOrCreate()
    
    print("SparkSession initialisée avec succès!")
    print(f"Version Spark: {spark.version}")
    
    # Tables à corriger
    tables_to_fix = [
        {
            "name": "performance_routes",
            "sql": """
                CREATE EXTERNAL TABLE IF NOT EXISTS performance_routes (
                    road_id STRING,
                    avg_vehicles DOUBLE,
                    avg_speed DOUBLE,
                    avg_occupancy DOUBLE,
                    measurement_count BIGINT,
                    max_speed DOUBLE,
                    min_speed DOUBLE,
                    processed_at TIMESTAMP,
                    zone STRING,
                    rank INT,
                    performance_score INT,
                    status STRING,
                    road_type STRING
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
                    processed_at TIMESTAMP,
                    priorite_intervention INT,
                    impact_estime_minutes DOUBLE,
                    niveau_congestion STRING
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000/data/analytics/traffic/analyse_congestion'
            """
        }
    ]
    
    print("\n" + "="*60)
    print("CORRECTION DES TABLES HIVE")
    print("="*60)
    
    for table in tables_to_fix:
        print(f"\nRecréation de la table: {table['name']}")
        try:
            # Supprimer la table existante
            spark.sql(f"DROP TABLE IF EXISTS {table['name']}")
            print(f"  ✓ Table {table['name']} supprimée")
            
            # Créer la nouvelle table avec le bon schéma
            spark.sql(table['sql'])
            print(f"  ✓ Table {table['name']} recréée avec le schéma corrigé")
            
            # Tester une lecture
            df = spark.sql(f"SELECT * FROM {table['name']} LIMIT 1")
            print(f"  ✓ Test de lecture réussi ({df.count()} ligne)")
            
        except Exception as e:
            print(f"  ✗ Erreur: {str(e)}")
    
    # Vérification finale
    print("\n" + "="*60)
    print("VÉRIFICATION DES TABLES")
    print("="*60)
    
    for table in tables_to_fix:
        try:
            print(f"\n--- Table: {table['name']} ---")
            spark.sql(f"DESCRIBE {table['name']}").show(truncate=False)
            
            print(f"\nPremière ligne de {table['name']}:")
            spark.sql(f"SELECT * FROM {table['name']} LIMIT 1").show(truncate=False)
            
        except Exception as e:
            print(f"Erreur: {e}")
    
    print("\n" + "="*60)
    print("CORRECTION TERMINÉE")
    print("="*60)
    
    spark.stop()
    return True

if __name__ == "__main__":
    import sys
    try:
        success = fix_hive_tables()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)