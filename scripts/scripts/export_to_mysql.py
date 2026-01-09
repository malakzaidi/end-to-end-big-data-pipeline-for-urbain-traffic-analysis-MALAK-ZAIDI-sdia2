# scripts/export_to_mysql.py
"""
Export des données Hive vers MySQL pour Superset et Grafana
"""

from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import Error

def export_hive_to_mysql():
    """
    Exporte les données des tables Hive vers MySQL
    """
    
    # 1. Initialiser Spark avec support Hive
    spark = SparkSession.builder \
        .appName("Hive to MySQL Export") \
        .config("spark.master", "local[*]") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # 2. Configuration MySQL
    mysql_config = {
        'host': 'mysql-superset',
        'database': 'superset',
        'user': 'superset',
        'password': 'superset',
        'port': 3306
    }
    
    # 3. Tables à exporter
    tables_to_export = [
        {
            'hive_table': 'kpi_zone_strategique',
            'mysql_table': 'kpi_zone_strategique',
            'mode': 'overwrite'
        },
        {
            'hive_table': 'performance_routes',
            'mysql_table': 'performance_routes',
            'mode': 'overwrite'
        },
        {
            'hive_table': 'analyse_congestion',
            'mysql_table': 'analyse_congestion',
            'mode': 'overwrite'
        },
        {
            'hive_table': 'analyse_temporelle',
            'mysql_table': 'analyse_temporelle',
            'mode': 'overwrite'
        },
        {
            'hive_table': 'tableau_bord_executif',
            'mysql_table': 'tableau_bord_executif',
            'mode': 'overwrite'
        }
    ]
    
    print("="*60)
    print("EXPORT HIVE -> MYSQL")
    print("="*60)
    
    # 4. Exporter chaque table
    for table in tables_to_export:
        try:
            print(f"\nExport de {table['hive_table']}...")
            
            # Lire depuis Hive
            df = spark.sql(f"SELECT * FROM {table['hive_table']}")
            
            # Afficher le schéma
            print(f"Schéma: {df.schema}")
            print(f"Nombre de lignes: {df.count()}")
            
            # Écrire dans MySQL
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", table['mysql_table']) \
                .option("user", mysql_config['user']) \
                .option("password", mysql_config['password']) \
                .mode(table['mode']) \
                .save()
            
            print(f"✓ Export réussi: {table['hive_table']} -> {table['mysql_table']}")
            
        except Exception as e:
            print(f"✗ Erreur pour {table['hive_table']}: {str(e)[:100]}")
    
    # 5. Créer des vues spécialisées pour Grafana
    create_mysql_views(mysql_config)
    
    spark.stop()
    print("\n" + "="*60)
    print("EXPORT TERMINÉ AVEC SUCCÈS")
    print("="*60)

def create_mysql_views(mysql_config):
    """
    Crée des vues MySQL optimisées pour Grafana
    """
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Vue pour le dashboard principal
        cursor.execute("""
            CREATE OR REPLACE VIEW dashboard_principal AS
            SELECT 
                k.zone,
                k.trafic_moyen_vehicules,
                k.vitesse_moyenne_kmh,
                k.taux_occupation_moyen,
                k.niveau_service,
                c.congestion_rate_percent,
                c.niveau_congestion,
                c.priorite_intervention,
                c.impact_estime_minutes,
                k.processed_at
            FROM kpi_zone_strategique k
            LEFT JOIN analyse_congestion c ON k.zone = c.zone
            ORDER BY k.processed_at DESC, c.priorite_intervention ASC
        """)
        
        # Vue pour l'analyse temporelle
        cursor.execute("""
            CREATE OR REPLACE VIEW analyse_par_heure AS
            SELECT 
                hour,
                trafic_horaire_moyen,
                vitesse_horaire_moyenne,
                occupation_horaire_moyenne,
                periode_journee,
                processed_at
            FROM analyse_temporelle
            ORDER BY hour
        """)
        
        # Vue pour les alertes
        cursor.execute("""
            CREATE OR REPLACE VIEW alertes_congestion AS
            SELECT 
                zone,
                congestion_rate_percent,
                niveau_congestion,
                priorite_intervention,
                processed_at,
                CASE 
                    WHEN congestion_rate_percent > 70 THEN 'CRITIQUE'
                    WHEN congestion_rate_percent > 50 THEN 'ÉLEVÉ'
                    WHEN congestion_rate_percent > 30 THEN 'MOYEN'
                    ELSE 'FAIBLE'
                END as severite
            FROM analyse_congestion
            WHERE congestion_rate_percent > 30
            ORDER BY priorite_intervention DESC, processed_at DESC
        """)
        
        connection.commit()
        print("✓ Vues MySQL créées avec succès")
        
    except Error as e:
        print(f"✗ Erreur création vues: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    export_hive_to_mysql()