#!/usr/bin/env python3
"""
Génération de rapports d'analyse pour la décision urbaine
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_reports():
    """
    Génère des rapports et visualisations pour l'analyse de décision
    """
    spark = SparkSession.builder \
        .appName("Traffic Reports Generator") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n" + "="*60)
    print("GENERATION DE RAPPORTS D'ANALYSE")
    print("="*60)
    
    # Créer le dossier pour les rapports
    os.makedirs("/opt/reports", exist_ok=True)
    os.makedirs("/opt/reports/charts", exist_ok=True)
    
    # 1. RAPPORT 1 : Zones les plus congestionnées
    print("\n1. Rapport : Zones les plus congestionnées")
    df_congestion = spark.sql("""
        SELECT 
            zone,
            niveau_congestion,
            congestion_rate_percent,
            priorite_intervention,
            impact_estime_minutes
        FROM analyse_congestion
        ORDER BY congestion_rate_percent DESC
        LIMIT 10
    """)
    
    # Convertir en Pandas pour générer un rapport
    pd_congestion = df_congestion.toPandas()
    
    rapport_congestion = f"""
    RAPPORT D'ANALYSE DES ZONES CRITIQUES
    ====================================
    
    Zones nécessitant une intervention prioritaire :
    
    {pd_congestion.to_string(index=False)}
    
    Recommandations :
    1. Zones avec taux > 50% : Intervention immédiate nécessaire
    2. Zones avec taux 30-50% : Planification d'infrastructure
    3. Zones avec taux 15-30% : Monitoring renforcé
    
    Impact estimé total : {pd_congestion['impact_estime_minutes'].sum():.1f} minutes de retard/jour
    """
    
    with open("/opt/reports/zones_critiques.txt", "w") as f:
        f.write(rapport_congestion)
    
    print(f"   -> Rapport sauvegardé : /opt/reports/zones_critiques.txt")
    
    # 2. RAPPORT 2 : Performance globale
    print("\n2. Rapport : Performance globale du réseau")
    df_performance = spark.sql("""
        SELECT 
            niveau_service,
            COUNT(*) as nombre_zones,
            AVG(vitesse_moyenne_kmh) as vitesse_moyenne,
            AVG(trafic_moyen_vehicules) as trafic_moyen
        FROM kpi_zone_strategique
        GROUP BY niveau_service
        ORDER BY 
            CASE niveau_service
                WHEN 'A - Excellent' THEN 1
                WHEN 'B - Bon' THEN 2
                WHEN 'C - Moyen' THEN 3
                WHEN 'D - Faible' THEN 4
                WHEN 'E - Critique' THEN 5
                ELSE 6
            END
    """)
    
    pd_performance = df_performance.toPandas()
    
    rapport_performance = f"""
    RAPPORT DE PERFORMANCE DU RESEAU ROUTIER
    ========================================
    
    Distribution des zones par niveau de service :
    
    {pd_performance.to_string(index=False)}
    
    Analyse :
    - Excellent (A) : Circulation fluide, vitesse > 70 km/h
    - Bon (B) : Bonnes conditions, vitesse 50-70 km/h
    - Moyen (C) : Conditions acceptables, vitesse 30-50 km/h
    - Faible (D) : Ralentissements fréquents, vitesse 20-30 km/h
    - Critique (E) : Congestion sévère, vitesse < 20 km/h
    """
    
    with open("/opt/reports/performance_reseau.txt", "w") as f:
        f.write(rapport_performance)
    
    print(f"   -> Rapport sauvegardé : /opt/reports/performance_reseau.txt")
    
    # 3. RAPPORT 3 : Analyse temporelle
    print("\n3. Rapport : Analyse des tendances horaires")
    df_temporal = spark.sql("""
        SELECT 
            periode_journee,
            AVG(trafic_horaire_moyen) as trafic_moyen,
            AVG(vitesse_horaire_moyenne) as vitesse_moyenne,
            AVG(occupation_horaire_moyenne) as occupation_moyenne
        FROM analyse_temporelle
        GROUP BY periode_journee
        ORDER BY 
            CASE periode_journee
                WHEN 'Heures de pointe matin' THEN 1
                WHEN 'Pleine journée' THEN 2
                WHEN 'Heures de pointe soir' THEN 3
                WHEN 'Soirée' THEN 4
                WHEN 'Nuit' THEN 5
                ELSE 6
            END
    """)
    
    pd_temporal = df_temporal.toPandas()
    
    rapport_temporal = f"""
    RAPPORT D'ANALYSE TEMPORELLE
    ============================
    
    Performance par période de la journée :
    
    {pd_temporal.to_string(index=False)}
    
    Insights :
    - Heures de pointe : Trafic élevé, vitesse réduite
    - Pleine journée : Conditions optimales
    - Nuit : Trafic minimal, vitesse maximale
    """
    
    with open("/opt/reports/analyse_temporelle.txt", "w") as f:
        f.write(rapport_temporal)
    
    print(f"   -> Rapport sauvegardé : /opt/reports/analyse_temporelle.txt")
    
    # 4. VISUALISATIONS
    print("\n4. Génération de visualisations")
    
    # Graphique 1 : Congestion par zone
    plt.figure(figsize=(12, 6))
    top_zones = pd_congestion.head(5)
    bars = plt.bar(top_zones['zone'], top_zones['congestion_rate_percent'], 
                  color=['red' if x > 50 else 'orange' if x > 30 else 'yellow' for x in top_zones['congestion_rate_percent']])
    plt.title('Top 5 des Zones les Plus Congestionnées', fontsize=16)
    plt.xlabel('Zone', fontsize=12)
    plt.ylabel('Taux de Congestion (%)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    
    # Ajouter les valeurs sur les barres
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{height:.1f}%', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('/opt/reports/charts/congestion_zones.png', dpi=300)
    plt.close()
    
    # Graphique 2 : Distribution des niveaux de service
    plt.figure(figsize=(10, 6))
    service_counts = pd_performance.set_index('niveau_service')['nombre_zones']
    colors = ['green', 'lightgreen', 'yellow', 'orange', 'red']
    service_counts.plot(kind='pie', autopct='%1.1f%%', colors=colors, startangle=90)
    plt.title('Distribution des Zones par Niveau de Service', fontsize=16)
    plt.ylabel('')
    plt.tight_layout()
    plt.savefig('/opt/reports/charts/niveaux_service.png', dpi=300)
    plt.close()
    
    # Graphique 3 : Tendances horaires
    df_hourly = spark.sql("""
        SELECT hour, trafic_horaire_moyen, vitesse_horaire_moyenne 
        FROM analyse_temporelle 
        ORDER BY hour
    """).toPandas()
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    ax1.set_xlabel('Heure de la Journée')
    ax1.set_ylabel('Trafic Moyen (véhicules)', color='tab:blue')
    ax1.plot(df_hourly['hour'], df_hourly['trafic_horaire_moyen'], color='tab:blue', marker='o')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    
    ax2 = ax1.twinx()
    ax2.set_ylabel('Vitesse Moyenne (km/h)', color='tab:red')
    ax2.plot(df_hourly['hour'], df_hourly['vitesse_horaire_moyenne'], color='tab:red', marker='s')
    ax2.tick_params(axis='y', labelcolor='tab:red')
    
    plt.title('Évolution du Trafic et de la Vitesse par Heure', fontsize=16)
    fig.tight_layout()
    plt.savefig('/opt/reports/charts/evolution_horaire.png', dpi=300)
    plt.close()
    
    print(f"   -> Visualisations sauvegardées dans /opt/reports/charts/")
    
    # 5. RAPPORT SYNTHESE EXECUTIF
    print("\n5. Rapport exécutif synthétique")
    
    # Calculer les KPI globaux
    total_zones = spark.sql("SELECT COUNT(DISTINCT zone) FROM kpi_zone_strategique").collect()[0][0]
    avg_speed = spark.sql("SELECT AVG(vitesse_moyenne_kmh) FROM kpi_zone_strategique").collect()[0][0]
    avg_congestion = spark.sql("SELECT AVG(congestion_rate_percent) FROM analyse_congestion").collect()[0][0]
    critical_zones = spark.sql("SELECT COUNT(*) FROM analyse_congestion WHERE niveau_congestion = 'Élevée ( > 50%)'").collect()[0][0]
    
    rapport_executif = f"""
    RAPPORT EXECUTIF - ANALYSE DU TRAFIC URBAIN
    ===========================================
    
    SYNTHESE DES KPI PRINCIPAUX :
    
    • Zones surveillées : {total_zones}
    • Vitesse moyenne réseau : {avg_speed:.1f} km/h
    • Taux de congestion moyen : {avg_congestion:.1f}%
    • Zones en état critique : {critical_zones}
    
    RECOMMANDATIONS STRATEGIQUES :
    
    1. INTERVENTION PRIORITAIRE :
       - Zones avec taux de congestion > 50%
       - Planifier des travaux d'infrastructure
       - Optimiser la signalisation
    
    2. AMELIORATION DU RESEAU :
       - Développer les transports alternatifs
       - Mettre en place des zones à circulation restreinte
       - Améliorer la synchronisation des feux
    
    3. SURVEILLANCE ET MONITORING :
       - Renforcer la surveillance des zones critiques
       - Déployer des capteurs supplémentaires
       - Mettre en place un système d'alerte temps réel
    
    IMPACT ESTIME DES ACTIONS :
    
    • Réduction de 20-30% du temps de trajet moyen
    • Diminution de 15-25% des émissions CO2
    • Amélioration de la qualité de vie urbaine
    
    Prochaine revue : 1 mois
    """
    
    with open("/opt/reports/rapport_executif.txt", "w") as f:
        f.write(rapport_executif)
    
    print(f"   -> Rapport exécutif : /opt/reports/rapport_executif.txt")
    
    print("\n" + "="*60)
    print("GENERATION DES RAPPORTS TERMINEE")
    print("="*60)
    
    print("\nFichiers générés :")
    print("  /opt/reports/zones_critiques.txt")
    print("  /opt/reports/performance_reseau.txt")
    print("  /opt/reports/analyse_temporelle.txt")
    print("  /opt/reports/rapport_executif.txt")
    print("  /opt/reports/charts/congestion_zones.png")
    print("  /opt/reports/charts/niveaux_service.png")
    print("  /opt/reports/charts/evolution_horaire.png")
    
    spark.stop()

if __name__ == "__main__":
    generate_reports()