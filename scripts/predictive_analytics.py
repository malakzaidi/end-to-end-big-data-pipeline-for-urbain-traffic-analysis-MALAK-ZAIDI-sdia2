#!/usr/bin/env python3
"""
Étape Avancée — Analyse Prédictive du Trafic

Objectif : Implémenter des modèles de machine learning pour prédire le trafic
- Prédiction de congestion 1-2 heures à l'avance
- Détection d'anomalies en temps réel
- Analyse de tendances saisonnières
- Recommandations d'optimisation des routes

Modèles utilisés :
- Régression linéaire pour prédiction de trafic
- Random Forest pour classification de congestion
- Isolation Forest pour détection d'anomalies
- Time Series Analysis pour tendances
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max,
    min as spark_min, when, current_timestamp,
    hour, date_format, dayofweek, month, year,
    lag, lead, window, unix_timestamp,
    round as spark_round, stddev, mean,
    to_timestamp, date_add, explode, arrays_zip
)
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
import pyspark.pandas as ps
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import json

# ============================================================
# 1. CONFIGURATION ET SESSION SPARK
# ============================================================

def create_spark_session():
    """Création de la session Spark optimisée pour le ML"""
    spark = SparkSession.builder \
        .appName("Smart City Traffic Prediction") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ml.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark ML créée avec succès")
    return spark

# ============================================================
# 2. CHARGEMENT DES DONNÉES HISTORIQUES
# ============================================================

def load_historical_data(spark):
    """Chargement des données historiques pour l'entraînement"""
    print("\n" + "="*60)
    print("CHARGEMENT DES DONNÉES HISTORIQUES")
    print("="*60)

    try:
        # Charger depuis HDFS - données brutes
        raw_data_path = "hdfs://namenode:9000/data/raw/traffic"
        print(f"Chargement depuis : {raw_data_path}")

        # Lire tous les fichiers JSON dans le répertoire raw
        raw_df = spark.read.json(f"{raw_data_path}/*/*.json")
        total_events = raw_df.count()
        print(f"Événements bruts chargés : {total_events}")

        if total_events < 100:
            print("AVERTISSEMENT : Peu de données historiques. Génération de données supplémentaires recommandée.")
            return None

        # Nettoyage et préparation
        df = raw_df.select(
            to_timestamp("event_time").alias("timestamp"),
            col("sensor_id"),
            col("road_id"),
            col("road_type"),
            col("zone"),
            col("vehicle_count").cast("double"),
            col("average_speed").cast("double"),
            col("occupancy_rate").cast("double")
        ).filter(col("timestamp").isNotNull())

        cleaned_count = df.count()
        print(f"Événements après nettoyage : {cleaned_count}")

        # Ajouter les features temporelles
        df = df.withColumn("hour", hour("timestamp")) \
               .withColumn("day_of_week", dayofweek("timestamp")) \
               .withColumn("month", month("timestamp")) \
               .withColumn("year", year("timestamp")) \
               .withColumn("date", date_format("timestamp", "yyyy-MM-dd"))

        print("Features temporelles ajoutées")
        print(f"Période couverte : {df.select(min('date'), max('date')).first()}")

        return df

    except Exception as e:
        print(f"Erreur lors du chargement : {e}")
        return None

# ============================================================
# 3. INGÉNIERIE DES FEATURES
# ============================================================

def create_features_for_prediction(df):
    """Création des features pour les modèles prédictifs"""
    print("\n" + "="*60)
    print("INGÉNIERIE DES FEATURES")
    print("="*60)

    # 1. Features temporelles encodées
    print("1. Encodage des features temporelles...")

    # Encoder road_type et zone
    road_type_indexer = StringIndexer(inputCol="road_type", outputCol="road_type_index")
    zone_indexer = StringIndexer(inputCol="zone", outputCol="zone_index")

    # One-hot encoding
    road_type_encoder = OneHotEncoder(inputCol="road_type_index", outputCol="road_type_vec")
    zone_encoder = OneHotEncoder(inputCol="zone_index", outputCol="zone_vec")

    # Pipeline d'encodage
    encoding_pipeline = Pipeline(stages=[road_type_indexer, zone_indexer, road_type_encoder, zone_encoder])
    encoded_df = encoding_pipeline.fit(df).transform(df)

    # 2. Features de lag (valeurs passées)
    print("2. Création des features de lag...")

    window_spec = window.Window.partitionBy("road_id", "zone").orderBy("timestamp")

    lag_df = encoded_df.withColumn("vehicle_count_lag1", lag("vehicle_count", 1).over(window_spec)) \
                       .withColumn("vehicle_count_lag2", lag("vehicle_count", 2).over(window_spec)) \
                       .withColumn("speed_lag1", lag("average_speed", 1).over(window_spec)) \
                       .withColumn("speed_lag2", lag("average_speed", 2).over(window_spec)) \
                       .withColumn("occupancy_lag1", lag("occupancy_rate", 1).over(window_spec)) \
                       .withColumn("occupancy_lag2", lag("occupancy_rate", 2).over(window_spec))

    # 3. Features statistiques glissantes
    print("3. Calcul des statistiques glissantes...")

    # Moyenne mobile sur 3 heures
    rolling_window = window.Window.partitionBy("road_id", "zone").orderBy("timestamp").rowsBetween(-3, 0)

    stats_df = lag_df.withColumn("vehicle_count_ma3", avg("vehicle_count").over(rolling_window)) \
                     .withColumn("speed_ma3", avg("average_speed").over(rolling_window)) \
                     .withColumn("occupancy_ma3", avg("occupancy_rate").over(rolling_window)) \
                     .withColumn("vehicle_count_std3", stddev("vehicle_count").over(rolling_window)) \
                     .withColumn("speed_std3", stddev("average_speed").over(rolling_window))

    # 4. Target variables (ce qu'on veut prédire)
    print("4. Création des variables cibles...")

    # Prédire le trafic dans 1 heure
    lead_window = window.Window.partitionBy("road_id", "zone").orderBy("timestamp")
    final_df = stats_df.withColumn("vehicle_count_target", lead("vehicle_count", 1).over(lead_window)) \
                      .withColumn("speed_target", lead("average_speed", 1).over(lead_window)) \
                      .withColumn("congestion_target",
                                 when(lead("average_speed", 1).over(lead_window) < 30, 1.0)
                                 .when(lead("occupancy_rate", 1).over(lead_window) > 70, 1.0)
                                 .otherwise(0.0))

    # Nettoyer les valeurs nulles
    clean_df = final_df.filter(col("vehicle_count_target").isNotNull()) \
                      .filter(col("speed_target").isNotNull()) \
                      .fillna(0, ["vehicle_count_lag1", "vehicle_count_lag2",
                                 "speed_lag1", "speed_lag2", "occupancy_lag1", "occupancy_lag2",
                                 "vehicle_count_ma3", "speed_ma3", "occupancy_ma3",
                                 "vehicle_count_std3", "speed_std3"])

    print(f"Dataset final : {clean_df.count()} lignes avec {len(clean_df.columns)} colonnes")
    print("Features créées avec succès")

    return clean_df

# ============================================================
# 4. MODÈLE DE PRÉDICTION DE TRAFIC
# ============================================================

def train_traffic_prediction_model(df):
    """Entraînement du modèle de prédiction de trafic"""
    print("\n" + "="*60)
    print("MODÈLE DE PRÉDICTION DE TRAFIC")
    print("="*60)

    # Sélection des features pour la prédiction de trafic
    feature_cols = [
        "hour", "day_of_week", "month",
        "vehicle_count", "average_speed", "occupancy_rate",
        "vehicle_count_lag1", "vehicle_count_lag2",
        "speed_lag1", "speed_lag2", "occupancy_lag1", "occupancy_lag2",
        "vehicle_count_ma3", "speed_ma3", "occupancy_ma3",
        "vehicle_count_std3", "speed_std3",
        "road_type_vec", "zone_vec"
    ]

    # Assembler les features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

    # Modèle de régression
    lr = LinearRegression(
        featuresCol="features",
        labelCol="vehicle_count_target",
        predictionCol="predicted_vehicle_count",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.1
    )

    # Pipeline complet
    pipeline = Pipeline(stages=[assembler, lr])

    # Division train/test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Train set : {train_df.count()} lignes")
    print(f"Test set : {test_df.count()} lignes")

    # Entraînement
    print("Entraînement du modèle...")
    model = pipeline.fit(train_df)

    # Évaluation
    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="vehicle_count_target",
        predictionCol="predicted_vehicle_count",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)
    print(".2f")

    evaluator_mae = RegressionEvaluator(
        labelCol="vehicle_count_target",
        predictionCol="predicted_vehicle_count",
        metricName="mae"
    )

    mae = evaluator_mae.evaluate(predictions)
    print(".2f")

    # Afficher quelques prédictions
    print("\nExemples de prédictions :")
    predictions.select("vehicle_count_target", "predicted_vehicle_count", "road_id", "zone") \
              .show(10, truncate=False)

    return model, predictions

# ============================================================
# 5. MODÈLE DE CLASSIFICATION DE CONGESTION
# ============================================================

def train_congestion_classification_model(df):
    """Entraînement du modèle de classification de congestion"""
    print("\n" + "="*60)
    print("MODÈLE DE CLASSIFICATION DE CONGESTION")
    print("="*60)

    # Features pour classification
    feature_cols = [
        "hour", "day_of_week", "month",
        "vehicle_count", "average_speed", "occupancy_rate",
        "vehicle_count_lag1", "speed_lag1", "occupancy_lag1",
        "vehicle_count_ma3", "speed_ma3", "occupancy_ma3",
        "road_type_vec", "zone_vec"
    ]

    # Assembler
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

    # Random Forest Classifier
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="congestion_target",
        predictionCol="congestion_prediction",
        numTrees=50,
        maxDepth=10,
        seed=42
    )

    # Pipeline
    pipeline = Pipeline(stages=[assembler, rf])

    # Division train/test
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)

    # Entraînement
    print("Entraînement du modèle de congestion...")
    model = pipeline.fit(train_df)

    # Évaluation
    predictions = model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="congestion_target",
        predictionCol="congestion_prediction",
        metricName="accuracy"
    )

    accuracy = evaluator.evaluate(predictions)
    print(".3f")

    # Matrice de confusion
    confusion_matrix = predictions.groupBy("congestion_target", "congestion_prediction").count()
    print("\nMatrice de confusion :")
    confusion_matrix.show()

    return model, predictions

# ============================================================
# 6. DÉTECTION D'ANOMALIES
# ============================================================

def detect_anomalies_isolation_forest(df):
    """Détection d'anomalies avec Isolation Forest"""
    print("\n" + "="*60)
    print("DÉTECTION D'ANOMALIES")
    print("="*60)

    # Convertir en Pandas pour sklearn
    pandas_df = df.select(
        "vehicle_count", "average_speed", "occupancy_rate",
        "hour", "day_of_week", "road_type_index", "zone_index"
    ).toPandas()

    from sklearn.ensemble import IsolationForest

    # Features pour la détection d'anomalies
    feature_cols = ["vehicle_count", "average_speed", "occupancy_rate", "hour", "day_of_week"]
    X = pandas_df[feature_cols]

    # Modèle Isolation Forest
    iso_forest = IsolationForest(
        n_estimators=100,
        contamination=0.1,  # 10% d'anomalies attendues
        random_state=42
    )

    # Prédiction
    anomaly_scores = iso_forest.fit_predict(X)
    anomaly_scores_proba = iso_forest.score_samples(X)

    # Ajouter les résultats au DataFrame
    pandas_df["anomaly_score"] = anomaly_scores_proba
    pandas_df["is_anomaly"] = (anomaly_scores == -1).astype(int)

    # Convertir en Spark DataFrame
    anomaly_df = spark.createDataFrame(pandas_df)

    # Statistiques
    anomaly_count = anomaly_df.filter(col("is_anomaly") == 1).count()
    total_count = anomaly_df.count()

    print(f"Anomalies détectées : {anomaly_count}/{total_count} ({anomaly_count/total_count*100:.1f}%)")

    # Afficher les anomalies les plus sévères
    print("\nTop 10 anomalies les plus sévères :")
    anomaly_df.filter(col("is_anomaly") == 1) \
              .orderBy(col("anomaly_score").asc()) \
              .select("vehicle_count", "average_speed", "occupancy_rate", "hour", "anomaly_score") \
              .show(10)

    return anomaly_df

# ============================================================
# 7. ANALYSE DE TENDANCES SAISONNIÈRES
# ============================================================

def analyze_seasonal_trends(df):
    """Analyse des tendances saisonnières et temporelles"""
    print("\n" + "="*60)
    print("ANALYSE DES TENDANCES SAISONNIÈRES")
    print("="*60)

    # Agrégation par heure et jour de la semaine
    hourly_trends = df.groupBy("hour", "day_of_week").agg(
        avg("vehicle_count").alias("avg_traffic"),
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("*").alias("sample_count")
    ).orderBy("day_of_week", "hour")

    print("Tendances horaires par jour de la semaine :")
    hourly_trends.show(24, truncate=False)

    # Analyse par type de route
    road_type_trends = df.groupBy("road_type", "hour").agg(
        avg("vehicle_count").alias("avg_traffic"),
        avg("average_speed").alias("avg_speed"),
        stddev("vehicle_count").alias("traffic_variability")
    ).orderBy("road_type", "hour")

    print("\nTendances par type de route :")
    road_type_trends.show(20, truncate=False)

    # Pics de trafic détectés
    peak_hours = hourly_trends.filter(col("avg_traffic") > hourly_trends.agg(avg("avg_traffic")).first()[0] * 1.5)
    print(f"\nHeures de pointe détectées : {peak_hours.count()} périodes")

    return hourly_trends, road_type_trends

# ============================================================
# 8. GÉNÉRATION DE RECOMMANDATIONS
# ============================================================

def generate_route_optimization_recommendations(df, predictions_df):
    """Génération de recommandations d'optimisation des routes"""
    print("\n" + "="*60)
    print("RECOMMANDATIONS D'OPTIMISATION")
    print("="*60)

    # Routes avec congestion fréquente
    congestion_routes = df.groupBy("road_id", "road_type", "zone").agg(
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("*").alias("total_measurements"),
        spark_sum(when(col("average_speed") < 30, 1).otherwise(0)).alias("congestion_events")
    ).withColumn("congestion_rate", col("congestion_events") / col("total_measurements")) \
     .orderBy(col("congestion_rate").desc())

    print("Routes avec congestion fréquente (>20%) :")
    congestion_routes.filter(col("congestion_rate") > 0.2).show(10, truncate=False)

    # Recommandations par type
    recommendations = []

    # Pour les routes très congestionnées
    high_congestion = congestion_routes.filter(col("congestion_rate") > 0.3)
    for row in high_congestion.collect():
        rec = {
            "road_id": row["road_id"],
            "zone": row["zone"],
            "type": "HIGH_CONGESTION",
            "recommendation": f"Augmenter la capacité ou ajouter des voies alternatives",
            "congestion_rate": float(row["congestion_rate"]),
            "priority": "HIGH"
        }
        recommendations.append(rec)

    # Pour les routes à vitesse moyenne basse
    low_speed = df.groupBy("road_id").agg(
        avg("average_speed").alias("avg_speed")
    ).filter(col("avg_speed") < 40).orderBy("avg_speed")

    for row in low_speed.collect()[:5]:  # Top 5
        rec = {
            "road_id": row["road_id"],
            "type": "LOW_SPEED",
            "recommendation": f"Améliorer les conditions de circulation",
            "avg_speed": float(row["avg_speed"]),
            "priority": "MEDIUM"
        }
        recommendations.append(rec)

    # Convertir en DataFrame Spark
    rec_df = spark.createDataFrame(recommendations)

    print(f"\nRecommandations générées : {rec_df.count()}")
    rec_df.show(truncate=False)

    return rec_df

# ============================================================
# 9. SAUVEGARDE DES RÉSULTATS
# ============================================================

def save_prediction_results(spark, predictions_df, congestion_predictions, anomalies_df, recommendations_df):
    """Sauvegarde des résultats de prédiction dans HDFS"""
    print("\n" + "="*60)
    print("SAUVEGARDE DES RÉSULTATS DE PRÉDICTION")
    print("="*60)

    PREDICTIONS_BASE = "hdfs://namenode:9000/data/predictions/traffic"

    try:
        # Créer le répertoire
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        ).mkdirs(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(PREDICTIONS_BASE))

        # Sauvegarder les prédictions de trafic
        traffic_pred_path = f"{PREDICTIONS_BASE}/traffic_predictions"
        predictions_df.select(
            "timestamp", "road_id", "zone", "vehicle_count_target",
            "predicted_vehicle_count", "average_speed", "occupancy_rate"
        ).write.mode("overwrite").parquet(traffic_pred_path)
        print(f"Prédictions trafic sauvegardées : {traffic_pred_path}")

        # Sauvegarder les prédictions de congestion
        congestion_pred_path = f"{PREDICTIONS_BASE}/congestion_predictions"
        congestion_predictions.select(
            "timestamp", "road_id", "zone", "congestion_target",
            "congestion_prediction", "average_speed", "occupancy_rate"
        ).write.mode("overwrite").parquet(congestion_pred_path)
        print(f"Prédictions congestion sauvegardées : {congestion_pred_path}")

        # Sauvegarder les anomalies
        anomalies_path = f"{PREDICTIONS_BASE}/anomalies_detected"
        anomalies_df.select(
            "vehicle_count", "average_speed", "occupancy_rate",
            "hour", "day_of_week", "is_anomaly", "anomaly_score"
        ).write.mode("overwrite").parquet(anomalies_path)
        print(f"Anomalies sauvegardées : {anomalies_path}")

        # Sauvegarder les recommandations
        recommendations_path = f"{PREDICTIONS_BASE}/route_recommendations"
        recommendations_df.write.mode("overwrite").parquet(recommendations_path)
        print(f"Recommandations sauvegardées : {recommendations_path}")

        print("\nVérification des sauvegardes :")
        for path in [traffic_pred_path, congestion_pred_path, anomalies_path, recommendations_path]:
            try:
                count = spark.read.parquet(path).count()
                print(f"  {path}: {count} enregistrements")
            except:
                print(f"  {path}: ERREUR de lecture")

    except Exception as e:
        print(f"Erreur lors de la sauvegarde : {e}")

# ============================================================
# 10. MAIN - Point d'entrée principal
# ============================================================

def main():
    print("\n" + "="*80)
    print("ÉTAPE AVANCÉE — ANALYSE PRÉDICTIVE DU TRAFIC")
    print("="*80)
    print("Objectif : Prédire le trafic et détecter les anomalies")
    print("="*80)

    # 1. Création de la session Spark
    spark = create_spark_session()

    # 2. Chargement des données historiques
    historical_df = load_historical_data(spark)

    if historical_df is None or historical_df.count() < 50:
        print("ERREUR : Données historiques insuffisantes pour l'entraînement.")
        print("Générez plus de données avant de lancer l'analyse prédictive.")
        spark.stop()
        sys.exit(1)

    # 3. Ingénierie des features
    features_df = create_features_for_prediction(historical_df)

    if features_df is None or features_df.count() < 20:
        print("ERREUR : Impossible de créer les features.")
        spark.stop()
        sys.exit(1)

    # 4. Modèle de prédiction de trafic
    traffic_model, traffic_predictions = train_traffic_prediction_model(features_df)

    # 5. Modèle de classification de congestion
    congestion_model, congestion_predictions = train_congestion_classification_model(features_df)

    # 6. Détection d'anomalies
    anomalies_df = detect_anomalies_isolation_forest(features_df)

    # 7. Analyse des tendances
    hourly_trends, road_trends = analyze_seasonal_trends(features_df)

    # 8. Génération de recommandations
    recommendations_df = generate_route_optimization_recommendations(features_df, traffic_predictions)

    # 9. Sauvegarde des résultats
    save_prediction_results(spark, traffic_predictions, congestion_predictions,
                          anomalies_df, recommendations_df)

    # 10. Résumé final
    print("\n" + "="*80)
    print("RÉSUMÉ FINAL - ANALYSE PRÉDICTIVE")
    print("="*80)

    print("Modèles entraînés avec succès :")
    print("  ✓ Prédiction de trafic (Régression Linéaire)")
    print("  ✓ Classification de congestion (Random Forest)")
    print("  ✓ Détection d'anomalies (Isolation Forest)")
    print("  ✓ Analyse de tendances saisonnières")
    print("  ✓ Recommandations d'optimisation")

    print(f"\nMétriques de performance :")
    print(f"  - Données d'entraînement : {features_df.count()} échantillons")
    print(f"  - Anomalies détectées : {anomalies_df.filter(col('is_anomaly') == 1).count()}")
    print(f"  - Recommandations générées : {recommendations_df.count()}")

    print(f"\nEmplacements HDFS :")
    print("  hdfs://namenode:9000/data/predictions/traffic/")
    print("    ├── traffic_predictions/")
    print("    ├── congestion_predictions/")
    print("    ├── anomalies_detected/")
    print("    └── route_recommendations/")

    print("\n" + "="*80)
    print("ANALYSE PRÉDICTIVE TERMINÉE AVEC SUCCÈS")
    print("Le système peut maintenant prédire le trafic en temps réel !")
    print("="*80)

    spark.stop()

# ============================================================
# 11. Exécution
# ============================================================

if __name__ == "__main__":
    main()