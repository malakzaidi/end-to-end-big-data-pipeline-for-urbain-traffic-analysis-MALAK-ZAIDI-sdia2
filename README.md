# Smart City Traffic Data Pipeline - Étape 1: Collecte des Données

##  Vue d'ensemble

Ce projet implémente un pipeline Big Data end-to-end pour l'analyse du trafic urbain dans le cadre d'une Smart City. Cette première étape concerne la **collecte et génération des données de trafic**.

##  Objectif de l'Étape 1

Simuler un réseau de capteurs urbains générant des événements de trafic en temps réel avec des valeurs réalistes.

##  Structure des Données

Chaque événement de trafic généré respecte la structure JSON suivante :

```json
{
  "sensor_id": "SENSOR_0001",
  "road_id": "ROAD_0042",
  "road_type": "autoroute",
  "zone": "Centre-Ville",
  "vehicle_count": 145,
  "average_speed": 95.32,
  "occupancy_rate": 78.45,
  "event_time": "2026-01-04T23:45:12.123456"
}
```

### Champs obligatoires

| Champ | Type | Description |
|-------|------|-------------|
| `sensor_id` | string | Identifiant unique du capteur (format: SENSOR_XXXX) |
| `road_id` | string | Identifiant unique de la route (format: ROAD_XXXX) |
| `road_type` | string | Type de route (autoroute, avenue, rue) |
| `zone` | string | Zone géographique |
| `vehicle_count` | integer | Nombre de véhicules détectés |
| `average_speed` | float | Vitesse moyenne en km/h |
| `occupancy_rate` | float | Taux d'occupation en pourcentage |
| `event_time` | string | Date et heure de la mesure (ISO 8601) |

##  Architecture du Générateur

### Zones urbaines simulées
- **Centre-Ville** : Zone urbaine dense
- **Zone-Industrielle** : Secteur industriel
- **Quartier-Residentiel** : Zone résidentielle
- **Zone-Commerciale** : Zone commerciale
- **Peripherie-Nord** : Périphérie nord
- **Peripherie-Sud** : Périphérie sud

### Types de routes et caractéristiques

####  Autoroute
- **Vitesse** : 80-130 km/h
- **Véhicules** : 50-200 véhicules
- **Occupation** : 40-95%

####  Avenue
- **Vitesse** : 40-80 km/h
- **Véhicules** : 20-100 véhicules
- **Occupation** : 30-85%

####  Rue
- **Vitesse** : 20-50 km/h
- **Véhicules** : 5-50 véhicules
- **Occupation** : 10-70%

##  Utilisation

### Installation des dépendances

Le générateur utilise uniquement des bibliothèques Python standard. Aucune dépendance externe n'est requise.

### Mode Demo (Test rapide)

Générer 50 événements pour tester le système :

```bash
python3 traffic_data_generator.py --demo
```

### Génération continue

Générer des événements en continu et les sauvegarder dans un fichier :

```bash
python3 traffic_data_generator.py --output traffic_events.json
```

### Options avancées

```bash
python3 traffic_data_generator.py \
  --sensors 100 \
  --roads 200 \
  --interval 0.5 \
  --batch-size 20 \
  --output traffic_events.json \
  --max-events 10000
```

### Paramètres disponibles

| Paramètre | Description | Défaut |
|-----------|-------------|--------|
| `--sensors` | Nombre de capteurs à simuler | 50 |
| `--roads` | Nombre de routes à simuler | 100 |
| `--interval` | Intervalle entre les batchs (secondes) | 1.0 |
| `--batch-size` | Nombre d'événements par batch | 10 |
| `--output` | Fichier de sortie (format JSON Lines) | None |
| `--max-events` | Nombre maximum d'événements | Illimité |
| `--demo` | Mode démo (50 événements) | False |

##  Réalisme des Données

### Variation temporelle

Le générateur simule des variations de trafic réalistes selon l'heure :

- **Heures de pointe matin** (7h-9h) : Facteur 1.2-1.5 
- **Heures de pointe soir** (17h-19h) : Facteur 1.3-1.5 
- **Heures creuses nuit** (22h-6h) : Facteur 0.3-0.6 
- **Heures normales** : Facteur 0.7-1.1 

### Corrélation des métriques

Le générateur assure une cohérence entre les différentes métriques :

1. **Plus de véhicules → Vitesse réduite**
   - La vitesse diminue proportionnellement à la densité du trafic

2. **Plus de véhicules → Taux d'occupation élevé**
   - Le taux d'occupation augmente avec le nombre de véhicules

3. **Heures de pointe → Plus de congestion**
   - Tous les indicateurs reflètent l'augmentation du trafic

##  Format de sortie

Le générateur produit des fichiers au format **JSON Lines** (JSONL), où chaque ligne est un événement JSON valide :

```jsonl
{"sensor_id": "SENSOR_0001", "road_id": "ROAD_0042", ...}
{"sensor_id": "SENSOR_0023", "road_id": "ROAD_0015", ...}
{"sensor_id": "SENSOR_0045", "road_id": "ROAD_0089", ...}
```

Ce format est optimal pour :
-  Le streaming de données
-  L'ingestion dans Kafka
-  Le traitement par Spark
-  Le stockage dans HDFS

## Exemples d'utilisation

### 1. Générer 1000 événements pour test

```bash
python3 traffic_data_generator.py \
  --max-events 1000 \
  --output test_data.json
```

### 2. Simulation haute fréquence

```bash
python3 traffic_data_generator.py \
  --interval 0.1 \
  --batch-size 50 \
  --output high_frequency.json
```

### 3. Réseau urbain étendu

```bash
python3 traffic_data_generator.py \
  --sensors 200 \
  --roads 500 \
  --output large_network.json
```

### 4. Génération continue (production)

```bash
python3 traffic_data_generator.py \
  --sensors 100 \
  --roads 200 \
  --interval 1.0 \
  --batch-size 10 \
  --output /data/traffic/events.json
```

##  Statistiques et monitoring

Le générateur affiche en temps réel :
-  Nombre d'événements générés
-  Heure actuelle
-  Facteur de trafic en cours
-  Configuration des capteurs et routes

##  Validation des données

### Vérification de la structure

Toutes les données générées respectent :
-  Structure JSON valide
-  Tous les champs obligatoires présents
-  Types de données corrects
-  Format ISO 8601 pour les timestamps

### Vérification de la cohérence

Les données générées sont cohérentes :
-  Vitesses réalistes selon le type de route
-  Corrélation trafic/vitesse respectée
-  Variation temporelle simulée
-  Valeurs dans les plages attendues

## Prochaines étapes

Une fois la génération de données validée, les étapes suivantes du projet seront :

1. **Étape 2** : Ingestion avec Apache Kafka
2. **Étape 3** : Stockage dans HDFS (Data Lake)
3. **Étape 4** : Traitement avec Apache Spark
4. **Étape 5** : Zone analytique (Parquet)
5. **Étape 6** : Visualisation avec Grafana
6. **Étape 7** : Orchestration avec Airflow

##  Notes techniques

- Le générateur utilise uniquement Python 3 standard
- Pas de dépendances externes requises
- Compatible Linux, macOS, Windows
- Thread-safe pour génération parallèle
- Optimisé pour performance et mémoire

##  Dépannage

### Le script ne démarre pas
```bash
# Vérifier la version Python
python3 --version  # Doit être >= 3.6

# Rendre le script exécutable
chmod +x traffic_data_generator.py
```

### Problèmes de permissions
```bash
# Créer le répertoire de sortie
mkdir -p /data/traffic

# Ajuster les permissions
chmod 755 /data/traffic
```

##  Contact

Pour toute question ou problème, veuillez consulter la documentation du projet Big Data.

---

**Projet** : Pipeline Big Data pour Smart City  
**Étape** : 1 - Collecte des données  
**Statut** :  Complété  
