# Utilise l'image officielle Apache Spark (version récente stable en 2026)
FROM apache/spark:latest

# Passe en root pour installer des packages Python
USER root

# Installe pip et les libs pour visualisations
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir matplotlib seaborn pandas && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Retour à l'utilisateur par défaut de l'image Spark
USER ${spark_uid}