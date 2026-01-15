#!/usr/bin/env python3
"""
Kafka Consumer → HDFS Raw Zone
Étape 3 : Ingestion des données brutes dans le Data Lake
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
import argparse
import sys
import os
import subprocess

class TrafficKafkaConsumerHDFS:
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'traffic-events',
        group_id: str = 'hdfs-writer-group'
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.hdfs_base = "/data/raw/traffic"
        
        # Créer le répertoire de base dans HDFS
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", self.hdfs_base], check=False)
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=100,
            session_timeout_ms=30000
        )
        
        print("Kafka Consumer → HDFS Raw Zone initialisé avec succès")
        print(f"  Topic: {topic}")
        print(f"  Écriture dans: {self.hdfs_base}/date=YYYY-MM-DD/zone=Nom_Zone/")
    
    def consume_and_write(self, max_events: int = None, verbose: bool = True):
        event_count = 0
        batch = []
        batch_size = 50  # Écriture par batch de 50 événements
        
        print("\n" + "="*80)
        print(" DÉBUT DE L'INGESTION : Kafka → HDFS Raw Zone")
        print("="*80)
        
        try:
            for message in self.consumer:
                if max_events and event_count >= max_events:
                    break
                
                event_count += 1
                event = message.value
                
                if verbose or event_count <= 10 or event_count % 50 == 0:
                    print(f"\n[Événement #{event_count}] Reçu et écrit dans HDFS")
                    print(f"  Partition: {message.partition} | Offset: {message.offset}")
                    print(f"  Zone: {event['zone']} | Véhicules: {event['vehicle_count']} | Vitesse: {event['average_speed']} km/h")
                
                # === GESTION ROBUSTE DE event_time (clé pour éviter les NULL dans Spark) ===
                event_time_str = event['event_time']
                try:
                    if 'T' in event_time_str:
                        # Format ISO avec ou sans millisecondes : "2026-01-05T12:29:40.123" ou "2026-01-05T12:29:40"
                        clean_time = event_time_str.split('.')[0]  # enlève les millisecondes
                        event_time = datetime.fromisoformat(clean_time.replace('T', ' '))
                    else:
                        # Format classique : "2026-01-05 12:29:40"
                        event_time = datetime.strptime(event_time_str, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"Erreur parsing date '{event_time_str}': {e} → date actuelle utilisée")
                    event_time = datetime.now()
                
                date = event_time.strftime("%Y-%m-%d")
                zone = event['zone'].replace(" ", "_").replace("-", "_")
                partition_path = f"{self.hdfs_base}/date={date}/zone={zone}"
                
                batch.append(json.dumps(event))
                
                # Écriture par batch
                if len(batch) >= batch_size:
                    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", partition_path], check=False)
                    
                    data = "\n".join(batch) + "\n"
                    cmd = f"echo \"{data}\" | docker exec -i namenode hdfs dfs -put - {partition_path}/part-{event_count//batch_size:05d}.jsonl"
                    os.system(cmd)
                    
                    if verbose:
                        print(f"Écrit {len(batch)} événements → {partition_path}")
                    
                    batch = []
                
                if event_count % 100 == 0:
                    print(f"{event_count} événements consommés et écrits dans HDFS...")
        
        except KeyboardInterrupt:
            print("\n\nArrêt du consumer (Ctrl+C)...")
        
        finally:
            # Écriture du dernier batch
            if batch:
                event_time = datetime.now()
                date = event_time.strftime("%Y-%m-%d")
                zone = "Unknown"
                partition_path = f"{self.hdfs_base}/date={date}/zone={zone}"
                subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", partition_path], check=False)
                data = "\n".join(batch) + "\n"
                cmd = f"echo \"{data}\" | docker exec -i namenode hdfs dfs -put - {partition_path}/final-batch.jsonl"
                os.system(cmd)
                print(f"Écrit dernier batch ({len(batch)} événements) dans HDFS")
            
            print("\n" + "="*80)
            print(f"INGESTION TERMINÉE : {event_count} événements écrits dans HDFS Raw Zone")
            print("="*80)
            self.consumer.close()

def main():
    parser = argparse.ArgumentParser(description="Kafka Consumer → HDFS Raw Zone (Étape 3)")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='traffic-events')
    parser.add_argument('--group-id', default='hdfs-writer-group')
    parser.add_argument('--max-events', type=int, help='Arrêter après N événements')
    parser.add_argument('--quiet', action='store_true', help='Moins verbeux')
    
    args = parser.parse_args()
    
    consumer = TrafficKafkaConsumerHDFS(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )
    
    consumer.consume_and_write(
        max_events=args.max_events,
        verbose=not args.quiet
    )

if __name__ == "__main__":
    main()