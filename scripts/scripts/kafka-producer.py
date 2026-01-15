#!/usr/bin/env python3
"""
Kafka Producer for Smart City Traffic Data - Batch mode from file
Ingests pre-generated traffic events from a JSON file into Kafka
"""

import json
import argparse
import sys
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError


class TrafficKafkaProducer:
    """
    Produces traffic events to Kafka from a pre-generated file
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'kafka:9092',  # ← important : nom du service docker
        topic: str = 'traffic-events'
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=5,
            batch_size=16384,
            linger_ms=5,
            compression_type='gzip',
            request_timeout_ms=30000,
            max_in_flight_requests_per_connection=5
        )
        
        print("✓ Kafka Producer initialisé (mode BATCH depuis fichier)")
        print(f"  Bootstrap servers: {bootstrap_servers}")
        print(f"  Topic: {topic}")

    def send_event(self, event: dict) -> bool:
        try:
            partition_key = event.get('zone', 'default')
            
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=event
            )
            # On peut attendre la confirmation si on veut être très sûr
            # future.get(timeout=10)
            return True
            
        except KafkaError as e:
            print(f"✗ Erreur envoi événement : {e}")
            return False

    def send_from_file(self, filepath: str):
        try:
            events = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            event = json.loads(line)
                            events.append(event)
                        except json.JSONDecodeError:
                            print(f"✗ Ligne invalide ignorée : {line[:60]}...")
                            continue

            if not events:
                print("ERREUR: Aucun événement valide trouvé dans le fichier")
                return 1

            print(f"\nEnvoi de {len(events)} événements vers Kafka...")
            
            success_count = 0
            for i, event in enumerate(events, 1):
                if self.send_event(event):
                    success_count += 1
                if i % 100 == 0:
                    print(f"  {i}/{len(events)} envoyés...")
                time.sleep(0.001)  # très léger backpressure

            self.producer.flush()
            print(f"\nRésultat final : {success_count}/{len(events)} événements envoyés avec succès")
            
            return 0 if success_count == len(events) else 1

        except FileNotFoundError:
            print(f"ERREUR: Fichier introuvable : {filepath}")
            return 1
        except Exception as e:
            print(f"Erreur inattendue : {e}")
            return 1

    def close(self):
        print("Fermeture du producer...")
        self.producer.flush()
        self.producer.close()


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer - Mode BATCH depuis fichier JSON")
    parser.add_argument('--bootstrap-servers', default='kafka:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='traffic-events',
                        help='Nom du topic Kafka')
    parser.add_argument('--file', required=True,
                        help='Chemin du fichier JSON contenant les événements (obligatoire en mode batch)')
    
    args = parser.parse_args()

    producer = TrafficKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )

    exit_code = producer.send_from_file(args.file)
    
    producer.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()