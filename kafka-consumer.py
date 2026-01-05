#!/usr/bin/env python3
"""
Kafka Consumer for Smart City Traffic Data
Consumes and displays traffic events from Kafka
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import argparse
import sys


class TrafficKafkaConsumer:
    """
    Consumes traffic events from Kafka topic
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'traffic-events',
        group_id: str = 'traffic-consumer-group'
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            # Start from beginning if no offset committed
            auto_offset_reset='earliest',
            # Commit offsets automatically
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            # Performance settings
            max_poll_records=100,
            session_timeout_ms=30000
        )
        
        print("✓ Kafka Consumer initialized")
        print(f"  Bootstrap servers: {bootstrap_servers}")
        print(f"  Topic: {topic}")
        print(f"  Group ID: {group_id}")
    
    def consume_continuous(self, max_events: int = None, verbose: bool = True):
        """
        Continuously consume messages from Kafka
        """
        event_count = 0
        
        print("\n" + "="*80)
        print(" Starting Traffic Data Consumption from Kafka")
        print("="*80)
        print(f"  Topic: {self.topic}")
        print(f"  Max events: {max_events or 'unlimited'}")
        print("="*80 + "\n")
        
        try:
            for message in self.consumer:
                if max_events and event_count >= max_events:
                    break
                
                event_count += 1
                event = message.value
                
                # Display event
                if verbose or event_count <= 10 or event_count % 50 == 0:
                    print(f"\n[Event #{event_count}] 📨 Received")
                    print(f"  Partition: {message.partition}")
                    print(f"  Offset: {message.offset}")
                    print(f"  Key: {message.key}")
                    print(f"  Timestamp: {datetime.fromtimestamp(message.timestamp/1000)}")
                    print(f"  ---")
                    print(f"  Sensor: {event['sensor_id']}")
                    print(f"  Road: {event['road_id']} ({event['road_type']})")
                    print(f"  Zone: {event['zone']}")
                    print(f"  Vehicles: {event['vehicle_count']}")
                    print(f"  Speed: {event['average_speed']} km/h")
                    print(f"  Occupancy: {event['occupancy_rate']}%")
                
                # Statistics every 100 events
                if event_count % 100 == 0:
                    print("\n" + "="*80)
                    print(f"📊 Consumed {event_count} events so far...")
                    print(f"  Time: {datetime.now().strftime('%H:%M:%S')}")
                    print("="*80 + "\n")
                
        except KeyboardInterrupt:
            print("\n\n  Stopping consumer (Ctrl+C detected)...")
        
        finally:
            print("\n" + "="*80)
            print("📈 Final Statistics")
            print("="*80)
            print(f"  Total events consumed: {event_count}")
            print("="*80)
            
            print("\n Closing Kafka consumer...")
            self.consumer.close()
            print("✓ Consumer closed successfully\n")


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Consumer for Smart City Traffic Data"
    )
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='traffic-events',
        help='Kafka topic name (default: traffic-events)'
    )
    parser.add_argument(
        '--group-id',
        default='traffic-consumer-group',
        help='Consumer group ID (default: traffic-consumer-group)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        help='Maximum number of events to consume (default: unlimited)'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Show only summary statistics'
    )
    
    args = parser.parse_args()
    
    try:
        consumer = TrafficKafkaConsumer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id
        )
        
        consumer.consume_continuous(
            max_events=args.max_events,
            verbose=not args.quiet
        )
            
    except KeyboardInterrupt:
        print("\n  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()