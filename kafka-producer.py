#!/usr/bin/env python3
"""
Kafka Producer for Smart City Traffic Data
Ingests real-time traffic events into Kafka
"""

import json
import time
from datetime import datetime
from typing import Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import sys

# Import your traffic generator
from traffic_data_generator import TrafficDataGenerator


class TrafficKafkaProducer:
    """
    Produces traffic events to Kafka topic
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'traffic-events',
        num_sensors: int = 50,
        num_roads: int = 100
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Performance settings
            acks='all',  # Wait for all replicas
            retries=3,
            batch_size=16384,  # Batch messages for efficiency
            linger_ms=10,  # Wait 10ms to batch messages
            compression_type='gzip',  # Compress data
            # Error handling
            max_in_flight_requests_per_connection=5
        )
        
        # Initialize traffic data generator
        self.generator = TrafficDataGenerator(num_sensors, num_roads)
        
        print("✓ Kafka Producer initialized")
        print(f"  Bootstrap servers: {bootstrap_servers}")
        print(f"  Topic: {topic}")
        print(f"  Sensors: {num_sensors}")
        print(f"  Roads: {num_roads}")
    
    def send_event(self, event: Dict) -> None:
        """
        Send a single event to Kafka
        Uses zone as partition key for better distribution
        """
        try:
            # Use zone as partition key to group related events
            partition_key = event['zone']
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=event
            )
            
            # Optional: wait for confirmation (synchronous)
            # record_metadata = future.get(timeout=10)
            
        except KafkaError as e:
            print(f"✗ Failed to send event: {e}")
            raise
    
    def produce_continuous(
        self,
        interval: float = 1.0,
        batch_size: int = 10,
        max_events: int = None
    ):
        """
        Continuously generate and send traffic events
        """
        event_count = 0
        failed_count = 0
        start_time = time.time()
        
        print("\n" + "="*80)
        print(" Starting Traffic Data Ingestion to Kafka")
        print("="*80)
        print(f"  Batch size: {batch_size} events")
        print(f"  Interval: {interval}s")
        print(f"  Max events: {max_events or 'unlimited'}")
        print("="*80 + "\n")
        
        try:
            while True:
                if max_events and event_count >= max_events:
                    break
                
                batch_start = time.time()
                
                # Generate batch of events
                batch = self.generator.generate_batch(batch_size)
                
                # Send each event to Kafka
                for event in batch:
                    try:
                        self.send_event(event)
                        event_count += 1
                        
                        # Print sample events
                        if event_count <= 5 or event_count % 100 == 0:
                            print(f"\n[Event #{event_count}] ✓ Sent to Kafka")
                            print(f"  Sensor: {event['sensor_id']}")
                            print(f"  Road: {event['road_id']} ({event['road_type']})")
                            print(f"  Zone: {event['zone']}")
                            print(f"  Vehicles: {event['vehicle_count']}")
                            print(f"  Speed: {event['average_speed']} km/h")
                            print(f"  Occupancy: {event['occupancy_rate']}%")
                        
                    except Exception as e:
                        failed_count += 1
                        print(f"✗ Error sending event: {e}")
                
                # Flush producer to ensure delivery
                self.producer.flush()
                
                # Statistics every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    print("\n" + "="*80)
                    print(f" Statistics")
                    print(f"  Total events sent: {event_count}")
                    print(f"  Failed: {failed_count}")
                    print(f"  Rate: {rate:.2f} events/sec")
                    print(f"  Running time: {elapsed:.2f}s")
                    print(f"  Time: {datetime.now().strftime('%H:%M:%S')}")
                    print("="*80 + "\n")
                
                # Wait for next batch
                batch_duration = time.time() - batch_start
                sleep_time = max(0, interval - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\n\n  Stopping producer (Ctrl+C detected)...")
        
        finally:
            # Cleanup
            print("\n" + "="*80)
            print(" Final Statistics")
            print("="*80)
            elapsed = time.time() - start_time
            print(f"  Total events sent: {event_count}")
            print(f"  Failed events: {failed_count}")
            print(f"  Success rate: {(event_count/(event_count+failed_count)*100):.2f}%")
            print(f"  Average rate: {event_count/elapsed:.2f} events/sec")
            print(f"  Total time: {elapsed:.2f}s")
            print("="*80)
            
            print("\n Closing Kafka producer...")
            self.producer.close()
            print("✓ Producer closed successfully\n")


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Producer for Smart City Traffic Data"
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
        '--sensors',
        type=int,
        default=50,
        help='Number of sensors (default: 50)'
    )
    parser.add_argument(
        '--roads',
        type=int,
        default=100,
        help='Number of roads (default: 100)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between batches in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of events per batch (default: 10)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        help='Maximum number of events to send (default: unlimited)'
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Run in demo mode (fast, limited events)'
    )
    
    args = parser.parse_args()
    
    try:
        producer = TrafficKafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            num_sensors=args.sensors,
            num_roads=args.roads
        )
        
        if args.demo:
            print("\n Running in DEMO mode\n")
            producer.produce_continuous(
                interval=0.5,
                batch_size=5,
                max_events=50
            )
        else:
            producer.produce_continuous(
                interval=args.interval,
                batch_size=args.batch_size,
                max_events=args.max_events
            )
            
    except KeyboardInterrupt:
        print("\n Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()