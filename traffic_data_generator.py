#!/usr/bin/env python3
"""
Traffic Data Generator for Smart City
Simulates urban traffic sensors generating real-time traffic events
"""

import json
import random
import time
from datetime import datetime
from typing import Dict, List
import argparse


class TrafficDataGenerator:
    """
    Generates realistic traffic data for smart city simulation
    """
    
    # Configuration des zones urbaines
    ZONES = [
        "Centre-Ville",
        "Zone-Industrielle", 
        "Quartier-Residentiel",
        "Zone-Commerciale",
        "Peripherie-Nord",
        "Peripherie-Sud"
    ]
    
    # Types de routes
    ROAD_TYPES = {
        "autoroute": {
            "speed_range": (80, 130),
            "vehicle_count_range": (50, 200),
            "occupancy_range": (40, 95)
        },
        "avenue": {
            "speed_range": (40, 80),
            "vehicle_count_range": (20, 100),
            "occupancy_range": (30, 85)
        },
        "rue": {
            "speed_range": (20, 50),
            "vehicle_count_range": (5, 50),
            "occupancy_range": (10, 70)
        }
    }
    
    def __init__(self, num_sensors: int = 50, num_roads: int = 100):
        """
        Initialize the traffic data generator
        
        Args:
            num_sensors: Number of sensors to simulate
            num_roads: Number of roads to simulate
        """
        self.num_sensors = num_sensors
        self.num_roads = num_roads
        self.sensors = self._initialize_sensors()
        self.roads = self._initialize_roads()
        
    def _initialize_sensors(self) -> List[Dict]:
        """Initialize sensor configuration"""
        sensors = []
        for i in range(self.num_sensors):
            sensor = {
                "sensor_id": f"SENSOR_{i+1:04d}",
                "zone": random.choice(self.ZONES),
                "status": "active"
            }
            sensors.append(sensor)
        return sensors
    
    def _initialize_roads(self) -> List[Dict]:
        """Initialize road configuration"""
        roads = []
        for i in range(self.num_roads):
            road_type = random.choice(list(self.ROAD_TYPES.keys()))
            road = {
                "road_id": f"ROAD_{i+1:04d}",
                "road_type": road_type,
                "zone": random.choice(self.ZONES)
            }
            roads.append(road)
        return roads
    
    def _get_time_factor(self) -> float:
        """
        Calculate traffic factor based on time of day
        Returns a multiplier between 0.3 and 1.5
        """
        current_hour = datetime.now().hour
        
        # Heures de pointe du matin (7h-9h)
        if 7 <= current_hour <= 9:
            return random.uniform(1.2, 1.5)
        # Heures de pointe du soir (17h-19h)
        elif 17 <= current_hour <= 19:
            return random.uniform(1.3, 1.5)
        # Heures creuses (22h-6h)
        elif current_hour >= 22 or current_hour <= 6:
            return random.uniform(0.3, 0.6)
        # Heures normales
        else:
            return random.uniform(0.7, 1.1)
    
    def _calculate_realistic_speed(self, road_type: str, vehicle_count: int, 
                                   base_range: tuple) -> float:
        """
        Calculate realistic speed based on traffic density
        
        Args:
            road_type: Type of road
            vehicle_count: Number of vehicles
            base_range: Base speed range for the road type
            
        Returns:
            Realistic speed in km/h
        """
        min_speed, max_speed = base_range
        
        # More vehicles = slower speed
        max_vehicles = self.ROAD_TYPES[road_type]["vehicle_count_range"][1]
        density_factor = 1 - (vehicle_count / max_vehicles) * 0.7
        
        # Calculate speed with some randomness
        speed = min_speed + (max_speed - min_speed) * density_factor
        speed *= random.uniform(0.9, 1.1)  # Add variation
        
        return round(max(min_speed * 0.5, min(speed, max_speed)), 2)
    
    def generate_event(self) -> Dict:
        """
        Generate a single traffic event
        
        Returns:
            Dictionary containing traffic event data
        """
        # Select random sensor and road
        sensor = random.choice(self.sensors)
        road = random.choice(self.roads)
        road_type = road["road_type"]
        
        # Get road type configuration
        road_config = self.ROAD_TYPES[road_type]
        
        # Apply time-based traffic factor
        time_factor = self._get_time_factor()
        
        # Generate vehicle count based on time and road type
        base_min, base_max = road_config["vehicle_count_range"]
        vehicle_count = int(random.randint(base_min, base_max) * time_factor)
        
        # Calculate realistic speed based on traffic
        speed_range = road_config["speed_range"]
        average_speed = self._calculate_realistic_speed(
            road_type, vehicle_count, speed_range
        )
        
        # Calculate occupancy rate (higher traffic = higher occupancy)
        occ_min, occ_max = road_config["occupancy_range"]
        base_occupancy = random.randint(occ_min, occ_max)
        occupancy_rate = round(min(base_occupancy * time_factor, 100), 2)
        
        # Create event
        event = {
            "sensor_id": sensor["sensor_id"],
            "road_id": road["road_id"],
            "road_type": road_type,
            "zone": road["zone"],
            "vehicle_count": vehicle_count,
            "average_speed": average_speed,
            "occupancy_rate": occupancy_rate,
            "event_time": datetime.now().isoformat()
        }
        
        return event
    
    def generate_batch(self, batch_size: int = 100) -> List[Dict]:
        """
        Generate a batch of traffic events
        
        Args:
            batch_size: Number of events to generate
            
        Returns:
            List of traffic events
        """
        return [self.generate_event() for _ in range(batch_size)]
    
    def generate_continuous(self, interval: float = 1.0, 
                          batch_size: int = 10,
                          output_file: str = None,
                          max_events: int = None):
        """
        Generate traffic events continuously
        
        Args:
            interval: Time between batches in seconds
            batch_size: Number of events per batch
            output_file: Optional file to save events (JSON Lines format)
            max_events: Maximum number of events to generate (None = infinite)
        """
        event_count = 0
        file_handle = None
        
        if output_file:
            file_handle = open(output_file, 'a')
            print(f" Saving events to: {output_file}")
        
        print(f" Starting continuous traffic data generation...")
        print(f" Configuration:")
        print(f"   - Sensors: {self.num_sensors}")
        print(f"   - Roads: {self.num_roads}")
        print(f"   - Zones: {len(self.ZONES)}")
        print(f"   - Batch size: {batch_size} events")
        print(f"   - Interval: {interval}s")
        print(f"   - Max events: {max_events if max_events else 'unlimited'}")
        print(f"\n Generation started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        try:
            while True:
                if max_events and event_count >= max_events:
                    break
                
                batch = self.generate_batch(batch_size)
                
                for event in batch:
                    event_count += 1
                    
                    # Print to console
                    if event_count % 100 == 0 or event_count <= 10:
                        print(f"\n[Event #{event_count}]")
                        print(json.dumps(event, indent=2))
                    
                    # Save to file if specified
                    if file_handle:
                        file_handle.write(json.dumps(event) + '\n')
                        file_handle.flush()
                
                # Status update
                if event_count % 100 == 0:
                    print(f"\n{'='*80}")
                    print(f" Generated {event_count} events so far...")
                    print(f" Current time: {datetime.now().strftime('%H:%M:%S')}")
                    print(f" Traffic factor: {self._get_time_factor():.2f}")
                    print(f"{'='*80}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n\n  Generation stopped by user")
        finally:
            if file_handle:
                file_handle.close()
            
            print(f"\n{'='*80}")
            print(f" Summary:")
            print(f"   - Total events generated: {event_count}")
            print(f"   - Duration: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            if output_file:
                print(f"   - Saved to: {output_file}")
            print(f"{'='*80}")
            print(" Generation completed!")


def main():
    """Main function to run the traffic data generator"""
    parser = argparse.ArgumentParser(
        description='Smart City Traffic Data Generator'
    )
    parser.add_argument(
        '--sensors',
        type=int,
        default=50,
        help='Number of sensors to simulate (default: 50)'
    )
    parser.add_argument(
        '--roads',
        type=int,
        default=100,
        help='Number of roads to simulate (default: 100)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Time between batches in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of events per batch (default: 10)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (JSON Lines format)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        help='Maximum number of events to generate'
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Run a quick demo (generate 50 events)'
    )
    
    args = parser.parse_args()
    
    # Create generator
    generator = TrafficDataGenerator(
        num_sensors=args.sensors,
        num_roads=args.roads
    )
    
    # Demo mode
    if args.demo:
        print(" Running DEMO mode (50 events)")
        generator.generate_continuous(
            interval=0.1,
            batch_size=5,
            output_file=args.output or "traffic_events_demo.json",
            max_events=50
        )
    else:
        # Continuous generation
        generator.generate_continuous(
            interval=args.interval,
            batch_size=args.batch_size,
            output_file=args.output,
            max_events=args.max_events
        )


if __name__ == "__main__":
    main()
