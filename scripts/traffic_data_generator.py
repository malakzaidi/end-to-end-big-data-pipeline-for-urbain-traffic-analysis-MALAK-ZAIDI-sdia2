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

    ZONES = [
        "Centre-Ville",
        "Zone-Industrielle",
        "Quartier-Residentiel",
        "Zone-Commerciale",
        "Peripherie-Nord",
        "Peripherie-Sud"
    ]

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
        self.num_sensors = num_sensors
        self.num_roads = num_roads
        self.sensors = self._initialize_sensors()
        self.roads = self._initialize_roads()

    def _initialize_sensors(self) -> List[Dict]:
        sensors = []
        for i in range(self.num_sensors):
            sensors.append({
                "sensor_id": f"SENSOR_{i+1:04d}",
                "zone": random.choice(self.ZONES),
                "status": "active"
            })
        return sensors

    def _initialize_roads(self) -> List[Dict]:
        roads = []
        for i in range(self.num_roads):
            road_type = random.choice(list(self.ROAD_TYPES.keys()))
            roads.append({
                "road_id": f"ROAD_{i+1:04d}",
                "road_type": road_type,
                "zone": random.choice(self.ZONES)
            })
        return roads

    def _get_time_factor(self) -> float:
        hour = datetime.now().hour
        if 7 <= hour <= 9:
            return random.uniform(1.2, 1.5)
        elif 17 <= hour <= 19:
            return random.uniform(1.3, 1.5)
        elif hour >= 22 or hour <= 6:
            return random.uniform(0.3, 0.6)
        else:
            return random.uniform(0.7, 1.1)

    def _calculate_realistic_speed(self, road_type, vehicle_count, base_range):
        min_speed, max_speed = base_range
        max_vehicles = self.ROAD_TYPES[road_type]["vehicle_count_range"][1]
        density_factor = 1 - (vehicle_count / max_vehicles) * 0.7
        speed = min_speed + (max_speed - min_speed) * density_factor
        speed *= random.uniform(0.9, 1.1)
        return round(max(min_speed * 0.5, min(speed, max_speed)), 2)

    def generate_event(self) -> Dict:
        sensor = random.choice(self.sensors)
        road = random.choice(self.roads)
        config = self.ROAD_TYPES[road["road_type"]]
        time_factor = self._get_time_factor()

        vehicle_count = int(
            random.randint(*config["vehicle_count_range"]) * time_factor
        )

        average_speed = self._calculate_realistic_speed(
            road["road_type"],
            vehicle_count,
            config["speed_range"]
        )

        occupancy_rate = round(
            min(random.randint(*config["occupancy_range"]) * time_factor, 100),
            2
        )

        return {
            "sensor_id": sensor["sensor_id"],
            "road_id": road["road_id"],
            "road_type": road["road_type"],
            "zone": road["zone"],
            "vehicle_count": vehicle_count,
            "average_speed": average_speed,
            "occupancy_rate": occupancy_rate,
            "event_time": datetime.now().isoformat()
        }

    def generate_batch(self, batch_size: int) -> List[Dict]:
        return [self.generate_event() for _ in range(batch_size)]

    def generate_continuous(
        self,
        interval: float = 1.0,
        batch_size: int = 10,
        output_file: str = None,
        max_events: int = None
    ):
        output_file = output_file or "traffic_events.json"
        event_count = 0

        print(" Starting continuous traffic data generation...")
        print(f" Sensors: {self.num_sensors}")
        print(f" Roads: {self.num_roads}")
        print(f" Batch size: {batch_size}")
        print(f" Interval: {interval}s")
        print(f" Saving events to: {output_file}")
        if max_events:
            print(f" Max events: {max_events}")
        print("=" * 80)

        with open(output_file, "w", encoding="utf-8") as f:
            try:
                while True:
                    if max_events and event_count >= max_events:
                        print(f"\nMaximum events reached: {max_events}")
                        break

                    if max_events:
                        remaining = max_events - event_count
                        current_batch_size = min(batch_size, remaining)
                    else:
                        current_batch_size = batch_size

                    batch = self.generate_batch(current_batch_size)

                    for event in batch:
                        event_count += 1

                        if event_count <= 10 or event_count % 100 == 0:
                            print(f"\n[Event #{event_count}]")
                            print(json.dumps(event, indent=2))

                        f.write(json.dumps(event, ensure_ascii=False) + "\n")
                        f.flush()

                    if event_count % 100 == 0:
                        print("=" * 80)
                        print(f" Generated {event_count} events so far...")
                        print(f" Time: {datetime.now().strftime('%H:%M:%S')}")
                        print(f" Traffic factor: {self._get_time_factor():.2f}")
                        if max_events:
                            print(f" Progress: {event_count}/{max_events} ({100*event_count/max_events:.1f}%)")
                        print("=" * 80)

                    if max_events and event_count >= max_events:
                        break

                    if interval > 0:
                        time.sleep(interval)

            except KeyboardInterrupt:
                print("\nGeneration stopped by user")

        print("=" * 80)
        print(" Summary")
        print(f" Total events: {event_count}")
        print(f" Saved to: {output_file}")
        print("=" * 80)
        print(" Generation completed!")


def main():
    parser = argparse.ArgumentParser(description="Smart City Traffic Data Generator")
    parser.add_argument("--sensors", type=int, default=50)
    parser.add_argument("--roads", type=int, default=100)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--output", type=str)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--demo", action="store_true")

    args = parser.parse_args()

    generator = TrafficDataGenerator(args.sensors, args.roads)

    if args.demo:
        generator.generate_continuous(
            interval=0.1,
            batch_size=5,
            output_file=args.output or "traffic_events_demo.json",
            max_events=50
        )
    else:
        generator.generate_continuous(
            interval=args.interval,
            batch_size=args.batch_size,
            output_file=args.output,
            max_events=args.max_events
        )


if __name__ == "__main__":
    main()