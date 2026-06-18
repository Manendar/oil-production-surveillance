from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone
import os


# Load well baselines from config
baselines_path = os.path.join(os.path.dirname(__file__), "well_baselines.json")
with open(baselines_path) as f:
    WELLS = json.load(f)


# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
    )


def inject_anomaly(reading, baselines):
    """Randomly inject a realistic anomaly into the reading."""
    anomaly_type = random.choice([
        "pressure_drop",
        "vibration_spike",
        "h2s_leak",
        "pump_failure",
        "water_cut_surge",
        "gas_breakthrough",
        "sand_erosion",
        "casing_integrity_loss",
    ])

    if anomaly_type == "pressure_drop":
        reading["wellhead_pressure"] = round(baselines["wellhead_pressure"] * random.uniform(0.5, 0.7), 2)
        reading["tubing_pressure"] = round(baselines["tubing_pressure"] * random.uniform(0.5, 0.7), 2)
    elif anomaly_type == "vibration_spike":
        reading["vibration_level"] = round(baselines["vibration_level"] * random.uniform(3, 5), 2)
    elif anomaly_type == "h2s_leak":
        reading["h2s_concentration"] = round(random.uniform(15, 80), 2)
    elif anomaly_type == "pump_failure":
        reading["motor_current"] = 0
        reading["pump_speed"] = 0
        reading["oil_flow_rate"] = 0
        reading["well_status"] = "shut-in"
    elif anomaly_type == "water_cut_surge":
        reading["water_cut"] = round(min(baselines["water_cut"] * random.uniform(2, 3), 95), 2)
        reading["water_flow_rate"] = round(baselines["water_flow_rate"] * random.uniform(2, 3), 2)
    elif anomaly_type == "gas_breakthrough":
        reading["gas_oil_ratio"] = round(baselines["gas_oil_ratio"] * random.uniform(2, 4), 2)
        reading["gas_flow_rate"] = round(baselines["gas_flow_rate"] * random.uniform(2, 3), 2)
    elif anomaly_type == "sand_erosion":
        reading["sand_rate"] = round(baselines["sand_rate"] * random.uniform(5, 10), 2)
    elif anomaly_type == "casing_integrity_loss":
        reading["casing_pressure"] = round(baselines["casing_pressure"] * random.uniform(2, 3.5), 2)

    reading["anomaly_type"] = anomaly_type
    return reading


def generate_reading(well_id, baselines):
    """Generate a single sensor reading for a well."""
    reading = {
        "well_id": well_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "well_status": "producing",
    }

    # Normal variation: ±5% from baseline
    for sensor, baseline in baselines.items():
        variation = random.uniform(-0.05, 0.05)
        reading[sensor] = round(baseline * (1 + variation), 2)

    # 5% chance of anomaly injection
    if random.random() < 0.05:
        reading = inject_anomaly(reading, baselines)

    return reading


if __name__ == "__main__":
    print("Starting well data producer...")
    print(f"Producing to topic: well-production-data")
    print(f"Number of wells: {len(WELLS)}")
    print("-" * 50)

    topic = "well-production-data"

    try:
        while True:
            for well_id, baselines in WELLS.items():
                reading = generate_reading(well_id, baselines)
                producer.send(topic, key=well_id, value=reading)

                if "anomaly_type" in reading:
                    print(f"[ANOMALY] {well_id} - {reading['anomaly_type']} at {reading['timestamp']}")
                else:
                    print(f"[OK] {well_id} at {reading['timestamp']}")

            producer.flush()
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nShutting down producer...")
        producer.close()
