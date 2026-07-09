from kafka import KafkaProducer
import json
import os
import time
import random
import yaml
from datetime import datetime, timezone
from anomaly_injector import inject_anomaly
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from alerting.alert_handler import send_alert


# Load well baselines from config
baselines_path = os.path.join(os.path.dirname(__file__), "well_baselines.json")
with open(baselines_path) as f:
    WELLS = json.load(f)


# Load config
config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config.yml")
with open(config_path) as f:
    config = yaml.safe_load(f)

# Environment
env = os.getenv("ENV", "dev")

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=config["kafka"]["broker"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)


# Topic with environment suffix
topic = f"{config['kafka']['topic']}-{env}"


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
    print(f"Producing to topic: {topic}")
    print(f"Number of wells: {len(WELLS)}")
    print("-" * 50)


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
    except Exception as e:
        print(f"\n[CRITICAL] Producer crashed: {e}")
        send_alert(
            well_id="SYSTEM",
            anomaly_type="producer_crash",
            severity="critical",
            value=str(e),
            threshold="N/A",
        )
        raise
