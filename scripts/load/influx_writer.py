from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import sys
import json
import time
import yaml
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from alerting.alert_handler import send_alert


# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config.yml")
with open(config_path) as f:
    config = yaml.safe_load(f)

# Environment
env = os.getenv("ENV", "dev")

# InfluxDB config
INFLUXDB_URL = config["influxdb"]["url"]
INFLUXDB_TOKEN = config["influxdb"]["token"]
INFLUXDB_ORG = config["influxdb"]["org"]
INFLUXDB_BUCKET = f"{config['influxdb']['bucket']}-{env}"

# Kafka config
KAFKA_BROKER = config["kafka"]["broker"]
KAFKA_TOPIC = f"{config['kafka']['topic']}-{env}"
KAFKA_GROUP_ID = config["kafka"]["consumer_group_influx"]

# Anomaly thresholds
THRESHOLDS = {
    "h2s_concentration": {"max": 10, "severity": "critical"},
    "vibration_level": {"max": 8, "severity": "critical"},
    "wellhead_pressure": {"min": 400, "severity": "critical"},
    "water_cut": {"max": 85, "severity": "warning"},
    "motor_current": {"min": 1, "severity": "critical"},
    "sand_rate": {"max": 40, "severity": "warning"},
    "casing_pressure": {"max": 500, "severity": "critical"},
}


def check_anomalies(reading):
    """Check reading against thresholds, return list of detected anomalies."""
    anomalies = []

    for field, rules in THRESHOLDS.items():
        value = reading.get(field)
        if value is None:
            continue

        if "max" in rules and value > rules["max"]:
            anomalies.append({
                "field": field,
                "value": value,
                "threshold": rules["max"],
                "severity": rules["severity"],
                "type": f"{field}_high"
            })
        if "min" in rules and value < rules["min"]:
            anomalies.append({
                "field": field,
                "value": value,
                "threshold": rules["min"],
                "severity": rules["severity"],
                "type": f"{field}_low"
            })

    return anomalies


def write_reading(write_api, reading):
    """Write a well reading to InfluxDB."""
    point = Point("well_readings") \
        .tag("well_id", reading["well_id"]) \
        .tag("well_status", reading.get("well_status", "unknown"))

    numeric_fields = [
        "oil_flow_rate", "gas_flow_rate", "water_flow_rate",
        "wellhead_pressure", "bottomhole_pressure", "casing_pressure",
        "tubing_pressure", "wellhead_temperature", "bottomhole_temperature",
        "choke_size", "water_cut", "gas_oil_ratio", "pump_speed",
        "motor_current", "vibration_level", "sand_rate", "h2s_concentration"
    ]

    for field in numeric_fields:
        if field in reading:
            point = point.field(field, float(reading[field]))

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)


def write_anomaly(write_api, reading, anomaly):
    """Write a detected anomaly to InfluxDB."""
    point = Point("well_anomalies") \
        .tag("well_id", reading["well_id"]) \
        .tag("anomaly_type", anomaly["type"]) \
        .tag("severity", anomaly["severity"]) \
        .field("value", float(anomaly["value"])) \
        .field("threshold", float(anomaly["threshold"]))

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)


def main():
    # Setup InfluxDB
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )

    logger.info("Starting InfluxDB writer...")
    logger.info(f"Environment: {env}")
    logger.info(f"Consuming from: {KAFKA_TOPIC}")
    logger.info(f"Writing to bucket: {INFLUXDB_BUCKET}")
    logger.info("-" * 50)

    try:
        while True:
            try:
                for message in consumer:
                    reading = message.value

                    # Write raw reading
                    write_reading(write_api, reading)
                    logger.info(f"[OK] {reading['well_id']} at {reading.get('timestamp', 'N/A')}")

                    # Check for anomalies
                    anomalies = check_anomalies(reading)
                    for anomaly in anomalies:
                        write_anomaly(write_api, reading, anomaly)
                        logger.warning(
                            f"[ANOMALY] {reading['well_id']} - {anomaly['type']} "
                            f"(value: {anomaly['value']}, threshold: {anomaly['threshold']}, "
                            f"severity: {anomaly['severity']})"
                        )

                        # Send SNS alert for critical anomalies
                        if anomaly["severity"] == "critical":
                            send_alert(
                                well_id=reading["well_id"],
                                anomaly_type=anomaly["type"],
                                severity=anomaly["severity"],
                                value=anomaly["value"],
                                threshold=anomaly["threshold"],
                            )
            except ValueError as e:
                logger.error(f"Connection error: {e}. Reconnecting...")
                time.sleep(2)
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BROKER,
                    group_id=KAFKA_GROUP_ID,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='latest'
                )

    except KeyboardInterrupt:
        logger.info("\nShutting down InfluxDB writer...")
        consumer.close()
        client.close()
    except Exception as e:
        logger.critical(f"InfluxDB writer crashed: {e}")
        send_alert(
            well_id="SYSTEM",
            anomaly_type="influx_writer_crash",
            severity="critical",
            value=str(e),
            threshold="N/A",
        )
        raise
    

if __name__ == "__main__":
    main()

