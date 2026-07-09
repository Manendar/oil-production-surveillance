import sys
import os
import yaml

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql.types import StructType, StructField, StringType, FloatType
from utils.spark_functions import (
    create_spark_session,
    read_from_kafka,
    parse_messages,
    detect_anomalies,
    write_all_to_s3,
    write_anomalies_to_s3,
)


# Load config
config_path = config_path = os.getenv("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "..", "config.yml"))
with open(config_path) as f:
    config = yaml.safe_load(f)

# Environment
env = os.getenv("ENV", "dev")

# Config values
KAFKA_BROKER = os.getenv("KAFKA_BROKER", config["kafka"]["broker"])
KAFKA_TOPIC = f"{config['kafka']['topic']}-{env}"
KAFKA_GROUP_ID = config["kafka"]["consumer_group_spark"]
S3_BUCKET = f"oil-surveillance-{env}"

# Schema
WELL_READING_SCHEMA = StructType([
    StructField("well_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("well_status", StringType(), True),
    StructField("oil_flow_rate", FloatType(), True),
    StructField("gas_flow_rate", FloatType(), True),
    StructField("water_flow_rate", FloatType(), True),
    StructField("wellhead_pressure", FloatType(), True),
    StructField("bottomhole_pressure", FloatType(), True),
    StructField("casing_pressure", FloatType(), True),
    StructField("tubing_pressure", FloatType(), True),
    StructField("wellhead_temperature", FloatType(), True),
    StructField("bottomhole_temperature", FloatType(), True),
    StructField("choke_size", FloatType(), True),
    StructField("water_cut", FloatType(), True),
    StructField("gas_oil_ratio", FloatType(), True),
    StructField("pump_speed", FloatType(), True),
    StructField("motor_current", FloatType(), True),
    StructField("vibration_level", FloatType(), True),
    StructField("sand_rate", FloatType(), True),
    StructField("h2s_concentration", FloatType(), True),
    StructField("anomaly_type", StringType(), True),
])


def main():
    print("Starting Spark Stream Processor...")
    print(f"Environment: {env}")
    print(f"Reading from: {KAFKA_TOPIC}")
    print(f"Writing to: s3a://{S3_BUCKET}/")
    print("-" * 50)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    kafka_df = read_from_kafka(spark, KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID)

    # Parse JSON messages
    parsed_df = parse_messages(kafka_df, WELL_READING_SCHEMA)

    # Write all data to S3
    all_data_query = write_all_to_s3(parsed_df, S3_BUCKET)

    # Detect and write anomalies
    anomalies_df = detect_anomalies(parsed_df)
    anomalies_query = write_anomalies_to_s3(anomalies_df, S3_BUCKET)

    # Wait for streams to finish
    print("Stream processors running... Press Ctrl+C to stop.")
    all_data_query.awaitTermination()


if __name__ == "__main__":
    main()
