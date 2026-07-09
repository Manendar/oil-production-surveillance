from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_spark_session(app_name="OilSurveillanceStreamProcessor"):
    """Create and return a configured Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()


def read_from_kafka(spark, broker, topic, group_id):
    """Read streaming data from Kafka."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", group_id) \
        .load()


def parse_messages(kafka_df, schema):
    """Parse Kafka messages from JSON to structured DataFrame."""
    return kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")


def detect_anomalies(df):
    """Filter rows that have anomalous readings based on thresholds."""
    return df.filter(
        (col("h2s_concentration") > 10) |
        (col("vibration_level") > 8) |
        (col("wellhead_pressure") < 400) |
        (col("water_cut") > 85) |
        (col("motor_current") < 1) |
        (col("sand_rate") > 40) |
        (col("casing_pressure") > 500)
    )


def write_all_to_s3(df, bucket):
    """Write all readings to S3 in parquet format."""
    return df.writeStream \
        .format("parquet") \
        .option("path", f"s3a://{bucket}/raw/") \
        .option("checkpointLocation", f"s3a://{bucket}/checkpoints/raw/") \
        .partitionBy("well_id") \
        .outputMode("append") \
        .start()


def write_anomalies_to_s3(df, bucket):
    """Write anomaly readings to a separate S3 path."""
    return df.writeStream \
        .format("parquet") \
        .option("path", f"s3a://{bucket}/anomalies/") \
        .option("checkpointLocation", f"s3a://{bucket}/checkpoints/anomalies/") \
        .partitionBy("well_id") \
        .outputMode("append") \
        .start()
