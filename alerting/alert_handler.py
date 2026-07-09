import boto3
import os
import yaml
import logging
from datetime import datetime, timezone


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config_path = os.getenv("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "..", "config.yml"))
with open(config_path) as f:
    config = yaml.safe_load(f)

# AWS config
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", config["aws"]["sns_topic_arn"])
AWS_REGION = config["project"]["region"]


def send_alert(well_id, anomaly_type, severity, value, threshold):
    """Send an SNS alert for a critical anomaly."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not configured. Skipping alert.")
        return

    sns_client = boto3.client("sns", region_name=AWS_REGION)

    subject = f"[{severity.upper()}] Well {well_id} - {anomaly_type}"

    message = (
        f"Oil Production Surveillance Alert\n"
        f"{'=' * 40}\n"
        f"Well ID:      {well_id}\n"
        f"Anomaly:      {anomaly_type}\n"
        f"Severity:     {severity.upper()}\n"
        f"Value:        {value}\n"
        f"Threshold:    {threshold}\n"
        f"Timestamp:    {datetime.now(timezone.utc).isoformat()}\n"
        f"{'=' * 40}\n"
        f"Action Required: Investigate immediately."
    )

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
        logger.info(f"[ALERT SENT] {subject}")
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {e}")
