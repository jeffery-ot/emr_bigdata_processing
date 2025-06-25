import boto3
from datetime import datetime
import uuid

s3 = boto3.client('s3')
BUCKET_NAME = 'lab3-raw'
LOG_PREFIX = 'logs/'


def log_to_s3(message: str, level: str = "INFO", context: str = "general"):
    """
    Logs a message to S3 in the specified bucket under 'logs/' with a timestamped filename.

    :param message: The log message content.
    :param level: Log level (e.g., INFO, ERROR, DEBUG).
    :param context: Optional context or source of the log (used in filename).
    """
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]

    filename = f"{LOG_PREFIX}{now.strftime('%Y/%m/%d')}/{context}_{level}_{timestamp}_{unique_id}.log"
    log_content = f"[{timestamp}] [{level}] [{context}] {message}"

    s3.put_object(Bucket=BUCKET_NAME, Key=filename, Body=log_content.encode("utf-8"))
    print(f"Logged to S3: s3://{BUCKET_NAME}/{filename}")
