import boto3
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col
from pyspark.sql.types import DecimalType, TimestampType, BooleanType
from botocore.exceptions import ClientError

# ----------------------------------------
# Logging Setup
# ----------------------------------------

LOG_BUCKET = "lab3-raw"
LOG_PREFIX = "logs/summary/"
summary_log = []

def log_to_s3(message: str, level: str = "INFO", context: str = "Transform", immediate=False):
    global summary_log
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    formatted = f"[{timestamp}] [{level}] [{context}] {message}"
    summary_log.append(formatted)

    if immediate:
        s3 = boto3.client("s3")
        log_id = str(uuid.uuid4())[:8]
        key = f"{LOG_PREFIX}immediate/{context}_{level}_{timestamp.replace(':', '-')}_{log_id}.log"
        s3.put_object(Bucket=LOG_BUCKET, Key=key, Body=formatted.encode("utf-8"))

def upload_summary_log():
    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    key = f"{LOG_PREFIX}summary_{timestamp}.log"
    body = "\n".join(summary_log)
    s3.put_object(Bucket=LOG_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"Summary log written to s3://{LOG_BUCKET}/{key}")

# ----------------------------------------
# Helpers
# ----------------------------------------

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        log_to_s3(f"Bucket '{bucket_name}' already exists.", context="BucketCheck")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            log_to_s3(f"Creating bucket: {bucket_name}", context="BucketCheck")
            s3.create_bucket(Bucket=bucket_name)
        else:
            log_to_s3(f"Unexpected error checking bucket: {e}", level="ERROR", context="BucketCheck")
            raise

def archive_data(source_uri: str, dest_uri: str):
    s3 = boto3.client("s3")

    def parse_s3_uri(uri):
        if not uri.startswith("s3://"):
            raise ValueError("URI must start with s3://")
        parts = uri[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1].rstrip("/") + "/" if len(parts) > 1 else ""
        return bucket, prefix

    src_bucket, src_prefix = parse_s3_uri(source_uri)
    dest_bucket, dest_prefix = parse_s3_uri(dest_uri)

    ensure_s3_bucket(dest_bucket)

    paginator = s3.get_paginator("list_objects_v2")
    archived_files = 0

    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dest_key = dest_prefix + src_key[len(src_prefix):]
            s3.copy_object(
                Bucket=dest_bucket,
                CopySource={'Bucket': src_bucket, 'Key': src_key},
                Key=dest_key
            )
            s3.delete_object(Bucket=src_bucket, Key=src_key)
            archived_files += 1

    log_to_s3(f"Archived {archived_files} files from {source_uri} to {dest_uri}", context="Archive")

# ----------------------------------------
# Initialize Spark
# ----------------------------------------

spark = SparkSession.builder \
    .appName("Transform and Archive Curated Data") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# ----------------------------------------
# Ensure target bucket
# ----------------------------------------

presentation_bucket = "lab4-presentation-data"
ensure_s3_bucket(presentation_bucket)

# ----------------------------------------
# Load curated datasets
# ----------------------------------------

curated_base = "s3a://lab4-curated"

df_rentals = spark.read.parquet(f"{curated_base}/rental_transactions/")
df_users = spark.read.parquet(f"{curated_base}/users/")
df_vehicles = spark.read.parquet(f"{curated_base}/vehicles/")
df_locations = spark.read.parquet(f"{curated_base}/locations/")
log_to_s3("All curated datasets loaded successfully.", context="Load")

# ----------------------------------------
# Table 1: Vehicle and Location Performance Metrics
# ----------------------------------------

df_table1 = df_rentals.join(df_vehicles, on="vehicle_id", how="inner") \
    .select(
        col("total_amount").cast(DecimalType(10, 2)).alias("total_amount"),
        trim(df_rentals["dropoff_location"]).alias("location"),
        trim(df_rentals["rental_id"]).alias("rental_id"),
        df_rentals["vehicle_id"],
        trim(df_vehicles["vehicle_type"]).alias("vehicle_type")
    ).dropna()

row_count1 = df_table1.count()
df_table1.write.mode("overwrite").parquet(f"s3a://{presentation_bucket}/Vehicle_and_Location_Performance_Metrics/")
log_to_s3(f"Wrote {row_count1} records to Vehicle_and_Location_Performance_Metrics.", context="Table1")

# ----------------------------------------
# Table 2: User and Transaction Analysis
# ----------------------------------------

df_table2 = df_rentals.join(df_users, on="user_id", how="inner") \
    .select(
        col("total_amount").cast(DecimalType(10, 2)).alias("total_amount"),
        col("rental_end_time").cast(TimestampType()).alias("rental_end_time"),
        df_users["user_id"],
        col("is_active").cast(BooleanType()).alias("is_active")
    ).dropna()

row_count2 = df_table2.count()
df_table2.write.mode("overwrite").parquet(f"s3a://{presentation_bucket}/User_and_Transaction_Analysis/")
log_to_s3(f"Wrote {row_count2} records to User_and_Transaction_Analysis.", context="Table2")

# ----------------------------------------
# Archive curated data
# ----------------------------------------

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
for dataset in ["rental_transactions", "users", "vehicles", "locations"]:
    archive_data(
        source_uri=f"s3://lab4-curated/{dataset}/",
        dest_uri=f"s3://lab4-curated-archive/{dataset}/{timestamp}/"
    )

# ----------------------------------------
# Finalize
# ----------------------------------------

log_to_s3("Spark job completed successfully.", context="main")
upload_summary_log()
spark.stop()
