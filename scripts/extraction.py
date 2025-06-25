import boto3
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Raw to Curated - KPI Ready") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# AWS S3 Setup
s3 = boto3.client('s3')
curated_base = "lab4-curated"
log_bucket = "lab3-raw"
log_prefix = "logs/"

# Helper function for logging to S3
def log_to_s3(message: str, level: str = "INFO", context: str = "ETL"):
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"{log_prefix}{now.strftime('%Y/%m/%d')}/{context}_{level}_{timestamp}_{unique_id}.log"
    content = f"[{timestamp}] [{level}] [{context}] {message}"
    s3.put_object(Bucket=log_bucket, Key=filename, Body=content.encode("utf-8"))

# Paths and required columns based on KPI requirements
paths = {
    "locations": {
        "input": "s3a://lab4-raw/locations/locations.csv",
        "output": f"s3a://{curated_base}/locations/",
        "columns": ["location_id", "city", "state"]
    },
    "rental_transactions": {
        "input": "s3a://lab4-raw/rental_transactions/rental_transactions.csv",
        "output": f"s3a://{curated_base}/rental_transactions/",
        "columns": [
            "rental_id", "user_id", "vehicle_id",
            "rental_start_time", "rental_end_time",
            "pickup_location", "dropoff_location", "total_amount"
        ]
    },
    "users": {
        "input": "s3a://lab4-raw/users/users.csv",
        "output": f"s3a://{curated_base}/users/",
        "columns": ["user_id", "first_name", "last_name", "email", "creation_date", "is_active"]
    },
    "vehicles": {
        "input": "s3a://lab4-raw/vehicles/vehicles.csv",
        "output": f"s3a://{curated_base}/vehicles/",
        "columns": ["vehicle_id", "brand", "vehicle_type", "vehicle_year"]
    }
}

# Ensure target S3 folders exist
def ensure_s3_folder(bucket_name, prefix):
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in result:
        s3.put_object(Bucket=bucket_name, Key=f"{prefix}__init__.txt", Body="")

# Process each dataset
for name, cfg in paths.items():
    try:
        log_to_s3(f"Starting processing for dataset: {name}", context=name)

        input_path = cfg["input"]
        output_path = cfg["output"]
        selected_columns = cfg["columns"]

        # Extract bucket and prefix
        bucket_name = output_path.replace("s3a://", "").split("/")[0]
        prefix = "/".join(output_path.replace("s3a://", "").split("/")[1:])

        ensure_s3_folder(bucket_name, prefix)

        # Load data
        df = spark.read.option("header", True).csv(input_path)

        # Select and trim relevant columns
        df = df.select([trim(col(c)).alias(c) for c in selected_columns])

        # Log pre-clean row count
        raw_count = df.count()
        log_to_s3(f"{name}: Record count before cleaning: {raw_count}", context=name)

        # Drop nulls
        df_cleaned = df.dropna()

        # Log post-clean row count
        cleaned_count = df_cleaned.count()
        log_to_s3(f"{name}: Record count after cleaning: {cleaned_count}", context=name)

        # Write to S3 in Parquet format
        df_cleaned.write.mode("overwrite").parquet(output_path)

        log_to_s3(f"Completed processing for dataset: {name}", context=name)

    except Exception as e:
        log_to_s3(f"Error processing dataset {name}: {str(e)}", level="ERROR", context=name)

log_to_s3("ETL process completed successfully", context="main")
spark.stop()
