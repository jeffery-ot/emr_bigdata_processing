import boto3
from datetime import datetime, timezone
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, countDistinct, when, unix_timestamp
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType
from botocore.exceptions import ClientError

# ----------------------------------------
# Logging Helper
# ----------------------------------------

LOG_BUCKET = 'lab3-raw'
LOG_PREFIX = 'logs/'

def log_to_s3(message: str, level: str = "INFO", context: str = "general"):
    s3 = boto3.client('s3')
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"{LOG_PREFIX}{now.strftime('%Y/%m/%d')}/{context}_{level}_{timestamp}_{unique_id}.log"
    log_content = f"[{timestamp}] [{level}] [{context}] {message}"

    try:
        s3.put_object(Bucket=LOG_BUCKET, Key=filename, Body=log_content.encode("utf-8"))
        print(f"[S3 LOG] s3://{LOG_BUCKET}/{filename}")
    except Exception as e:
        print(f"[LOG ERROR] Failed to write to S3: {e}")

# ----------------------------------------
# Helpers
# ----------------------------------------

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        log_to_s3(f"Bucket '{bucket_name}' already exists.", context="s3_setup")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            log_to_s3(f"Creating bucket: {bucket_name}", context="s3_setup")
            try:
                region = boto3.Session().region_name or 'us-east-1'
                if region == 'us-east-1':
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                log_to_s3(f"Successfully created bucket: {bucket_name}", context="s3_setup")
            except ClientError as create_error:
                log_to_s3(f"Error creating bucket {bucket_name}: {create_error}", level="ERROR", context="s3_setup")
                raise
        else:
            log_to_s3(f"Unexpected error checking bucket: {e}", level="ERROR", context="s3_setup")
            raise

def ensure_s3_directory(bucket_name, directory_path):
    s3 = boto3.client("s3")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_path, MaxKeys=1)
        if 'Contents' not in response:
            log_to_s3(f"Creating directory: s3://{bucket_name}/{directory_path}", context="s3_setup")
    except ClientError as e:
        log_to_s3(f"Error checking directory {directory_path}: {e}", level="ERROR", context="s3_setup")

def move_table_to_presentation_bucket(spark, source_path, dest_bucket, dest_path):
    try:
        df = spark.read.parquet(source_path)
        df.write.mode("overwrite").parquet(f"s3a://{dest_bucket}/{dest_path}")
        log_to_s3(f"Moved table from {source_path} to s3://{dest_bucket}/{dest_path}", context="move_table")
        verify_df = spark.read.parquet(f"s3a://{dest_bucket}/{dest_path}")
        record_count = verify_df.count()
        log_to_s3(f"Verified move: {record_count} records", context="move_table")
        return True
    except Exception as e:
        log_to_s3(f"Error moving table: {e}", level="ERROR", context="move_table")
        return False

# ----------------------------------------
# Initialize Spark
# ----------------------------------------

spark = SparkSession.builder \
    .appName("Vehicle and Location Performance Metrics Job") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

log_to_s3("Spark session initialized successfully", context="spark")

# ----------------------------------------
# Setup S3 buckets and directories
# ----------------------------------------

presentation_bucket = "lab4-presentation-data"
ensure_s3_bucket(presentation_bucket)

required_directories = [
    "Vehicle_Location_Analytics/",
    "Vehicle_Location_Analytics/Vehicle_and_Location_Performance_Metrics/",
    "Vehicle_Location_Analytics/Performance_Metrics_KPIs/",
    "Vehicle_Location_Analytics/Performance_Metrics_KPIs/Location_Metrics/"
]

for directory in required_directories:
    ensure_s3_directory(presentation_bucket, directory)

# ----------------------------------------
# Load existing Table 1
# ----------------------------------------

table1_current_path = f"s3a://{presentation_bucket}/Vehicle_Location_Analytics/Vehicle_and_Location_Performance_Metrics/"
table1_root_path = f"s3a://{presentation_bucket}/Vehicle_and_Location_Performance_Metrics/"

try:
    df_table1 = spark.read.parquet(table1_current_path)
    log_to_s3(f"Loaded Table 1 from: {table1_current_path}", context="data_load")
except:
    try:
        df_table1 = spark.read.parquet(table1_root_path)
        log_to_s3(f"Loaded Table 1 from: {table1_root_path}", context="data_load")
    except Exception as e:
        log_to_s3("Table 1 not found in either path", level="ERROR", context="data_load")
        spark.stop()
        raise Exception("Table 1 not found")

log_to_s3(f"Table 1 loaded with {df_table1.count()} records", context="data_load")

# ----------------------------------------
# Compute KPI Metrics
# ----------------------------------------

log_to_s3("Computing KPI metrics...", context="kpi")

df_location_metrics = df_table1.groupBy("location") \
    .agg(
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_per_location"),
        count("rental_id").alias("total_transactions_per_location"),
        avg("total_amount").cast(DecimalType(10, 2)).alias("avg_transaction_amount"),
        spark_max("total_amount").cast(DecimalType(10, 2)).alias("max_transaction_amount"),
        spark_min("total_amount").cast(DecimalType(10, 2)).alias("min_transaction_amount"),
        countDistinct("vehicle_id").alias("unique_vehicles_used")
    )

df_vehicle_metrics = df_table1.groupBy("vehicle_type") \
    .agg(spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_by_vehicle_type"))

log_to_s3("KPI metrics computed", context="kpi")

# ----------------------------------------
# Write Results to S3
# ----------------------------------------

base_path = f"s3a://{presentation_bucket}/Vehicle_Location_Analytics"

df_table1.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Vehicle_and_Location_Performance_Metrics/")
log_to_s3("Table 1 written to S3", context="s3_write")

df_location_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Performance_Metrics_KPIs/Location_Metrics/")
log_to_s3("Location KPIs written to S3", context="s3_write")

df_vehicle_metrics.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Performance_Metrics_KPIs/Vehicle_Type_Metrics/")
log_to_s3("Vehicle Type KPIs written to S3", context="s3_write")

# ----------------------------------------
# Summary
# ----------------------------------------

total_records_table1 = df_table1.count()
total_locations = df_location_metrics.count()
total_vehicle_types = df_vehicle_metrics.count()
total_revenue = df_table1.agg(spark_sum("total_amount")).collect()[0][0]
total_transactions = total_records_table1

log_to_s3(f"✓ Table 1 Records: {total_records_table1:,}", context="summary")
log_to_s3(f"✓ Unique Locations: {total_locations}", context="summary")
log_to_s3(f"✓ Vehicle Types: {total_vehicle_types}", context="summary")
log_to_s3(f"✓ Total Revenue: ${total_revenue:,.2f}", context="summary")
log_to_s3(f"✓ Total Transactions: {total_transactions:,}", context="summary")
log_to_s3("All outputs written to S3 and ready for Glue Catalog", context="summary")

# ----------------------------------------
# Cleanup
# ----------------------------------------

spark.stop()
log_to_s3("Spark job completed successfully", context="job_status")
