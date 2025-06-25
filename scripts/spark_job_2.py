import boto3
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trim, col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, 
    countDistinct, when, unix_timestamp, to_date, date_format, 
    datediff, regexp_replace, split, desc
)
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType, DateType

# -----------------------------
# Logging Function to S3
# -----------------------------
def log_to_s3(message: str, level: str = "INFO", context: str = "general"):
    """
    Logs a message to S3 in the specified bucket under 'logs/' with a timestamped filename.
    """
    s3 = boto3.client('s3')
    BUCKET_NAME = 'lab3-raw'
    LOG_PREFIX = 'logs/'

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"{LOG_PREFIX}{now.strftime('%Y/%m/%d')}/{context}_{level}_{timestamp}_{unique_id}.log"
    log_content = f"[{timestamp}] [{level}] [{context}] {message}"

    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=filename, Body=log_content.encode("utf-8"))
        print(f"Logged to S3: s3://{BUCKET_NAME}/{filename}")
    except Exception as e:
        print(f"Failed to log to S3: {e}")

# -----------------------------
# Helper Functions
# -----------------------------

def ensure_s3_bucket(bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            try:
                region = boto3.Session().region_name or 'us-east-1'
                if region == 'us-east-1':
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Successfully created bucket: {bucket_name}")
            except ClientError as create_error:
                print(f"Error creating bucket {bucket_name}: {create_error}")
                raise
        else:
            print(f"Unexpected error checking bucket: {e}")
            raise

def ensure_s3_directory(bucket_name, directory_path):
    s3 = boto3.client("s3")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_path, MaxKeys=1)
        if 'Contents' not in response:
            print(f"Creating directory: s3://{bucket_name}/{directory_path}")
    except ClientError as e:
        print(f"Error checking directory {directory_path}: {e}")

# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("User and Transaction Analysis Job") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

log_to_s3("Spark session initialized", context="spark_init")

# -----------------------------
# Setup S3 Buckets and Directories
# -----------------------------
presentation_bucket = "lab4-presentation-data"
ensure_s3_bucket(presentation_bucket)

required_directories = [
    "User_Transaction_Analytics/",
    "User_Transaction_Analytics/User_and_Transaction_Analysis/",
    "User_Transaction_Analytics/Transaction_Metrics_KPIs/",
    "User_Transaction_Analytics/Transaction_Metrics_KPIs/Daily_Metrics/",
    "User_Transaction_Analytics/Transaction_Metrics_KPIs/User_Spending_Metrics/",
    "User_Transaction_Analytics/Transaction_Metrics_KPIs/User_Duration_Metrics/",
    "User_Transaction_Analytics/Transaction_Metrics_KPIs/Transaction_Amount_Metrics/"
]
for directory in required_directories:
    ensure_s3_directory(presentation_bucket, directory)

# -----------------------------
# Load Table 2
# -----------------------------
table2_current_path = f"s3a://{presentation_bucket}/User_Transaction_Analytics/User_and_Transaction_Analysis/"
table2_root_path = f"s3a://{presentation_bucket}/User_and_Transaction_Analysis/"

try:
    df_table2 = spark.read.parquet(table2_current_path)
    log_to_s3("Loaded Table 2 from organized location", context="load_table")
except:
    try:
        df_table2 = spark.read.parquet(table2_root_path)
        log_to_s3("Loaded Table 2 from root location", context="load_table")
    except Exception as e:
        log_to_s3(f"Failed to load Table 2: {e}", level="ERROR", context="load_table")
        spark.stop()
        raise Exception("Table 2 not found.")

# -----------------------------
# Data Preparation
# -----------------------------
available_columns = df_table2.columns
df_cleaned = df_table2
date_columns = [col for col in available_columns if 'date' in col.lower() or 'time' in col.lower()]

if 'pickup_date' in available_columns and 'return_date' in available_columns:
    df_cleaned = df_cleaned.withColumn("rental_duration_days", datediff(col("return_date"), col("pickup_date")))
    df_cleaned = df_cleaned.withColumn("transaction_date", to_date(col("pickup_date")))
elif 'pickup_date' in available_columns:
    df_cleaned = df_cleaned.withColumn("transaction_date", to_date(col("pickup_date")))

if 'total_amount' in available_columns:
    df_cleaned = df_cleaned.withColumn("total_amount", col("total_amount").cast(DecimalType(15, 2)))

log_to_s3("Data cleaning completed", context="data_prep")

# -----------------------------
# KPI Metrics
# -----------------------------
if 'transaction_date' in df_cleaned.columns:
    df_daily_metrics = df_cleaned.groupBy("transaction_date").agg(
        count("rental_id").alias("total_transactions_per_day"),
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_per_day")
    ).orderBy("transaction_date")
else:
    df_daily_metrics = None

df_user_spending_metrics = df_cleaned.groupBy("user_id").agg(
    spark_sum("total_amount").cast(DecimalType(15, 2)).alias("user_total_spending")
)

if 'rental_duration_days' in df_cleaned.columns:
    df_user_duration_metrics = df_cleaned.groupBy("user_id").agg(
        avg("rental_duration_days").cast(DecimalType(10, 2)).alias("user_avg_rental_duration"),
        spark_sum("rental_duration_days").alias("user_total_rental_duration")
    )
else:
    df_user_duration_metrics = None

df_transaction_amount_metrics = df_cleaned.agg(
    spark_max("total_amount").cast(DecimalType(10, 2)).alias("max_transaction_amount"),
    spark_min("total_amount").cast(DecimalType(10, 2)).alias("min_transaction_amount")
)

log_to_s3("All KPI metrics computed", context="kpi")

# -----------------------------
# Write to S3
# -----------------------------
base_path = f"s3a://{presentation_bucket}/User_Transaction_Analytics"

df_cleaned.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/User_and_Transaction_Analysis/")
if df_daily_metrics:
    df_daily_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Transaction_Metrics_KPIs/Daily_Metrics/")
df_user_spending_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Transaction_Metrics_KPIs/User_Spending_Metrics/")
if df_user_duration_metrics:
    df_user_duration_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Transaction_Metrics_KPIs/User_Duration_Metrics/")
df_transaction_amount_metrics.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_path}/Transaction_Metrics_KPIs/Transaction_Amount_Metrics/")

log_to_s3("All tables successfully written to presentation bucket", context="write_s3")

# -----------------------------
# Summary Stats
# -----------------------------
total_records = df_cleaned.count()
user_count = df_user_spending_metrics.count()
amount_metrics = df_transaction_amount_metrics.collect()[0]
total_revenue = df_user_spending_metrics.agg(spark_sum("user_total_spending")).collect()[0][0]

summary_message = f"""Summary:
Records: {total_records}
Users: {user_count}
Max Transaction: ${amount_metrics['max_transaction_amount']:.2f}
Min Transaction: ${amount_metrics['min_transaction_amount']:.2f}
Total Revenue: ${total_revenue:,.2f}
"""
log_to_s3(summary_message.strip(), context="summary")

spark.stop()
log_to_s3("Spark job completed successfully", context="end_job")
