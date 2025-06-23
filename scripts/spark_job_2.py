import boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trim, col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, 
    countDistinct, when, unix_timestamp, to_date, date_format, 
    datediff, regexp_replace, split, desc
)
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType, DateType
from botocore.exceptions import ClientError

# ----------------------------------------
# Helpers
# ----------------------------------------

def ensure_s3_bucket(bucket_name):
    """Ensure S3 bucket exists, create if it doesn't"""
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            print(f"Creating bucket: {bucket_name}")
            try:
                # For regions other than us-east-1, need to specify LocationConstraint
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
    """Ensure S3 directory exists by creating a placeholder object if needed"""
    s3 = boto3.client("s3")
    # Check if directory already has objects
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_path, MaxKeys=1)
        if 'Contents' not in response:
            # Create directory by putting an empty object with trailing slash
            print(f"Creating directory: s3://{bucket_name}/{directory_path}")
    except ClientError as e:
        print(f"Error checking directory {directory_path}: {e}")

# ----------------------------------------
# Initialize Spark
# ----------------------------------------

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

print("Spark session initialized successfully")

# ----------------------------------------
# Setup S3 buckets and directories
# ----------------------------------------

presentation_bucket = "lab4-presentation-data"
ensure_s3_bucket(presentation_bucket)

# Ensure required directories exist
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

# ----------------------------------------
# Load existing Table 2 from presentation bucket
# ----------------------------------------

print("Loading existing Table 2 from presentation bucket...")

# Check if Table 2 exists in the current location, if not, look for it in the root
table2_current_path = f"s3a://{presentation_bucket}/User_Transaction_Analytics/User_and_Transaction_Analysis/"
table2_root_path = f"s3a://{presentation_bucket}/User_and_Transaction_Analysis/"

try:
    # Try to load from current organized location first
    df_table2 = spark.read.parquet(table2_current_path)
    print(f"✓ Loaded Table 2 from organized location: {table2_current_path}")
except:
    try:
        # If not found, try loading from root level
        df_table2 = spark.read.parquet(table2_root_path)
        print(f"✓ Loaded Table 2 from root location: {table2_root_path}")
        print("ℹ️  Table will be moved to organized structure after KPI computation")
    except Exception as e:
        print(f"❌ Error: Could not find Table 2 in presentation bucket")
        print(f"   Looked in: {table2_current_path}")
        print(f"   Looked in: {table2_root_path}")
        print(f"   Error: {e}")
        spark.stop()
        raise Exception("Table 2 not found in presentation bucket. Please ensure it exists before running this job.")

print(f"Table 2 loaded successfully with {df_table2.count()} records")

# Display Table 2 schema for verification
print("\n=== Table 2 Schema ===")
df_table2.printSchema()

# Check available columns
available_columns = df_table2.columns
print(f"Available columns: {available_columns}")

# ----------------------------------------
# Data Preparation and Cleaning
# ----------------------------------------

print("Preparing data for analysis...")

# Clean and prepare the data
df_cleaned = df_table2

# Check if we have date columns and prepare them
date_columns = [col for col in available_columns if 'date' in col.lower() or 'time' in col.lower()]
print(f"Date-related columns found: {date_columns}")

# If we have pickup_date and return_date, calculate rental duration
if 'pickup_date' in available_columns and 'return_date' in available_columns:
    df_cleaned = df_cleaned.withColumn(
        "rental_duration_days",
        datediff(col("return_date"), col("pickup_date"))
    )
    
    # Extract date for daily aggregations (use pickup_date as transaction date)
    df_cleaned = df_cleaned.withColumn(
        "transaction_date",
        to_date(col("pickup_date"))
    )
    print("✓ Calculated rental duration and transaction date")
elif 'pickup_date' in available_columns:
    # If only pickup_date is available
    df_cleaned = df_cleaned.withColumn(
        "transaction_date",
        to_date(col("pickup_date"))
    )
    print("✓ Using pickup_date as transaction date")

# Ensure total_amount is properly typed
if 'total_amount' in available_columns:
    df_cleaned = df_cleaned.withColumn(
        "total_amount",
        col("total_amount").cast(DecimalType(15, 2))
    )

print("Data preparation completed")

# ----------------------------------------
# Compute KPI Metrics (Only Requested Metrics)
# ----------------------------------------

print("Computing User and Transaction Analysis KPIs...")

# 1. Daily Metrics: Total transactions per day & Revenue per day
if 'transaction_date' in df_cleaned.columns:
    df_daily_metrics = df_cleaned.groupBy("transaction_date") \
        .agg(
            count("rental_id").alias("total_transactions_per_day"),
            spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_per_day")
        ) \
        .orderBy("transaction_date")
    
    print("✓ Daily metrics computed")
else:
    print("⚠️  No date column available for daily metrics")
    df_daily_metrics = None

# 2. User-specific spending metrics
df_user_spending_metrics = df_cleaned.groupBy("user_id") \
    .agg(
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("user_total_spending")
    )

print("✓ User spending metrics computed")

# 3. User-specific rental duration metrics (if available)
if 'rental_duration_days' in df_cleaned.columns:
    df_user_duration_metrics = df_cleaned.groupBy("user_id") \
        .agg(
            avg("rental_duration_days").cast(DecimalType(10, 2)).alias("user_avg_rental_duration"),
            spark_sum("rental_duration_days").alias("user_total_rental_duration")
        )
    print("✓ User rental duration metrics computed")
else:
    print("⚠️  No rental duration available - skipping duration metrics")
    df_user_duration_metrics = None

# 4. Maximum and minimum transaction amounts (overall)
df_transaction_amount_metrics = df_cleaned.agg(
    spark_max("total_amount").cast(DecimalType(10, 2)).alias("max_transaction_amount"),
    spark_min("total_amount").cast(DecimalType(10, 2)).alias("min_transaction_amount")
)

print("✓ Transaction amount metrics computed")

print("KPI metrics computed successfully")

# ----------------------------------------
# Write tables to S3
# ----------------------------------------

base_path = f"s3a://{presentation_bucket}/User_Transaction_Analytics"

print("Writing tables to S3...")

# Write/Move Table 2 to organized structure (Base table for Glue crawling)
table2_path = f"{base_path}/User_and_Transaction_Analysis/"
df_cleaned.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(table2_path)
print(f"✓ Table 2 organized and written to: {table2_path}")

# Write Daily Metrics KPIs
if df_daily_metrics is not None:
    daily_kpi_path = f"{base_path}/Transaction_Metrics_KPIs/Daily_Metrics/"
    df_daily_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(daily_kpi_path)
    print(f"✓ Daily Metrics KPIs written to: {daily_kpi_path}")

# Write User Spending Metrics KPIs
user_spending_kpi_path = f"{base_path}/Transaction_Metrics_KPIs/User_Spending_Metrics/"
df_user_spending_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(user_spending_kpi_path)
print(f"✓ User Spending Metrics KPIs written to: {user_spending_kpi_path}")

# Write User Duration Metrics KPIs (if available)
if df_user_duration_metrics is not None:
    user_duration_kpi_path = f"{base_path}/Transaction_Metrics_KPIs/User_Duration_Metrics/"
    df_user_duration_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(user_duration_kpi_path)
    print(f"✓ User Duration Metrics KPIs written to: {user_duration_kpi_path}")

# Write Transaction Amount Metrics KPIs
amount_kpi_path = f"{base_path}/Transaction_Metrics_KPIs/Transaction_Amount_Metrics/"
df_transaction_amount_metrics.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(amount_kpi_path)
print(f"✓ Transaction Amount Metrics KPIs written to: {amount_kpi_path}")

# ----------------------------------------
# Display sample results
# ----------------------------------------

print("\n" + "="*60)
print("SAMPLE DATA PREVIEW")
print("="*60)

print("\n=== Table 2: User and Transaction Analysis (Sample) ===")
df_cleaned.show(5, truncate=False)

if df_daily_metrics is not None:
    print("\n=== Daily Transaction Metrics KPIs ===")
    df_daily_metrics.orderBy(desc("transaction_date")).show(10, truncate=False)

print("\n=== User Spending Metrics KPIs (Top 10) ===")
df_user_spending_metrics.orderBy(desc("user_total_spending")).show(10, truncate=False)

if df_user_duration_metrics is not None:
    print("\n=== User Duration Metrics KPIs (Top 10) ===")
    df_user_duration_metrics.orderBy(desc("user_total_rental_duration")).show(10, truncate=False)

print("\n=== Transaction Amount Metrics (Max/Min) ===")
df_transaction_amount_metrics.show(truncate=False)

# ----------------------------------------
# Summary Statistics
# ----------------------------------------

print("\n" + "="*60)
print("JOB SUMMARY")
print("="*60)

total_records_table2 = df_cleaned.count()
total_users_spending = df_user_spending_metrics.count()

print(f"✓ Table 2 Records: {total_records_table2:,}")
print(f"✓ Users with Spending Data: {total_users_spending:,}")

if df_daily_metrics is not None:
    total_days = df_daily_metrics.count()
    print(f"✓ Days with Transactions: {total_days}")

if df_user_duration_metrics is not None:
    total_users_duration = df_user_duration_metrics.count()
    print(f"✓ Users with Duration Data: {total_users_duration:,}")

# Get transaction amount metrics
amount_metrics = df_transaction_amount_metrics.collect()[0]
max_transaction = amount_metrics["max_transaction_amount"]
min_transaction = amount_metrics["min_transaction_amount"]

print(f"✓ Max Transaction Amount: ${max_transaction:.2f}")
print(f"✓ Min Transaction Amount: ${min_transaction:.2f}")

# Summary of total spending and revenue
total_revenue = df_user_spending_metrics.agg(spark_sum("user_total_spending")).collect()[0][0]
print(f"✓ Total Revenue (from user spending): ${total_revenue:,.2f}")

print(f"\n✅ All tables successfully written to s3://{presentation_bucket}/User_Transaction_Analytics/")
print("📋 Ready for AWS Glue Data Catalog crawling!")

# ----------------------------------------
# Cleanup
# ----------------------------------------

spark.stop()
print("\n🎉 Spark job completed successfully!")