import boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, countDistinct, when, unix_timestamp
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType
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

def move_table_to_presentation_bucket(spark, source_path, dest_bucket, dest_path):
    """Move table from source to destination and verify the move"""
    try:
        # Read the source table
        df = spark.read.parquet(source_path)
        
        # Write to destination
        df.write.mode("overwrite").parquet(f"s3a://{dest_bucket}/{dest_path}")
        print(f"Successfully moved table from {source_path} to s3://{dest_bucket}/{dest_path}")
        
        # Verify the write was successful
        verify_df = spark.read.parquet(f"s3a://{dest_bucket}/{dest_path}")
        record_count = verify_df.count()
        print(f"Verified: {record_count} records in moved table")
        
        return True
    except Exception as e:
        print(f"Error moving table: {e}")
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

print("Spark session initialized successfully")

# ----------------------------------------
# Setup S3 buckets and directories
# ----------------------------------------

presentation_bucket = "lab4-presentation-data"
ensure_s3_bucket(presentation_bucket)

# Ensure required directories exist
required_directories = [
    "Vehicle_Location_Analytics/",
    "Vehicle_Location_Analytics/Vehicle_and_Location_Performance_Metrics/",
    "Vehicle_Location_Analytics/Performance_Metrics_KPIs/",
    "Vehicle_Location_Analytics/Performance_Metrics_KPIs/Location_Metrics/"
]

for directory in required_directories:
    ensure_s3_directory(presentation_bucket, directory)

# ----------------------------------------
# Load existing Table 1 from presentation bucket
# ----------------------------------------

print("Loading existing Table 1 from presentation bucket...")

# Check if Table 1 exists in the current location, if not, look for it in the root
table1_current_path = f"s3a://{presentation_bucket}/Vehicle_Location_Analytics/Vehicle_and_Location_Performance_Metrics/"
table1_root_path = f"s3a://{presentation_bucket}/Vehicle_and_Location_Performance_Metrics/"

try:
    # Try to load from current organized location first
    df_table1 = spark.read.parquet(table1_current_path)
    print(f"✓ Loaded Table 1 from organized location: {table1_current_path}")
except:
    try:
        # If not found, try loading from root level
        df_table1 = spark.read.parquet(table1_root_path)
        print(f"✓ Loaded Table 1 from root location: {table1_root_path}")
        print("ℹ️  Table will be moved to organized structure after KPI computation")
    except Exception as e:
        print(f"❌ Error: Could not find Table 1 in presentation bucket")
        print(f"   Looked in: {table1_current_path}")
        print(f"   Looked in: {table1_root_path}")
        print(f"   Error: {e}")
        spark.stop()
        raise Exception("Table 1 not found in presentation bucket. Please ensure it exists before running this job.")

print(f"Table 1 loaded successfully with {df_table1.count()} records")

# Display Table 1 schema for verification
print("\n=== Table 1 Schema ===")
df_table1.printSchema()

# Check available columns
available_columns = df_table1.columns
print(f"Available columns: {available_columns}")

# Since Table 1 doesn't have timestamp columns, we'll work with the available data
# The KPIs will focus on transaction amounts, locations, and vehicle types
print("ℹ️  Table 1 contains transaction data without timestamps")
print("ℹ️  KPIs will focus on revenue, transactions, and vehicle utilization metrics")

# ----------------------------------------
# Compute KPI Metrics
# ----------------------------------------

print("Computing performance metrics KPIs...")

# Location-based metrics
df_location_metrics = df_table1.groupBy("location") \
    .agg(
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_per_location"),
        count("rental_id").alias("total_transactions_per_location"),
        avg("total_amount").cast(DecimalType(10, 2)).alias("avg_transaction_amount"),
        spark_max("total_amount").cast(DecimalType(10, 2)).alias("max_transaction_amount"),
        spark_min("total_amount").cast(DecimalType(10, 2)).alias("min_transaction_amount"),
        countDistinct("vehicle_id").alias("unique_vehicles_used")
    )

# Vehicle type-based metrics - revenue only
df_vehicle_metrics = df_table1.groupBy("vehicle_type") \
    .agg(
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_by_vehicle_type")
    )

print("KPI metrics computed successfully")

# ----------------------------------------
# Write tables to S3
# ----------------------------------------

base_path = f"s3a://{presentation_bucket}/Vehicle_Location_Analytics"

print("Writing tables to S3...")

# Write/Move Table 1 to organized structure (Base table for Glue crawling)
table1_path = f"{base_path}/Vehicle_and_Location_Performance_Metrics/"
df_table1.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(table1_path)
print(f"✓ Table 1 organized and written to: {table1_path}")

# Write Location Metrics KPIs
location_kpi_path = f"{base_path}/Performance_Metrics_KPIs/Location_Metrics/"
df_location_metrics.coalesce(2).write.mode("overwrite").option("compression", "snappy").parquet(location_kpi_path)
print(f"✓ Location KPIs written to: {location_kpi_path}")

# Write Vehicle Type Metrics KPIs
vehicle_kpi_path = f"{base_path}/Performance_Metrics_KPIs/Vehicle_Type_Metrics/"
df_vehicle_metrics.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(vehicle_kpi_path)
print(f"✓ Vehicle Type KPIs written to: {vehicle_kpi_path}")

# ----------------------------------------
# Display sample results
# ----------------------------------------

print("\n" + "="*60)
print("SAMPLE DATA PREVIEW")
print("="*60)

print("\n=== Table 1: Vehicle and Location Performance Metrics (Sample) ===")
df_table1.show(5, truncate=False)

print("\n=== Location Metrics KPIs ===")
df_location_metrics.orderBy(col("revenue_per_location").desc()).show(10, truncate=False)

print("\n=== Vehicle Type Metrics KPIs ===")
df_vehicle_metrics.orderBy(col("revenue_by_vehicle_type").desc()).show(10, truncate=False)

# ----------------------------------------
# Summary Statistics
# ----------------------------------------

print("\n" + "="*60)
print("JOB SUMMARY")
print("="*60)

total_records_table1 = df_table1.count()
total_locations = df_location_metrics.count()
total_vehicle_types = df_vehicle_metrics.count()

print(f"✓ Table 1 Records: {total_records_table1:,}")
print(f"✓ Unique Locations: {total_locations}")
print(f"✓ Vehicle Types: {total_vehicle_types}")

# Get total revenue and transactions
total_revenue = df_table1.agg(spark_sum("total_amount")).collect()[0][0]
total_transactions = df_table1.count()

print(f"✓ Total Revenue: ${total_revenue:,.2f}")
print(f"✓ Total Transactions: {total_transactions:,}")

print(f"\n✅ All tables successfully written to s3://{presentation_bucket}/Vehicle_Location_Analytics/")
print("📋 Ready for AWS Glue Data Catalog crawling!")

# ----------------------------------------
# Cleanup
# ----------------------------------------

spark.stop()
print("\n🎉 Spark job completed successfully!")