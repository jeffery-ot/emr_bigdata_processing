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
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            print(f"Creating bucket: {bucket_name}")
            s3.create_bucket(Bucket=bucket_name)
        else:
            print(f"Unexpected error checking bucket: {e}")
            raise

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
    .getOrCreate()

# ----------------------------------------
# Ensure target bucket and folders
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

# ----------------------------------------
# Create base table: Vehicle and Location Performance Metrics
# ----------------------------------------

df_base_table = df_rentals.join(df_vehicles, on="vehicle_id", how="inner") \
    .select(
        col("total_amount").cast(DecimalType(10, 2)).alias("total_amount"),
        trim(df_rentals["dropoff_location"]).alias("location"),
        trim(df_rentals["rental_id"]).alias("rental_id"),
        df_rentals["vehicle_id"],
        trim(df_vehicles["vehicle_type"]).alias("vehicle_type"),
        col("rental_start_time").cast(TimestampType()).alias("rental_start_time"),
        col("rental_end_time").cast(TimestampType()).alias("rental_end_time")
    ).dropna()

# Calculate rental duration in hours
df_base_table = df_base_table.withColumn(
    "rental_duration_hours",
    ((unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600).cast(DecimalType(10, 2))
)

# ----------------------------------------
# Compute Performance Metrics
# ----------------------------------------

# Location-based metrics
df_location_metrics = df_base_table.groupBy("location") \
    .agg(
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_per_location"),
        count("rental_id").alias("total_transactions_per_location"),
        avg("total_amount").cast(DecimalType(10, 2)).alias("avg_transaction_amount"),
        spark_max("total_amount").cast(DecimalType(10, 2)).alias("max_transaction_amount"),
        spark_min("total_amount").cast(DecimalType(10, 2)).alias("min_transaction_amount"),
        countDistinct("vehicle_id").alias("unique_vehicles_used")
    )

# Vehicle type-based metrics
df_vehicle_metrics = df_base_table.groupBy("vehicle_type") \
    .agg(
        spark_sum("rental_duration_hours").cast(DecimalType(15, 2)).alias("total_rental_duration_hours"),
        spark_sum("total_amount").cast(DecimalType(15, 2)).alias("revenue_by_vehicle_type"),
        avg("rental_duration_hours").cast(DecimalType(10, 2)).alias("avg_rental_duration_hours"),
        count("rental_id").alias("total_rentals_by_vehicle_type")
    )

# Combined metrics table
df_performance_metrics = df_location_metrics.crossJoin(df_vehicle_metrics) \
    .select(
        col("location"),
        col("revenue_per_location"),
        col("total_transactions_per_location"),
        col("avg_transaction_amount"),
        col("max_transaction_amount"),
        col("min_transaction_amount"),
        col("unique_vehicles_used"),
        col("vehicle_type"),
        col("total_rental_duration_hours"),
        col("revenue_by_vehicle_type"),
        col("avg_rental_duration_hours"),
        col("total_rentals_by_vehicle_type")
    )

# ----------------------------------------
# Write tables to organized structure
# ----------------------------------------

# Create Vehicle_Location_Analytics folder structure
base_path = f"s3a://{presentation_bucket}/Vehicle_Location_Analytics"

# Write the base table
df_base_table.write.mode("overwrite").parquet(f"{base_path}/Vehicle_and_Location_Performance_Metrics/")

# Write the performance metrics KPI table
df_performance_metrics.write.mode("overwrite").parquet(f"{base_path}/Performance_Metrics_KPIs/")

print("Successfully created Vehicle and Location Performance Analytics tables:")
print(f"1. Base Table: {base_path}/Vehicle_and_Location_Performance_Metrics/")
print(f"2. KPI Table: {base_path}/Performance_Metrics_KPIs/")

# Show sample data
print("\n=== Sample Base Table Data ===")
df_base_table.show(5)

print("\n=== Sample Performance Metrics KPIs ===")
df_performance_metrics.show(5)

spark.stop()