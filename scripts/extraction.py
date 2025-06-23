import boto3
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
    print(f"Processing {name}...")
    input_path = cfg["input"]
    output_path = cfg["output"]
    selected_columns = cfg["columns"]

    # Extract bucket and prefix
    bucket_name = output_path.replace("s3a://", "").split("/")[0]
    prefix = "/".join(output_path.replace("s3a://", "").split("/")[1:])

    # Ensure output folder exists
    ensure_s3_folder(bucket_name, prefix)

    # Load data
    df = spark.read.option("header", True).csv(input_path)

    # Select only necessary columns (handle missing with try/except if needed)
    df = df.select([trim(col(c)).alias(c) for c in selected_columns])

    # Basic cleaning: drop nulls
    df_cleaned = df.dropna()

    # Write cleaned data to curated S3 path in Parquet format
    df_cleaned.write.mode("overwrite").parquet(output_path)

print("ETL process completed.")
spark.stop()
