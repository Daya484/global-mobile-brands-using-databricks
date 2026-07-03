# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Stage 2: Bronze Layer (Ingest CSV to Delta)
# MAGIC 
# MAGIC Reads transformed CSV files from GCS, adds metadata columns (ingestion time, file name, report date), enforces types, and appends to a Delta table partitioned by date.

# COMMAND ----------
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType
)

# COMMAND ----------
# Define widgets
dbutils.widgets.text("env", "dv")
dbutils.widgets.text("source_bucket", "")
dbutils.widgets.text("pipeline_bucket", "")
dbutils.widgets.text("run_id", "")

env = dbutils.widgets.get("env")
source_bucket = dbutils.widgets.get("source_bucket")
pipeline_bucket = dbutils.widgets.get("pipeline_bucket")
run_id = dbutils.widgets.get("run_id")

if not source_bucket or not pipeline_bucket:
    raise ValueError("Parameters 'source_bucket' and 'pipeline_bucket' are required.")

# COMMAND ----------
# MAGIC %run ./Credentials

# COMMAND ----------
# Configure GCS Authentication in Spark
try:
    if 'gcp_key_json' in locals() and gcp_key_json.strip():
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.json.keyfile.data", gcp_key_json)
        print("GCS Authentication configured in Spark Session using direct JSON key.")
    else:
        print("gcp_key_json variable is empty or not defined. Attempting default environment credentials.")
except Exception as e:
    print(f"Using default GCP environment authentication / Error: {e}")

# Enable dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------
# SCHEMA DEFINITIONS
BRONZE_SCHEMA = StructType([
    StructField("Brand",            StringType(),    True),
    StructField("Model",            StringType(),    True),
    StructField("Distributor_code", StringType(),    True),
    StructField("Retailer_code",    StringType(),    True),
    StructField("Store_code",       StringType(),    True),
    StructField("EAN_code",         LongType(),      True),
    StructField("Currency",         StringType(),    True),
    StructField("Price",            IntegerType(),   True),
    StructField("Stock_units",      IntegerType(),   True),
    StructField("Sale_units",       IntegerType(),   True),
    StructField("date_reported",    DateType(),      False),
    StructField("file_name",        StringType(),    False),
    StructField("load_timestamp",   TimestampType(), False),
])

BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]

# COMMAND ----------
def process_brand(brand):
    src = f"gs://{source_bucket}/transformed/{brand.lower()}/*.csv"
    dest = f"gs://{pipeline_bucket}/delta/bronze/{brand.lower()}/"
    
    print(f"Reading CSVs from: {src}")
    
    try:
        df = spark.read.option("header", "true").csv(src)
    except Exception as exc:
        print(f"No files found for brand={brand}: {exc}")
        return 0
        
    if df.rdd.isEmpty():
        print(f"Empty dataset for brand={brand}")
        return 0
        
    # Extract file name and reporting date
    df_with_meta = df \
        .withColumn("file_name", F.substring_index(F.input_file_name(), "/", -1)) \
        .withColumn("date_reported", F.to_date(
            F.substring_index(F.substring_index(F.col("file_name"), "_", -1), ".", 1),
            "yyyyMMdd"
        )) \
        .withColumn("load_timestamp", F.current_timestamp())
        
    # Enforce Schema
    df_bronze = df_with_meta.select(*[
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in BRONZE_SCHEMA.fields
    ])
    
    # Write to Bronze (using DELTA format instead of Parquet)
    count = df_bronze.count()
    
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date_reported") \
        .save(dest)
        
    print(f"Successfully processed brand={brand} | written {count} rows to {dest}")
    return count

# COMMAND ----------
total_rows = 0
for brand in BRANDS:
    try:
        total_rows += process_brand(brand)
    except Exception as exc:
        raise RuntimeError(f"Bronze processing failed for brand={brand}: {exc}")

dbutils.notebook.exit(f"Bronze processing completed. Total rows: {total_rows}")
