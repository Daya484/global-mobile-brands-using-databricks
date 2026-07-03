# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Stage 3: Silver Layer (Deduplication and Cleansing)
# MAGIC 
# MAGIC Reads Bronze Delta data, derives the market code, casts data types to Silver specifications, deduplicates data using the business keys (keeping the newest timestamp), and writes to the consolidated Silver Delta table.

# COMMAND ----------
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# COMMAND ----------
# Define widgets
dbutils.widgets.text("env", "dv")
dbutils.widgets.text("pipeline_bucket", "")
dbutils.widgets.text("dt", "") # Optional partition date YYYY-MM-DD
dbutils.widgets.text("run_id", "")

env = dbutils.widgets.get("env")
pipeline_bucket = dbutils.widgets.get("pipeline_bucket")
dt = dbutils.widgets.get("dt")
run_id = dbutils.widgets.get("run_id")

if not pipeline_bucket:
    raise ValueError("Parameter 'pipeline_bucket' is required.")

# COMMAND ----------
# MAGIC %run ./Credentials

# COMMAND ----------
# Configure GCS Authentication in Spark
try:
    if 'gcp_key_json' in locals() and gcp_key_json.strip():
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.json.keyfile.data", gcp_key_json)
        print("GCS Authentication configured using direct JSON key.")
    else:
        print("gcp_key_json variable is empty or not defined. Attempting default environment credentials.")
except Exception as e:
    print(f"GCS Auth fallback / Error: {e}")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------
# BUSINESS KEY & SCHEMA DEFINITION
BUSINESS_KEY_COLS = [
    "Market_code",
    "Distributor_code",
    "Retailer_code",
    "Store_code",
    "EAN_code",
    "date_reported",
]

SILVER_SCHEMA = StructType([
    StructField("Market_code",      StringType(),    False),
    StructField("Brand",            StringType(),    True),
    StructField("Model",            StringType(),    True),
    StructField("Distributor_code", StringType(),    False),
    StructField("Retailer_code",    StringType(),    True),
    StructField("Store_code",       StringType(),    True),
    StructField("EAN_code",         LongType(),      False),
    StructField("Currency",         StringType(),    True),
    StructField("Price",            IntegerType(),   True),
    StructField("Stock_units",      IntegerType(),   True),
    StructField("Sale_units",       IntegerType(),   True),
    StructField("date_reported",    DateType(),      False),
    StructField("file_name",        StringType(),    False),
    StructField("load_timestamp",   TimestampType(), False),
])

BRAND_FOLDERS = ["apple", "samsung", "oppo", "vivo", "oneplus"]

# COMMAND ----------
def read_bronze_delta(pipeline_bucket_name, date_filter):
    base = f"gs://{pipeline_bucket_name}/delta/bronze/"
    dfs = []
    
    for brand in BRAND_FOLDERS:
        brand_path = f"{base}{brand}/"
        print(f"Reading Bronze Delta for brand={brand} at {brand_path}")
        
        try:
            df = spark.read.format("delta").load(brand_path)
            
            # If date_filter is specified, read only that partition
            if date_filter:
                df = df.filter(F.col("date_reported") == F.lit(date_filter).cast("date"))
                
            dfs.append(df)
        except Exception as exc:
            print(f"Skipping brand={brand} (no data found): {exc}")
            
    if not dfs:
        return None
        
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)
        
    return final_df

# COMMAND ----------
# Read data
df_bronze = read_bronze_delta(pipeline_bucket, dt)

if df_bronze is None or df_bronze.rdd.isEmpty():
    raise RuntimeError(f"No Bronze data found to process for date: {dt or 'all'}")

print(f"Read {df_bronze.count()} raw rows from Bronze.")

# COMMAND ----------
# Deduplicate and Cleanse
# 1) Derive Market_code from first 3 characters of Distributor_code
df_market = df_bronze.withColumn("Market_code", F.substring(F.col("Distributor_code"), 1, 3))

# 2) Cast columns to silver schema
df_casted = df_market.select(*[
    F.col(f.name).cast(f.dataType).alias(f.name)
    for f in SILVER_SCHEMA.fields
])

# 3) Window Deduplication - Keep the record with latest load timestamp and filename alphabetically
window_spec = Window \
    .partitionBy(*BUSINESS_KEY_COLS) \
    .orderBy(F.col("load_timestamp").desc(), F.col("file_name").desc())

df_deduped = df_casted \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

# Repartition to optimize write layout
df_output = df_deduped.repartition(8)

# COMMAND ----------
# Write to Silver Delta Table
silver_path = f"gs://{pipeline_bucket}/delta/silver/"
print(f"Writing deduped data to Silver Delta Table: {silver_path}")

df_output.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date_reported") \
    .save(silver_path)

dbutils.notebook.exit("Silver layer processing completed successfully.")
