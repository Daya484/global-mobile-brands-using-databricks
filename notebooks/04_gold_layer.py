# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Stage 4: Gold Layer (Star Schema & Struct Optimization)
# MAGIC 
# MAGIC Reads the Silver Delta table, loads dimension tables from BigQuery, performs a salted join on the calendar dimension to prevent data skew, organizes fields into optimized nested STRUCTs, and overwrites the target Gold BigQuery table.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, lit, current_timestamp, rand, floor,
    regexp_replace, explode, array, struct, coalesce
)

# COMMAND ----------
# Define widgets
dbutils.widgets.text("env", "dv")
dbutils.widgets.text("pipeline_bucket", "")
dbutils.widgets.text("project_id", "")
dbutils.widgets.text("dataset", "mobile_brands")
dbutils.widgets.text("run_id", "")

env = dbutils.widgets.get("env")
pipeline_bucket = dbutils.widgets.get("pipeline_bucket")
project_id = dbutils.widgets.get("project_id")
dataset = dbutils.widgets.get("dataset")
run_id = dbutils.widgets.get("run_id")

if not pipeline_bucket or not project_id:
    raise ValueError("Parameters 'pipeline_bucket' and 'project_id' are required.")

# COMMAND ----------
# Configure GCS and BigQuery Authentication in Spark
# PASTE YOUR GCP SERVICE ACCOUNT JSON KEY CONTENT INSIDE THE TRIPLE QUOTES BELOW
gcp_key_json = """"""

try:
    if gcp_key_json.strip():
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.json.keyfile.data", gcp_key_json)
        
        # Configure BigQuery credentials specifically for the connector
        spark.conf.set("parentProject", project_id)
        spark.conf.set("credentials", gcp_key_json)
        print("GCS and BigQuery Authentication configured using direct JSON key.")
    else:
        print("gcp_key_json variable is empty. Attempting default environment credentials.")
except Exception as e:
    print(f"Fallback to GCP Environment Default Auth / Error: {e}")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "100")

# COMMAND ----------
SALT_BUCKETS = 10

def read_bq_table(table_name):
    # Reads BigQuery dimension tables using the spark bigquery connector
    return spark.read \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{table_name}") \
        .load()

# COMMAND ----------
# Read Silver Delta Table
silver_path = f"gs://{pipeline_bucket}/delta/silver/"
print(f"Reading Silver Delta table from: {silver_path}")
fact_df = spark.read.format("delta").load(silver_path)

# Load BigQuery Dimensions
print(f"Loading dimensions from BigQuery dataset: {project_id}.{dataset}")
dim_calender = read_bq_table("dim_date")
dim_market = read_bq_table("dim_market")
dim_product = read_bq_table("product_v2").cache()
dim_customer = read_bq_table("customer").cache()

# COMMAND ----------
# Salted Join + Struct Transformation
# ── Prepare Fact (add salt and format join date)
f_salted = fact_df \
    .withColumn("join_date", regexp_replace(col("date_reported").cast("string"), "-", "")) \
    .withColumn("salt", floor(rand() * SALT_BUCKETS)) \
    .alias("f")

# ── Prepare Calendar (explode salts to match salted facts)
c_salted = dim_calender \
    .withColumn("join_date", col("date_code").cast("string")) \
    .withColumn("salt_array", array([lit(i) for i in range(SALT_BUCKETS)])) \
    .select("*", explode(col("salt_array")).alias("salt")) \
    .alias("c")

m = dim_market.alias("m")
p = dim_product.alias("p")
c_cust = dim_customer.alias("c_cust")

# COMMAND ----------
# Chain Joins
joined_df = f_salted \
    .join(broadcast(m), col("f.Market_code") == col("m.market_code3"), "left") \
    .join(c_salted, (col("f.join_date") == col("c.join_date")) & (col("f.salt") == col("c.salt")), "left") \
    .join(broadcast(p), (col("f.Brand") == col("p.brand")) & (col("f.EAN_code") == col("p.ean_code")), "left") \
    .join(broadcast(c_cust), col("f.Store_code") == col("c_cust.store_code"), "left")

# COMMAND ----------
# Select Nested Gold Schema (STRUCTs)
gold_df = joined_df.select(
    coalesce(col("f.Brand"), col("p.brand")).alias("tech_brand_name"),
    lit(None).cast("string").alias("brand_segment"),
    col("m.market_name").alias("tech_orga_country_name"),

    struct(
        col("m.market_name").alias("country_name"),
        col("m.market_code2").alias("market_code2"),
        col("m.market_code3").alias("market_code3"),
        lit(None).cast("string").alias("zone_code"),
        lit(None).cast("string").alias("zone_name"),
    ).alias("GEOGRAPHY"),

    struct(
        col("f.date_reported").alias("period_date"),
        col("c.year").alias("year"),
        col("c.month").alias("month"),
        col("c.week_num").alias("week_num"),
        col("c.month_year").alias("month_year"),
        col("c.quarter").alias("quarter"),
        col("c.quarter_year").alias("quarter_year"),
    ).alias("TRANSACTION_DATE"),

    struct(
        col("f.EAN_code").alias("product_code"),
        coalesce(col("f.Brand"), col("p.brand")).alias("brand"),
        col("f.Model").alias("model"),
        col("p.display_specification").alias("display"),
        col("p.processor_chipset").alias("processor"),
        col("p.front_camera").alias("rear_camera"),
        col("p.back_camera").alias("back_camera"),
        col("p.ram_gb").alias("ram"),
        col("p.rom_options").alias("rom"),
        col("p.refresh_rate_hz").alias("refresh_rate"),
        col("p.launch_year").alias("year_of_launch"),
    ).alias("PRODUCT"),

    struct(
        col("f.Distributor_code").alias("distributor_code"),
        col("c_cust.distributor_name").alias("distributor_name"),
        col("f.Retailer_code").alias("retailer_code"),
        col("c_cust.retailer_name").alias("retailer_name"),
        col("f.Store_code").alias("store_code"),
        col("c_cust.store_name").alias("store_name"),
        col("c_cust.channel_mode").alias("channel_mode"),
    ).alias("CUSTOMER"),

    struct(
        col("f.Currency").alias("currency"),
        col("m.hub_currency").alias("hub_currency"),
        lit(None).cast("string").alias("conversion_rate"),
    ).alias("CURRENCY"),

    struct(
        col("f.Price").alias("asp_local"),
        col("f.Sale_units").alias("units_sold"),
        (col("f.Sale_units") * col("f.Price")).alias("sale_value"),
        lit(None).cast("string").alias("total_sale_price_eur_value"),
        col("f.Stock_units").alias("stock_units"),
        (col("f.Stock_units") * col("f.Price")).alias("stock_value"),
        lit(None).cast("string").alias("total_stock_price_eur_value"),
    ).alias("FACT_VALUE"),

    struct(
        col("f.file_name").alias("file_name"),
        col("f.load_timestamp").alias("file_creation_date"),
        current_timestamp().alias("load_timestamp"),
    ).alias("METADATA_TECHNICAL"),
)

# Repartition to optimize write size
gold_df_partitioned = gold_df.repartition(4)

# COMMAND ----------
# Write to BigQuery Table
gold_bq_table = f"{project_id}.{dataset}.gold_brand_daily_v1"
print(f"Writing to BigQuery Table: {gold_bq_table}")

row_count = gold_df_partitioned.count()
print(f"Row count to write: {row_count}")

# Write using the spark bigquery connector in indirect mode
(
    gold_df_partitioned.write
    .format("bigquery")
    .option("table", gold_bq_table)
    .option("writeMethod", "indirect")
    .option("temporaryGcsBucket", pipeline_bucket)
    .mode("overwrite")
    .save()
)

dbutils.notebook.exit(f"Gold table written successfully. Row count: {row_count}")
