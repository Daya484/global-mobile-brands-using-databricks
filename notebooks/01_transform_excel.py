# Databricks notebook source
# MAGIC %pip install openpyxl gcsfs fsspec google-cloud-storage

# COMMAND ----------
# MAGIC %md
# MAGIC # 🔄 Stage 1: Transform Excel to CSV
# #
# # Reads Excel files from GCS `landing/` directory, extracts brand sheets, and writes CSV files to `transformed/`.

# COMMAND ----------
import io
import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transform_excel")

# COMMAND ----------
# Define widgets (parameters)
dbutils.widgets.text("env", "dv")
dbutils.widgets.text("source_bucket", "")
dbutils.widgets.text("run_date", "")

env = dbutils.widgets.get("env")
source_bucket_name = dbutils.widgets.get("source_bucket")
run_date = dbutils.widgets.get("run_date")

if not source_bucket_name:
    raise ValueError("Parameter 'source_bucket' is required.")

logger.info(f"Running Transform Excel for Env: {env} | Bucket: {source_bucket_name} | Date: {run_date}")

# COMMAND ----------
# Authentication with GCS using Service Account Key
# PASTE YOUR GCP SERVICE ACCOUNT JSON KEY CONTENT INSIDE THE TRIPLE QUOTES BELOW
gcp_key_json = """"""

try:
    if gcp_key_json.strip():
        info = json.loads(gcp_key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        client = storage.Client(credentials=credentials, project=info.get("project_id"))
        logger.info("Successfully authenticated to GCS using direct JSON key.")
    else:
        logger.warning("gcp_key_json variable is empty. Attempting default credentials.")
        client = storage.Client()
except Exception as e:
    logger.error(f"Authentication failed: {e}. Falling back to default credentials.")
    client = storage.Client()

bucket = client.bucket(source_bucket_name)

# CONFIG
brand_sheets = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]
brand_folder_map = {
    "Samsung": "samsung",
    "Apple": "apple",
    "Oppo": "oppo",
    "Vivo": "vivo",
    "OnePlus": "oneplus"
}
excel_exts = (".xlsx", ".xls", ".xlsm")

# COMMAND ----------
# Helper functions
def list_excel_files(bucket, run_date_yyyymmdd):
    result = []
    prefix = "landing"
    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith("/") or not blob.name.lower().endswith(excel_exts):
            continue
        
        # If run_date is specified, filter files belonging to that run
        if run_date_yyyymmdd and run_date_yyyymmdd not in blob.name:
            continue
            
        result.append(blob.name)
    return result

def process_file(blob_name):
    logger.info(f"Processing Excel file: {blob_name}")
    src_blob = bucket.blob(blob_name)
    
    # Download bytes
    buf = io.BytesIO()
    src_blob.download_to_file(buf)
    buf.seek(0)
    data_bytes = buf.read()
    
    # Parse sheets
    try:
        xls = pd.ExcelFile(io.BytesIO(data_bytes), engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to read Excel file {blob_name}: {e}")
        return 0
        
    uploaded = 0
    csv_name = os.path.basename(blob_name)
    for ext in excel_exts:
        if csv_name.lower().endswith(ext):
            csv_name = csv_name[:-len(ext)] + ".csv"
            break
    
    for brand in brand_sheets:
        if brand not in xls.sheet_names:
            continue
            
        df = xls.parse(brand)
        folder = brand_folder_map[brand]
        
        # Write to GCS transformed directory
        dest_path = f"transformed/{folder}/{csv_name}"
        dest_blob = bucket.blob(dest_path)
        
        # Output DataFrame to CSV string
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        csv_data = csv_buf.getvalue().encode("utf-8")
        
        dest_blob.upload_from_string(csv_data, content_type="text/csv")
        logger.info(f"Successfully transformed and uploaded sheet '{brand}' to {dest_path}")
        uploaded += 1
        
    return uploaded

# COMMAND ----------
# Main execution
run_date_yyyymmdd = run_date.replace("-", "") if run_date else None
files = list_excel_files(bucket, run_date_yyyymmdd)
logger.info(f"Found {len(files)} files to transform.")

total_transformed = 0
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(process_file, f): f for f in files}
    for future in as_completed(futures):
        total_transformed += future.result()

logger.info(f"Transformation complete. Total sheets processed: {total_transformed}")
dbutils.notebook.exit(f"Processed {total_transformed} sheets successfully.")
