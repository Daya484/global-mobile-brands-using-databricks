# Databricks notebook source
# MAGIC %md
# MAGIC # 📦 Stage 5: Archival (Landing & Transformed Files)
# MAGIC 
# MAGIC Moves processed files from GCS `landing/` and `transformed/` directories into dated archive folders (`archive_landing/YYYY-MM-DD/` and `archive_transformed/YYYY-MM-DD/`).

# COMMAND ----------
import os
import json
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("archival")

# COMMAND ----------
# Define widgets
dbutils.widgets.text("env", "dv")
dbutils.widgets.text("source_bucket", "")
dbutils.widgets.text("run_date", "")

env = dbutils.widgets.get("env")
source_bucket_name = dbutils.widgets.get("source_bucket")
run_date = dbutils.widgets.get("run_date")

if not source_bucket_name:
    raise ValueError("Parameter 'source_bucket' is required.")

archive_date = run_date if run_date else date.today().strftime("%Y-%m-%d")
run_date_yyyymmdd = archive_date.replace("-", "")

logger.info(f"Archival Started for Env: {env} | Bucket: {source_bucket_name} | Date: {archive_date}")

# COMMAND ----------
# MAGIC %run ./Credentials

# COMMAND ----------
# Initialize GCS client using Service Account Key
try:
    if 'gcp_key_json' in locals() and gcp_key_json.strip():
        info = json.loads(gcp_key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        client = storage.Client(credentials=credentials, project=info.get("project_id"))
        logger.info("Successfully authenticated GCS storage client using direct JSON key.")
    else:
        logger.warning("gcp_key_json variable is empty or not defined. Attempting default credentials.")
        client = storage.Client()
except Exception as e:
    logger.error(f"Authentication failed: {e}. Falling back to default credentials.")
    client = storage.Client()

bucket = client.bucket(source_bucket_name)

# COMMAND ----------
# HELPER FUNCTIONS
def list_files_to_archive(prefix, extensions):
    files = []
    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith("/"):
            continue
        if not any(blob.name.lower().endswith(ext) for ext in extensions):
            continue
        # Filter files corresponding to the specific run date in the file name
        if run_date_yyyymmdd and run_date_yyyymmdd not in blob.name:
            continue
        files.append(blob.name)
    return files

def move_file(src_path, dest_path):
    try:
        dest_blob = bucket.blob(dest_path)
        if dest_blob.exists():
            logger.info(f"Skip (destination exists): {src_path} -> {dest_path}")
            return True
            
        src_blob = bucket.blob(src_path)
        bucket.copy_blob(src_blob, bucket, dest_path)
        src_blob.delete()
        logger.info(f"Moved: {src_path} -> {dest_path}")
        return True
    except Exception as exc:
        logger.error(f"Error moving {src_path} -> {dest_path}: {exc}")
        return False

# COMMAND ----------
# LANDING FILES MIGRATION
excel_exts = (".xlsx", ".xls", ".xlsm")
landing_files = list_files_to_archive("landing", excel_exts)
logger.info(f"Found {len(landing_files)} landing files to archive.")

errors = []

def process_landing_file(src):
    parts = src.split("/")
    if len(parts) < 3:
        return
    country = parts[1]
    file_name = parts[-1]
    dest = f"archive_landing/{archive_date}/{country}/{file_name}"
    
    success = move_file(src, dest)
    if not success:
        errors.append(src)

# TRANSFORMED FILES MIGRATION
csv_exts = (".csv",)
transformed_files = list_files_to_archive("transformed", csv_exts)
logger.info(f"Found {len(transformed_files)} transformed files to archive.")

def process_transformed_file(src):
    parts = src.split("/")
    if len(parts) < 3:
        return
    brand = parts[1]
    file_name = parts[-1]
    dest = f"archive_transformed/{archive_date}/{brand}/{file_name}"
    
    success = move_file(src, dest)
    if not success:
        errors.append(src)

# COMMAND ----------
# Run Parallel Moves
max_workers = 16

logger.info("Archiving Landing...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(process_landing_file, landing_files)

logger.info("Archiving Transformed...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(process_transformed_file, transformed_files)

# COMMAND ----------
# Final summary
logger.info(f"Archival complete. Errors encountered: {len(errors)}")

if errors:
    raise RuntimeError(f"Archival completed with errors for files: {errors}")

dbutils.notebook.exit("Archival completed successfully.")
