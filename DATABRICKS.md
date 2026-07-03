# 🧱 Google Cloud Databricks — Mobile Brands Pipeline

This guide explains the architecture, configuration steps, and business benefits of running the Mobile Brands data engineering pipeline on **Databricks** instead of a GCP-native stack (Cloud Run + Dataproc + Airflow).

---

## 1. ❓ What is Databricks?
**Databricks** is a unified, cloud-based data analytics platform built on top of **Apache Spark**. It introduces the **Lakehouse Architecture**, combining the raw scalability and low cost of data lakes (like GCS) with the structure, ACID transactions, and query speed of traditional data warehouses (like BigQuery).

---

## 2. 🎯 Purpose & Why We Use It
In your original GCP pipeline, code was fragmented across separate cloud services:
* **Cloud Run** executed lightweight Python scripts for Excel extraction and archival.
* **Dataproc Serverless** executed PySpark jobs for data cleaning and BigQuery ingestion.
* **Cloud Composer (Airflow)** orchestrated the execution flow.

**Databricks consolidates all these steps** into a single platform:
1. **Interactive Notebooks:** You write, run, and debug Python and Spark code in the same collaborative interface. No Docker containers or Cloud Build deployments needed.
2. **Unified Compute:** The exact same compute engine runs lightweight pandas operations and heavy-duty multi-node Spark operations.
3. **Delta Lake Format:** Standardizes all Bronze, Silver, and Gold data layers on **Delta Lake**, which natively supports rollback, schema verification, and file optimization.
4. **Built-in Scheduling:** Databricks Workflows handles DAG orchestration, eliminating the high idle costs associated with a dedicated Cloud Composer (Airflow) instance.

---

## 3. 🛠️ How to Implement & Configure (Step-by-Step)

Follow these steps to set up the pipeline on **Databricks Free/Community Edition**:

### Step 3.1: Link the Repository
1. In your Databricks Workspace, go to **Workspace** ➔ **Repos** ➔ **Add Repo**.
2. Paste your repository URL: `https://github.com/Daya484/global-mobile-brands-using-databricks.git`
3. Click **Create Repo**.

### Step 3.2: Configure GCP Service Account Credentials Locally
Because your private keys should **never** be checked into GitHub, we use a local workspace notebook called `Credentials` to store them safely:

1. Inside your Databricks Workspace Repos folder, navigate to the `notebooks/` directory.
2. Click **Create** ➔ **Notebook** and name it **`Credentials`**.
3. In the first cell of the `Credentials` notebook, paste your service account JSON key:
   ```python
   # Inside notebooks/Credentials
   gcp_key_json = """{
     "type": "service_account",
     "project_id": "pd-env-495516",
     "private_key_id": "82d3ba1e2e7f46195a1d00ebcb4852aa677922e7",
     "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
     "client_email": "mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com",
     ...
   }"""
   ```
*(This file is configured in `.gitignore` so it will remain strictly local to your workspace and will never be pushed back to GitHub).*

### Step 3.3: Run the Pipeline Notebooks Manually (Free Edition)
In the Free Edition, the Workflow scheduler is disabled. Run the notebooks manually in this exact order:

1. **`00_generate_excel`**: Generates mock sales Excel files and uploads them to GCS `landing/`.
2. **`01_transform_excel`**: Extracts the brand sheets to CSV files in GCS `transformed/`.
3. **`02_bronze_layer`**: Enforces schemas and saves the records as Delta tables in GCS `delta/bronze/`.
4. **`03_silver_layer`**: Deduplicates records and merges them into the `delta/silver/` Delta table.
5. **`04_gold_layer`**: Joins the data with BigQuery dimensions (salted join) and writes the results to BigQuery.
6. **`05_archival`**: Cleans up GCS landing and transformed folders, moving processed files todated archive folders.

---

## 4. ⚡ How It is Helpful (Key Benefits)

* **₹0 Platform Cost:** Using the Databricks Free/Community Edition and GCP Free Trial keeps your data engineering costs completely at zero.
* **No Docker/Registry Management:** You do not need to configure Dockerfiles, submit container builds via Cloud Build, or manage Artifact Registry images. Any code change is live in Databricks instantly.
* **Zero Airflow Management:** You avoid the setup, update, and high running costs of Apache Airflow (Composer) by running notebooks directly or utilizing Databricks Workflows.
* **Delta Lake Features:** Enables **Time Travel** (querying historical versions of your data) and **ACID Transactions** (preventing data corruption during write failures).
