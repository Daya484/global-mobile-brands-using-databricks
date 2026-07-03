# 🧱 Databricks Migration — Mobile Brands Pipeline

This directory contains the complete implementation for running the **Mobile Brands Pipeline** in **Databricks**.

---

## 1. Directory Structure

```
databricks_pipeline/
├── README.md                 ← This guide
├── notebooks/
│   ├── 00_generate_excel.py   ← Python: Generates mock distributor Excel files to GCS
│   ├── 01_transform_excel.py  ← Python: Reads GCS Excel files, extracts sheets, writes CSVs
│   ├── 02_bronze_layer.ipynb  ← PySpark: Reads CSVs, validates, writes Delta Bronze
│   ├── 03_silver_layer.ipynb  ← PySpark: Deduplicates and standardizes to Delta Silver
│   ├── 04_gold_layer.ipynb    ← PySpark: Joins with BigQuery dims, writes Gold layer
│   └── 05_archival.py         ← Python/DBUtils: Archives processed Excel files
└── workflows/
    └── workflow_definition.json ← JSON import definition for Databricks Workflow
```

---

## 2. Setting Up GCS Authentication

To allow Databricks to read/write to your GCP buckets (`mb-data-pd-495516` & `mb-pipeline-pd-495516`), copy the content of your downloaded service account JSON key file and configure it in the notebooks:

### Step 2.1: Open your Service Account Key File
Open the downloaded service account `.json` key file in a text editor (such as Notepad or VS Code) and copy the entire text.

### Step 2.2: Add key to Notebooks
Each notebook has a dedicated authentication section at the top. Paste your JSON key content inside the `gcp_key_json` variable:

```python
# Configure this block at the top of each notebook:
gcp_key_json = """
{
  "type": "service_account",
  "project_id": "pd-env-495516",
  ... (Paste your JSON content here)
}
"""
```

---

## 3. How to Deploy the Databricks Workflow (Paid Tiers)

If using a paid Databricks environment:
1. Go to your **Databricks Workspace**.
2. Navigate to **Workflows** > **Jobs** > **Create Job**.
3. Import the tasks using the JSON provided in `workflows/workflow_definition.json`.

*Note: For the free Community/Free Edition, the Workflows tab is disabled. You must run the notebooks manually in sequence: 01_transform_excel ➔ 02_bronze_layer ➔ 03_silver_layer ➔ 04_gold_layer ➔ 05_archival.*

---

## 4. Notebook Parameters (Widgets)

Each notebook uses **Databricks Widgets** to receive runtime parameters. When running the notebooks manually in the Free Edition, fill in these values in the top widgets bar:

* `env`: Deployment environment (`dv` or `pd`).
* `source_bucket`: GCS Bucket holding input files (e.g. `mb-data-pd-495516`).
* `pipeline_bucket`: GCS Bucket holding pipeline data (e.g. `mb-pipeline-pd-495516`).
* `run_id`: Unique run identifier (can be any string for manual runs).
* `run_date`: Target processing date (formatted as `YYYY-MM-DD`).
