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

---

## 5. 🏅 Real-Time Medallion Architecture — Bronze, Silver & Gold Layers

This pipeline is **implemented as a real-time, production-grade data engineering project** following the **Delta Lakehouse Medallion Architecture**. Each layer mirrors what a real enterprise data team ships at scale — with production-quality patterns including schema enforcement, ACID transactions, incremental partitioning, business-key deduplication, salted joins, and nested analytical schemas.

Data flows in three progressively refined quality stages, each persisted as a **Delta Lake table** on GCS:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  REAL-TIME DATA FLOW (PRODUCTION)                   │
  │                                                                     │
  │  📥 GCS landing/        Raw Excel files per brand per day           │
  │         │                                                           │
  │         ▼  [01_transform_excel — per-brand sheet extraction]        │
  │  📂 GCS transformed/    Clean per-brand CSVs                        │
  │         │                                                           │
  │         ▼  [02_bronze_layer — schema enforcement + metadata]        │
  │  🥉 BRONZE              gs://<bucket>/delta/bronze/<brand>/         │
  │         │                ACID append · partitioned by date          │
  │         │                                                           │
  │         ▼  [03_silver_layer — dedup + market derivation]            │
  │  🥈 SILVER              gs://<bucket>/delta/silver/                 │
  │         │                ACID overwrite · deduplicated              │
  │         │                                                           │
  │         ▼  [04_gold_layer — Star Schema + BQ dimensions join]       │
  │  🥇 GOLD                BigQuery: mobile_brands.gold_brand_daily_v1 │
  │                          Nested STRUCTs · analytical-ready          │
  └─────────────────────────────────────────────────────────────────────┘
```

> **Why this is production-grade:** Every layer uses Delta Lake's ACID guarantees — if a write fails mid-run, no partial data is committed. Each run is idempotent: Bronze appends only new files, Silver uses dynamic partition overwrite (only the processed date is replaced), and Gold fully overwrites the BigQuery table from a clean Silver state.

---

### 🥉 Bronze Layer — Raw Ingestion with Schema Enforcement

**Notebook:** [`02_bronze_layer.ipynb`](notebooks/02_bronze_layer.ipynb)

**Production Pattern:** In real projects, Bronze is the **system of record** — it stores every raw record that ever entered the pipeline with zero data loss. This layer never modifies data; it only enforces types and attaches lineage metadata.

**Real-Time Implementation:**
| Pattern | How It's Implemented Here |
|---|---|
| **Schema-on-Write** | `BRONZE_SCHEMA` StructType enforced with `.cast()` before writing — bad records fail loudly |
| **Lineage Tracking** | `file_name` and `load_timestamp` attached to every row at ingestion time |
| **Date Extraction** | `date_reported` parsed from the filename suffix (`yyyyMMdd`) using `substring_index` + `to_date` |
| **Incremental Append** | `.mode("append")` — new runs add data without touching historical partitions |
| **Partition Pruning** | `partitionBy("date_reported")` — downstream layers read only the date they need |
| **Multi-Brand Parallel** | Loops over `["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]` — each brand has its own Delta path |
| **Adaptive Query** | `spark.sql.adaptive.enabled = true` — Spark auto-optimises join strategies at runtime |
| **GCS Auth** | Service account JSON injected into Spark session config — no static credential files on disk |

**Bronze Schema:**

| Column | Type | Nullable | Description |
|---|---|---|---|
| `Brand` | String | Yes | Mobile brand name (e.g., Samsung, Apple) |
| `Model` | String | Yes | Device model identifier |
| `Distributor_code` | String | Yes | Distributor identifier |
| `Retailer_code` | String | Yes | Retailer identifier |
| `Store_code` | String | Yes | Store identifier |
| `EAN_code` | Long | Yes | International product barcode |
| `Currency` | String | Yes | Transaction currency code |
| `Price` | Integer | Yes | Unit price in local currency |
| `Stock_units` | Integer | Yes | Units available in stock |
| `Sale_units` | Integer | Yes | Units sold |
| `date_reported` | Date | **No** | Reporting date — partition key, parsed from filename |
| `file_name` | String | **No** | Source file name — full lineage traceability |
| `load_timestamp` | Timestamp | **No** | Exact UTC ingestion timestamp |

**Storage:** `gs://<pipeline_bucket>/delta/bronze/<brand>/` · **Mode:** `append` (ACID — partial writes never committed)

---

### 🥈 Silver Layer — Deduplication & Business Key Consolidation

**Notebook:** [`03_silver_layer.ipynb`](notebooks/03_silver_layer.ipynb)

**Production Pattern:** In real projects, Silver is the **single source of truth** — it is the clean, deduplicated, fully-typed table that all analytical queries and downstream jobs consume. It handles the hard problem of late-arriving or duplicate data using a business key window function.

**Real-Time Implementation:**
| Pattern | How It's Implemented Here |
|---|---|
| **Cross-Brand Union** | Reads all 5 brand Bronze tables and `unionByName(..., allowMissingColumns=True)` — schema-safe merge |
| **Market Derivation** | `Market_code` = `substring(Distributor_code, 1, 3)` — a real business rule, not a lookup join |
| **Window Deduplication** | `ROW_NUMBER()` over business key, ordered by `load_timestamp DESC, file_name DESC` — keeps the freshest record |
| **Business Key** | 6-column composite key: `Market_code + Distributor_code + Retailer_code + Store_code + EAN_code + date_reported` |
| **Idempotent Overwrite** | Dynamic partition overwrite — only the date partitions being processed are replaced, not the full table |
| **Output Optimisation** | `.repartition(8)` — controls output file count to avoid small-file problems at scale |
| **Optional Date Filter** | `dt` widget parameter — in production, pass today's date to process only the latest partition |
| **Adaptive Query** | `spark.sql.adaptive.enabled = true` + `partitionOverwriteMode = dynamic` |

**Business Key (deduplication key):**
```
Market_code · Distributor_code · Retailer_code · Store_code · EAN_code · date_reported
```

**Silver Schema:**

| Column | Type | Nullable | Description |
|---|---|---|---|
| `Market_code` | String | **No** | Derived market identifier — first 3 chars of Distributor_code |
| `Brand` | String | Yes | Mobile brand name |
| `Model` | String | Yes | Device model |
| `Distributor_code` | String | **No** | Distributor identifier |
| `Retailer_code` | String | Yes | Retailer identifier |
| `Store_code` | String | Yes | Store identifier |
| `EAN_code` | Long | **No** | Product barcode |
| `Currency` | String | Yes | Currency code |
| `Price` | Integer | Yes | Unit price |
| `Stock_units` | Integer | Yes | Units in stock |
| `Sale_units` | Integer | Yes | Units sold |
| `date_reported` | Date | **No** | Reporting date — partition key |
| `file_name` | String | **No** | Source file name |
| `load_timestamp` | Timestamp | **No** | Ingestion timestamp |

**Storage:** `gs://<pipeline_bucket>/delta/silver/` · **Mode:** `overwrite` with dynamic partition overwrite

---

### 🥇 Gold Layer — Analytical Star Schema with BigQuery Dimensions

**Notebook:** [`04_gold_layer.ipynb`](notebooks/04_gold_layer.ipynb)

**Production Pattern:** In real projects, Gold is the **reporting layer** — it is the final, business-ready table that powers dashboards, reports, and BI tools. It joins clean Silver facts against reference dimension tables, computes derived KPIs, and organises data into nested STRUCT columns for maximum query efficiency in BigQuery.

**Real-Time Implementation:**
| Pattern | How It's Implemented Here |
|---|---|
| **Star Schema Join** | Silver fact table joined to 4 BigQuery dimensions: `dim_date`, `dim_market`, `product_v2`, `customer` |
| **Salted Join (Anti-Skew)** | `dim_date` join uses salt buckets (N=10) — explodes the calendar dim and adds random salt to facts, distributing hot keys evenly across Spark executors |
| **Broadcast Joins** | `dim_market`, `product_v2`, `customer` are broadcast — avoids shuffle for small dimension tables |
| **Nested STRUCTs** | Gold output uses 6 STRUCTs (`GEOGRAPHY`, `TRANSACTION_DATE`, `PRODUCT`, `CUSTOMER`, `CURRENCY`, `FACT_VALUE`) — BigQuery nested types cut query cost by enabling column pruning |
| **Derived KPIs** | `sale_value = Sale_units × Price` and `stock_value = Stock_units × Price` computed inline |
| **Indirect BQ Write** | Spark BigQuery connector writes via GCS temp bucket → BigQuery load job — bypasses streaming insert limits |
| **Output Optimisation** | `.repartition(4)` before write — controls BigQuery load file count |
| **Credential Injection** | Both GCS and BigQuery auth share the same service account JSON, configured per-session |

**Gold Schema — Nested STRUCTs (BigQuery analytical model):**

| STRUCT | Fields | Business Meaning |
|---|---|---|
| *(root)* | `tech_brand_name`, `brand_segment`, `tech_orga_country_name` | Top-level brand & country keys |
| `GEOGRAPHY` | `country_name`, `market_code2`, `market_code3`, `zone_code`, `zone_name` | Full geographic hierarchy |
| `TRANSACTION_DATE` | `period_date`, `year`, `month`, `week_num`, `month_year`, `quarter`, `quarter_year` | Full calendar hierarchy from `dim_date` |
| `PRODUCT` | `product_code`, `brand`, `model`, `display`, `processor`, `rear_camera`, `back_camera`, `ram`, `rom`, `refresh_rate`, `year_of_launch` | Full product spec from `product_v2` |
| `CUSTOMER` | `distributor_code`, `distributor_name`, `retailer_code`, `retailer_name`, `store_code`, `store_name`, `channel_mode` | Full channel hierarchy from `customer` dim |
| `CURRENCY` | `currency`, `hub_currency`, `conversion_rate` | Local and hub currency |
| `FACT_VALUE` | `asp_local`, `units_sold`, `sale_value`, `total_sale_price_eur_value`, `stock_units`, `stock_value`, `total_stock_price_eur_value` | All KPIs — volume, value, stock |
| `METADATA_TECHNICAL` | `file_name`, `file_creation_date`, `load_timestamp` | Full audit trail |

**Write Destination:** BigQuery `<project_id>.mobile_brands.gold_brand_daily_v1`
**Write Mode:** `overwrite` via `indirect` (GCS staging → BigQuery load job)
