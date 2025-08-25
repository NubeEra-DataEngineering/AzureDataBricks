# Azure Databricks — Delta Lake & Delta Live Tables Demo

This repo gives you a runnable, end-to-end starter for **Delta Lake** and **Delta Live Tables (DLT)** on **Azure Databricks** using PySpark and a public sample dataset.

## What you’ll get
- **Databricks notebook** covering:
  - Intro to Delta Lake & ACID Transactions
  - Creating & Managing Delta Tables
  - **Time Travel**, **Schema Enforcement** & **Schema Evolution**
  - Batch ingestion + **Streaming** (Auto Loader) concepts
  - Optimizations: **OPTIMIZE**, **ZORDER**, **Caching**, **Compaction**
- **DLT**:
  - Python DLT pipeline notebook (`dlt/dlt_pipeline.py`)
  - Pipeline configuration (`dlt/pipeline.json`) with expectations for **data quality**
- **Cluster script** to create a cluster via Databricks REST API using the Databricks CLI.
- Ready to import into your workspace.

> Sample dataset: [`data.csv`](https://github.com/NubeEra-Samples/DataSets/blob/master/data.csv)

---

## Prerequisites
1. **Azure Databricks workspace** (any tier that supports Delta & DLT; DLT generally requires Premium+).
2. **Databricks CLI** configured with a **user PAT**:
   ```bash
   pip install databricks-cli
   databricks configure --token
   # DATABRICKS_HOST=https://adb-<workspace-id>.<az-region>.azuredatabricks.net
   # DATABRICKS_TOKEN=<your PAT>
   ```
3. **Workspace permissions** to create clusters, jobs, and DLT pipelines.

> If your environment blocks outbound internet, **manually upload** `data.csv` to `/FileStore/tables/data.csv` in Databricks (UI: Data » Add Data » Upload). The notebook also includes a fallback that downloads it from GitHub if outbound internet is allowed.

---

## Quick Start

### 1) Create a cluster
Edit `scripts/cluster.json` as needed (node types, runtime), then run:
```bash
cd scripts
bash create_cluster.sh
```
This uses the Databricks REST API via the CLI. It echoes the **cluster_id** upon success.

### 2) Import the notebooks
- In the Databricks UI: **Workspace → Import** and import:
  - `notebooks/DeltaLake_DLT_Demo.py`
  - `dlt/dlt_pipeline.py` (for DLT)

Alternatively, use the Databricks CLI `workspace import` command if you prefer.

### 3) Prepare the dataset
Option A — **Automatic (with internet)**: Run the first cell in `DeltaLake_DLT_Demo.py` to download the CSV into `/FileStore/tables/delta_dlt_demo/raw/`.

Option B — **Manual (no internet)**:
1. Download `data.csv` locally from GitHub (open the link and click **Raw** → Save).
2. In Databricks: **Data → Add data → Upload**, and upload to `/FileStore/tables/delta_dlt_demo/raw/`.

### 4) Run the Delta Lake notebook
Open `DeltaLake_DLT_Demo` in your cluster and run all cells. It will:
- Create a database and Delta locations under `/dbfs/tmp/delta_dlt_demo/`.
- Ingest CSV → **bronze** Delta.
- Show **ACID** ops, **time travel**, **schema enforcement/evolution**, and **optimizations**.

### 5) Run the DLT pipeline
1. In Databricks UI: **Workflows → Delta Live Tables → Create pipeline → Import configuration**.
2. Paste the contents of `dlt/pipeline.json` and fix the **notebook path** (see comments inside) to match where you import `dlt_pipeline.py`.
3. Choose a suitable **DLT product edition**, set a **storage location** (e.g. `dbfs:/tmp/dlt/delta_dlt_demo`), and click **Create**.
4. Start the pipeline in **triggered** or **continuous** mode.
5. Explore the lineage graph, table statuses, and expectations (quality rules).

---

## Files

```
scripts/
  create_cluster.sh     # Bash helper to create a cluster via Databricks CLI
  cluster.json          # Editable cluster spec (runtime, node size, autoscaling)

notebooks/
  DeltaLake_DLT_Demo.py # Main Delta Lake demo (PySpark + SQL)

dlt/
  dlt_pipeline.py       # Delta Live Tables pipeline (bronze → silver → gold)
  pipeline.json         # DLT pipeline config (edit notebook path before creating)
```

---

## Notes
- **ZORDER** requires Databricks **Photon** runtimes to get best performance, though the command runs on DBR 10.4+.
- **Auto Loader** (`cloudFiles`) works best with cloud object storage (e.g., ADLS Gen2). For a local demo, this sample uses DBFS paths for convenience.
- **Time Travel** retains history as per your Delta Lake `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` settings.

Happy Databricks-ing! ✨
