# LCBO Settlement Reconciliation Pipeline

An end-to-end, fully orchestrated data engineering pipeline that ingests messy retail settlement files, cleans and conforms them, tracks slowly-changing reference data, and serves a dimensional star schema built for financial reconciliation reporting.

Built entirely on local, open-source tooling — **Python, PostgreSQL, dbt, and Apache Airflow (on Docker)** — following a **Bronze → Silver → Gold (Medallion) architecture**.

> This is a hands-on learning / portfolio project. The data is synthetic, but every data-quality problem it handles — mixed date formats, currency noise, supplier renames, back-dated costs, reversal corrections, ghost records — is modelled on problems that occur in real production settlement feeds.

---

## Table of Contents
- [Why this project](#why-this-project)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [The Bronze layer](#the-bronze-layer-python)
- [The Silver layer](#the-silver-layer-dbt)
- [Slowly Changing Dimensions](#slowly-changing-dimensions-snapshots)
- [The Gold layer](#the-gold-layer-star-schema)
- [Idempotency](#idempotency--the-core-reliability-guarantee)
- [Orchestration with Airflow](#orchestration-with-airflow-on-docker)
- [Execution order](#execution-order)
- [Running it locally](#running-it-locally)
- [Project structure](#project-structure)
- [Challenges solved](#challenges-solved)
- [Roadmap](#roadmap)
- [Concepts demonstrated](#concepts-demonstrated)

---

## Why this project

Settlement reconciliation is a demanding test for a data pipeline because the source data is *deliberately* awful: the same field arrives in three date formats, prices carry dollar signs and thousands-commas, supplier names change, product costs get back-dated, and corrections arrive as negative reversal rows. The goal was to build a pipeline that handles all of this **idempotently** — re-runnable and reprocessable without ever duplicating data — and produces a clean star schema an analyst could query directly.

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │   Apache Airflow (Docker) — one DAG          │
                          │   Bronze ▸ Silver ▸ Gold task groups,        │
                          │   execution order enforced by dependencies   │
                          └─────────────────────────────────────────────┘
                                             │ orchestrates
            ┌────────────────────────────────┼────────────────────────────────┐
            ▼                                 ▼                                 ▼
 ┌───────────────────┐           ┌───────────────────────┐        ┌────────────────────────┐
 │  BRONZE (Python)  │           │   SILVER (dbt)        │        │   GOLD (dbt)           │
 │  raw ingest, TEXT │  ───────▶ │  clean · cast · flag  │ ─────▶ │  star schema           │
 │  content-hash     │           │  SCD2 snapshots       │        │  dims + incremental    │
 │  batch audit log  │           │  point-in-time join   │        │  fact (merge)          │
 └───────────────────┘           └───────────────────────┘        └────────────────────────┘
            │                                 │                                 │
            └─────────────────────────────────┴─────────────────────────────────┘
                                             ▼
                          ┌─────────────────────────────────────────────┐
                          │  PostgreSQL — bronze / silver / gold schemas │
                          └─────────────────────────────────────────────┘
```

Each layer has a distinct responsibility: **Bronze** preserves raw truth, **Silver** transforms and conforms, **Gold** serves analytics.

---

## Tech stack

| Concern | Tool | Role |
|---------|------|------|
| Ingestion | Python (`psycopg2`) | Chunked CSV load, batch audit, idempotency |
| Storage | PostgreSQL | `bronze` / `silver` / `gold` schemas in one database |
| Transformation | dbt (`dbt-postgres`) | Cleaning, modelling, SCD2, star schema |
| Packages | `dbt_utils` | Surrogate keys, date spine |
| Orchestration | Apache Airflow (LocalExecutor, Docker) | DAG scheduling and task ordering |
| Containerisation | Docker / Docker Compose | Reproducible Airflow + dbt environment |

**Why dbt for transformations?** dbt lets transformations live as version-controlled, modular SQL with reusable macros, automatic dependency management (via `ref()`), built-in testing, data lineage, and native handling of incremental loads and SCD2 snapshots — so the focus stays on transformation logic rather than plumbing.

---

## The Bronze layer (Python)

Three ingestion scripts handle raw loading:
- `bronze_batch_audit.py` — maintains the batch audit log that underpins idempotency
- `bronze_lcbo_raw.py` — chunked ingestion of settlement files
- `bronze_product_cost.py` — ingestion of the Operations-owned product cost reference

**Design choices:**
- **Everything lands as `TEXT`.** Bronze never casts. Raw data is preserved exactly as received, so nothing is lost or silently mangled at ingestion — all typing and validation happen downstream in Silver.
- **Metadata columns** (`_source_file`, `_batch_id`, `_ingested_at`, `_row_hash`, `_row_number`) stamp every row for lineage and debugging.
- **SHA-256 content-hash idempotency.** Files are identified by the hash of their *contents*, not their filename — so a re-sent identical file is skipped, and a corrected file (different contents) is treated as a new batch.
- **Chunked loading** via a generator and `execute_values` keeps memory flat regardless of file size.

---

## The Silver layer (dbt)

Silver cleans the mess. Models are materialised as **tables** (dbt owns the drop-and-create each run), so Silver always reflects the latest batch and stays inspectable for debugging.

### Reusable cleaning macros

| Macro | Handles |
|-------|---------|
| `parse_multi_format_date` | Three date formats (`2024-01-01`, `01/04/2024`, `04-Jan-2024`) routed by regex |
| `clean_currency` | Strips `$`, thousands-commas, quotes → validated numeric cast |
| `float_col_conversion` | Safe text → float with a validation guard |
| `clean_text` | Trim + collapse internal whitespace + consistent casing |
| `cleaning_itemid` | Repairs Excel scientific-notation item IDs |
| `target_schema` | Overrides dbt's `generate_schema_name` so custom schemas land as `gold`, not the default-concatenated `silver_gold` |

Every cleaning macro follows a **validate-then-cast-or-null** pattern: a value that doesn't match the expected shape becomes `NULL` rather than throwing and killing the run — then it's caught by a dbt test. Expectations become assertions.

### Model flow
```
dbt_lcbo_raw ─▶ dbt_lcbo_columns_trans ─▶ dbt_lcbo_columns_type ─▶ dbt_lcbo_cleansed_settlement
                                                                  ─▶ dbt_lcbo_filterout_negative_sales
                                                                       ─▶ dbt_lcbo_joined_dim_prod_cost
dbt_product_cost ─▶ dbt_dim_product_transform
```

**Handling the hard cases:**
- **Reversal corrections** (negative-quantity rows) are *preserved*, not filtered — they net against the original overstated sale, so deleting them would break reconciliation totals.
- **Point-in-time cost lookup** joins each settlement to the cost in effect on its settlement date, using a half-open interval against the cost SCD2 snapshot.

---

## Slowly Changing Dimensions (snapshots)

Two dbt snapshots, each strategy chosen for a reason:

| Snapshot | Strategy | Why |
|----------|----------|-----|
| `dim_product_cost` | `timestamp` on `cost_effective_date` | Operations supplies a reliable effective date → true effective-dating for the price join |
| `dim_product` | `check` on description / category / supplier | Product attributes come from invoices with no trustworthy date, so change is detected by comparing values |

This distinction — **`timestamp` when you trust the date, `check` when you must compare values** — is the core SCD2 design decision.

---

## The Gold layer (star schema)

A classic star: a narrow fact table, descriptive dimensions.

**Dimensions**
- `gold_dim_date` — static, generated with `dbt_utils.date_spine`, integer `YYYYMMDD` surrogate key
- `gold_dim_store` — SCD Type 1 (overwrite)
- `gold_dim_product_info` — SCD Type 2, reads the product snapshot
- `gold_dim_product_cost` — SCD Type 2, reads the cost snapshot

**Fact**
- `gold_fact_table` — incremental, **merge** strategy, keyed on a composite surrogate `settlement_row_id`

**Key modelling decisions:**
- **Surrogate keys everywhere.** SCD2 makes natural keys non-unique (one product → many versioned rows), so facts join on per-version surrogate keys, letting each sale point at the correct historical version.
- **Merge, not append.** Reprocessing a corrected file produces the same `settlement_row_id`, so merge updates in place instead of duplicating — idempotent end to end.
- **Descriptive columns live in dimensions, not the fact** — avoids stale copies that would drift from SCD2 history.
- **A ghost-record flag** marks facts whose dimension couldn't be resolved, so no sale is ever dropped.

---

## Idempotency — the core reliability guarantee

The pipeline can be re-run, or a corrected file reprocessed, **without ever duplicating a row.** Three layers work together:

1. **Content-hash (SHA-256) file detection** in Bronze — a re-sent identical file loads 0 rows
2. **A batch audit log** — tracks exactly what has been loaded
3. **A merge-strategy incremental fact table** — reprocessing updates in place rather than appending

**Demonstrated behaviour:** adding a single new row to an already-processed file and re-running results in **exactly one new row** in the gold fact table — existing rows untouched, no duplicates, no full reload.

---

## Orchestration with Airflow (on Docker)

The entire pipeline runs as **one Airflow DAG** with three task groups — **Bronze → Silver → Gold** — where the group dependencies *guarantee* ordering: Silver never starts before Bronze finishes, Gold never before Silver.

- **LocalExecutor** on Docker Compose (Airflow + its metadata Postgres)
- A **custom image** (`Dockerfile`) extends the base Airflow image with `dbt-postgres`
- **Volume mounts** expose the dbt project, ETL scripts, source files, and `profiles.yml` to the containers
- The containerised dbt reaches the host's PostgreSQL via `host.docker.internal` and an explicit `--target docker` profile
- Database credentials are kept out of code using **Airflow Variables** (`{{ var.value.* }}`), so nothing sensitive is committed

The two `dbt snapshot` steps are correctly sandwiched between `dbt run` phases, matching the dependency order below.

---

## Execution order

```
# Bronze (Python)
1. bronze_batch_audit.py
2. bronze_lcbo_raw.py
3. bronze_product_cost.py

# Silver (dbt run)
4–10. dbt_lcbo_raw → dbt_product_cost → columns_trans → columns_type
      → cleansed_settlement → filterout_negative_sales → dim_product_transform

# Snapshot
11. dbt snapshot dim_product_cost

# Silver (dbt run)
12. dbt_lcbo_joined_dim_prod_cost

# Snapshot
13. dbt snapshot dim_product

# Gold (dbt run)
14–19. dim_date → dim_product_cost → dim_product_info → dim_store
       → fact (silver) → gold_fact_table
```

---

## Running it locally

### Prerequisites
- Docker Desktop
- PostgreSQL (local) with your database and schemas (`bronze`, `silver`, `gold`)

### Setup
```bash
# 1. Clone
git clone <your-repo-url>
cd "Data Modeling Project"

# 2. Set AIRFLOW_UID in .env
echo "AIRFLOW_UID=50000" >> .env

# 3. Build the custom Airflow + dbt image
docker compose build

# 4. Initialise Airflow (one-time)
docker compose up airflow-init

# 5. Start
docker compose up -d
```

### Configure
- Set your DB credentials as **Airflow Variables** (Admin → Variables): `PG_PASSWORD`, `PG_USER`, `PG_DBNAME`
- Confirm connectivity from inside the container:
  ```bash
  docker compose exec airflow-scheduler bash
  cd /opt/airflow/dbt_project && dbt debug --target docker
  ```

### Run
Open the Airflow UI at `http://localhost:8080`, find the DAG, and trigger it. Watch Bronze → Silver → Gold execute in order in the Graph view.

---

## Project structure
```
Data Modeling Project/
├── dags/                            # Airflow DAG
├── etl_scripts/                     # Bronze layer (Python)
│   ├── bronze_batch_audit.py
│   ├── bronze_lcbo_raw.py
│   └── bronze_product_cost.py
├── dbt_transformation_lcbo/         # Silver + Gold (dbt)
│   ├── models/
│   │   ├── bronze_lcbo_staging/
│   │   ├── silver_lcbo_transform/
│   │   └── gold_LCBO_marts/
│   ├── snapshots/
│   ├── macros/
│   └── dbt_project.yml
├── Source files/                    # Input CSVs
├── docker-compose.yaml
├── Dockerfile
└── profiles.yml
```

---

## Challenges solved

Real problems worked through while building this — the kind tutorials skip:

- **dbt schema concatenation** — dbt's default `generate_schema_name` produced `silver_gold` instead of `gold`; fixed by overriding the macro.
- **Containerised networking** — inside Docker, `localhost` isn't the host; dbt and the Python scripts reach host Postgres via `host.docker.internal`.
- **CeleryExecutor crash loop** — switched to LocalExecutor and removed the orphaned worker/redis services.
- **pip dependency hell** — a `ResolutionTooDeep` build failure fixed by installing only `dbt-postgres` (which pulls the correct `dbt-core`).
- **Silent task failures** — a Bronze script caught an exception and exited 0, so Airflow saw "success"; fixed by re-raising so failures surface.
- **Merge fan-out** — `MERGE cannot affect row a second time` traced to duplicate surrogate keys from a dimension with duplicate rows and from an under-specified grain.
- **SCD2 snapshot thrashing** — `select distinct` passing two supplier names per item caused phantom version churn; resolved by deduplicating to one current row per key.
- **Secrets management** — moved hardcoded credentials out of the DAG into Airflow Variables.

---

## Roadmap

- **Late-arriving data** — correctly attributing a back-dated record to the dimension version valid *at its real date*. This is a point-in-time correctness problem; doing it properly requires source-provided effective dates on the product dimension, since the current SCD2 snapshot uses observation-time validity.
- **Scheduled runs** — moving from manual triggers to a schedule.
- **Expanded dbt tests** — `not_null` / `unique` / relationship tests wired as automated data-quality gates.
- **A reconciliation dashboard** on the Gold layer surfacing the source-vs-derived variance metric.
- **Cloud extension** — mapping the local Medallion design onto cloud equivalents.

---

## Concepts demonstrated

Medallion architecture · idempotent ingestion (content-hash + audit log + merge) · SCD Type 1 & Type 2 · dbt snapshots (timestamp vs check strategy) · point-in-time joins · surrogate keys · incremental models · star schema dimensional modelling · reusable Jinja macros · custom `generate_schema_name` override · ghost-record handling · data-quality flagging · Airflow DAG orchestration with task groups · Docker containerisation · secrets management via Airflow Variables.
