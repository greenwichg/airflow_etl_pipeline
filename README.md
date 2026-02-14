# Retail Sales ETL Pipeline

**Production-grade Apache Airflow pipeline with Change Data Capture (CDC)** that extracts sales data from multiple regional PostgreSQL databases, transforms it into a unified schema, validates data quality, and loads it into Snowflake for enterprise analytics. Supports both **batch (Airflow)** and **real-time (Debezium + Kafka + Snowpipe)** ingestion paths.

| | |
|---|---|
| **Schedule** | Daily at 06:00 UTC (batch) / Real-time (CDC) |
| **SLA** | 2 hours (batch) / < 2 min latency (CDC) |
| **Throughput** | ~2M records/day across 4 regions |
| **Runtime** | ~40 minutes (batch) |
| **Orchestrator** | Apache Airflow 2.5+ |
| **CDC** | Debezium + Kafka + Snowpipe Streaming |
| **Warehouse** | Snowflake (Streams & Tasks) |

---

## Table of Contents

- [Architecture](#architecture)
- [CDC Architecture](#cdc-architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [DAG Flow](#dag-flow)
- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [CDC Setup](#cdc-setup)
- [Data Quality](#data-quality)
- [Testing](#testing)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)
- [Security](#security)
- [Future Enhancements](#future-enhancements)

---

## Architecture

### Dual-Path Data Flow (Batch + CDC)

```mermaid
graph LR
    subgraph Sources["Regional Databases (PostgreSQL)"]
        PG1[(US East)]
        PG2[(US West)]
        PG3[(Europe)]
        PG4[(APAC)]
    end

    subgraph BatchPath["PATH 1: Batch (Airflow)"]
        EXT["Incremental<br/>Extraction<br/>(updated_at)"]
        TRN["Transform<br/>& Unify"]
        VAL{"Data Quality<br/>Validation"}
    end

    subgraph CDCPath["PATH 2: Real-Time (Debezium + Kafka)"]
        DBZ["Debezium<br/>Connectors"]
        KFK["Kafka<br/>Topics"]
        SNP["Snowpipe<br/>Streaming"]
    end

    subgraph Warehouse["Snowflake"]
        SF_STG[Staging Tables]
        STREAM["Streams"]
        TASK["Tasks"]
        SF_FINAL[(analytics.sales_fact)]
    end

    PG1 & PG2 & PG3 & PG4 --> EXT
    PG1 & PG2 & PG3 & PG4 -.->|WAL| DBZ

    EXT --> TRN --> VAL -->|Pass| SF_STG
    DBZ --> KFK --> SNP --> SF_STG
    SF_STG --> STREAM --> TASK -->|CDC MERGE| SF_FINAL
    VAL -->|Fail| ALERT[Alert & Skip]

    style VAL fill:#ffeb3b
    style ALERT fill:#f44336,color:#fff
    style SF_FINAL fill:#4caf50,color:#fff
    style CDCPath fill:#fff3e0
    style BatchPath fill:#e3f2fd
```

### Batch Pipeline (Airflow)

```
 Regional DBs          Airflow                 Staging              Snowflake
 ============     ================        ===============      ===============

 +-----------+    +--------------+
 | US East   |--->| extract_     |
 | PostgreSQL|    | us_east      |---+
 +-----------+    +--------------+   |
                                     |   +-------------+    +-----------+    +----------+
 +-----------+    +--------------+   +-->| PostgreSQL  |--->|    S3     |--->| Staging  |
 | US West   |--->| extract_     |---+   | staging     |    | (CSV)    |    | table    |
 | PostgreSQL|    | us_west      |       +------+------+    +----------+    +----+-----+
 +-----------+    +--------------+              |                                |
                                          +-----v------+                   +-----v-----+
 +-----------+    +--------------+        | Transform  |                   | CDC MERGE |
 | Europe    |--->| extract_     |---+    | & validate |                   | (upsert + |
 | PostgreSQL|    | europe       |   |    +------------+                   |  delete)  |
 +-----------+    +--------------+   |                                     +-----+-----+
                                     |                                           |
 +-----------+    +--------------+   |                                     +-----v-----+
 | APAC      |--->| extract_     |---+                                     | analytics |
 | PostgreSQL|    | asia_pacific |                                         | .sales_   |
 +-----------+    +--------------+                                         | fact      |
                                                                           +-----------+
```

---

## CDC Architecture

The pipeline implements **three layers of Change Data Capture (CDC)**, each addressing a different latency and complexity requirement:

### Layer 1: Timestamp-Based Incremental Extraction (Airflow)

The batch DAG uses `updated_at` timestamp windows instead of `sale_date` filters, capturing **all changes** (inserts, updates, soft-deletes) that occurred since the last run:

```sql
-- Old: missed back-dated edits
WHERE DATE(sale_date) = '2024-01-15'

-- New: captures every change in the window
WHERE updated_at >= '2024-01-15 00:00:00'
  AND updated_at <  '2024-01-16 00:00:00'
```

Soft-deleted rows (`is_deleted = TRUE`) flow through the pipeline and are propagated as `DELETE` operations in the Snowflake MERGE.

### Layer 2: Debezium + Kafka (Real-Time Streaming)

```mermaid
graph LR
    subgraph PG["PostgreSQL (per region)"]
        WAL["WAL<br/>(pgoutput)"]
    end

    subgraph Connect["Kafka Connect"]
        DBZ["Debezium<br/>Source<br/>Connector"]
        SINK["Snowflake<br/>Sink<br/>Connector"]
    end

    subgraph Kafka
        T["cdc.retail.sales<br/>.transactions"]
        SR["Schema<br/>Registry"]
    end

    subgraph SF["Snowflake"]
        RAW["CDC_SALES_EVENTS<br/>(raw JSON)"]
    end

    WAL -->|Logical Replication| DBZ
    DBZ --> T
    T --> SINK
    SINK -->|Snowpipe Streaming| RAW
    T --- SR

    style T fill:#ff9800,color:#fff
    style RAW fill:#4caf50,color:#fff
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Source connectors** | Debezium 2.4 (PostgreSQL) | Reads WAL, emits change events per region |
| **Message bus** | Apache Kafka (Confluent 7.5) | Durable, ordered event stream |
| **Schema management** | Confluent Schema Registry | Avro schemas, backward compatibility |
| **Sink connector** | Snowflake Kafka Connector | Snowpipe Streaming into staging |
| **Dead letter queue** | Kafka DLQ topics | Captures failed events for replay |

### Layer 3: Snowflake Streams & Tasks (Autonomous Processing)

```mermaid
graph TD
    RAW["CDC_SALES_EVENTS<br/>(raw Debezium JSON)"] --> SA["Stream A<br/>STREAM_CDC_RAW_EVENTS"]
    SA -->|"Task A (1 min)"| PARSE["PARSE_CDC_EVENTS"]
    PARSE --> STG["SALES_DATA_STAGE<br/>(parsed rows)"]
    STG --> SB["Stream B<br/>STREAM_SALES_STAGED"]
    SB -->|"Task B (child)"| MERGE["MERGE_TO_SALES_FACT"]
    MERGE --> FINAL["ANALYTICS.SALES_FACT"]
    MERGE --> AUDIT["AUDIT.CDC_MERGE_LOG"]

    style RAW fill:#fff3e0
    style FINAL fill:#4caf50,color:#fff
    style AUDIT fill:#e3f2fd
```

Tasks run **autonomously** -- no Airflow scheduling needed. They activate only when streams detect new data (`SYSTEM$STREAM_HAS_DATA()`), avoiding wasted compute.

---

## Project Structure

```
airflow_etl_pipeline/
|
|-- dags/
|   +-- retail_sales_etl_dag.py        # Main DAG: CDC extraction, transform, validate, load
|
|-- config/
|   |-- region_config.json             # Dynamic region definitions (add regions here)
|   |-- airflow_config_setup.py        # Connection & variable setup helpers
|   +-- debezium/
|       |-- connector_us_east.json     # Debezium source connector (US East)
|       |-- connector_us_west.json     # Debezium source connector (US West)
|       |-- connector_europe.json      # Debezium source connector (Europe)
|       |-- connector_asia_pacific.json  # Debezium source connector (APAC)
|       +-- snowflake_sink_connector.json  # Snowpipe Streaming sink connector
|
|-- sql/
|   |-- 01_postgresql_cdc_setup.sql    # WAL config, replication user, publications
|   |-- 02_snowflake_cdc_tables.sql    # CDC events, staging, fact, audit tables
|   |-- 03_snowflake_streams.sql       # Streams on staging tables
|   +-- 04_snowflake_tasks.sql         # Automated parse + merge tasks
|
|-- scripts/
|   +-- deploy_connectors.sh           # Deploy all Kafka Connect connectors
|
|-- tests/
|   +-- test_retail_sales_etl.py       # Unit tests: DAG, transforms, quality, CDC
|
|-- docs/
|   |-- architecture_diagrams.md       # Mermaid architecture diagrams
|   +-- interview_prep.md              # Interview Q&A guide
|
|-- envs/
|   +-- env.template                   # Environment variable template
|
|-- .github/workflows/
|   +-- ci.yml                         # CI: lint, DAG validation, tests
|
|-- Dockerfile                         # Airflow image with project deps
|-- docker-compose.yml                 # Full stack: Airflow + Kafka + Debezium
|-- requirements.txt                   # Pinned Python dependencies
|-- .gitignore
+-- README.md
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.5+ | DAG scheduling, task management, retries |
| **Source DBs** | PostgreSQL 12+ (WAL) | 4 regional transactional databases |
| **CDC Capture** | Debezium 2.4 | Reads PostgreSQL WAL via logical replication |
| **Message Bus** | Apache Kafka 7.5 | Durable, ordered CDC event stream |
| **Schema Mgmt** | Confluent Schema Registry | Avro schema evolution |
| **Staging DB** | PostgreSQL 12+ | Intermediate transformation storage |
| **Processing** | Python / Pandas | Data consolidation, cleaning, enrichment |
| **Object Storage** | AWS S3 | Staging area for Snowflake COPY |
| **Data Warehouse** | Snowflake | Streams, Tasks, and final analytics tables |
| **Streaming Ingest** | Snowpipe Streaming | Real-time Kafka-to-Snowflake ingestion |
| **Bulk Insert** | SQLAlchemy | Parameterized batch writes to PostgreSQL |
| **Testing** | pytest | Unit, config, CDC, and integration tests |
| **CI/CD** | GitHub Actions | Lint (ruff), DAG validation, test suite |
| **Containers** | Docker / Compose | Full stack: Airflow + Kafka + Debezium |
| **Monitoring** | Kafka UI | Topic inspection, consumer lag, connector status |

---

## DAG Flow

```mermaid
graph TD
    START(["start"]) --> TG

    subgraph TG["extract_data (TaskGroup -- parallel)"]
        E1["extract_us_east"]
        E2["extract_us_west"]
        E3["extract_europe"]
        E4["extract_asia_pacific"]
    end

    TG --> TRANSFORM["transform_sales_data"]
    TRANSFORM --> VALIDATE{"validate_data_quality"}

    VALIDATE -->|Pass| LOAD["load_to_snowflake"]
    VALIDATE -->|Fail| FAIL["data_quality_failure"]

    LOAD --> STAGE["stage_data_in_snowflake"]
    STAGE --> MERGE["merge_to_final_table"]
    MERGE --> VERIFY["verify_snowflake_load"]
    VERIFY --> SUCCESS(["pipeline_success"])

    SUCCESS --> END(["end"])
    FAIL --> END

    style TG fill:#e3f2fd
    style VALIDATE fill:#ffeb3b
    style SUCCESS fill:#4caf50,color:#fff
    style FAIL fill:#f44336,color:#fff
```

**Key settings:**

```python
schedule_interval = '0 6 * * *'   # Daily at 06:00 UTC
max_active_runs   = 1             # Sequential execution only
retries           = 2             # With exponential backoff (5 / 10 / 20 min)
sla               = 2 hours      # Alert if exceeded
```

---

## Features

### Parallel Extraction
Four regional databases are extracted concurrently inside an Airflow TaskGroup, cutting extraction time from ~40 min (sequential) to ~10 min.

### Config-Driven Dynamic DAG
Regions are defined in `config/region_config.json`. Adding a new region requires **zero code changes** -- just add a JSON entry and an Airflow connection:

```json
{
  "name": "latin_america",
  "conn_id": "postgres_latin_america",
  "display_name": "LATAM",
  "timezone": "America/Sao_Paulo",
  "extraction_timeout_minutes": 30,
  "retries": 3
}
```

### Comprehensive Transformations
| Step | Description |
|------|------------|
| Deduplication | Remove duplicate `sale_id` records |
| Type standardization | Cast dates, decimals consistently |
| Null handling | Fill missing `discount_amount` / `tax_amount` with 0 |
| Derived columns | `net_amount`, `gross_profit` |
| Partition columns | `sale_year`, `sale_month`, `sale_day` for Snowflake |
| Region normalization | `us_east` -> `US-EAST` (config-driven) |
| Business rules | Filter `quantity > 0`, `unit_price > 0`, `total_amount > 0` |

### CDC-Aware Loads
- **MERGE** with 3-way branch: `INSERT` new rows, `UPDATE` modified rows, `DELETE` soft-deleted rows
- `is_deleted` flag flows end-to-end from source through staging to Snowflake
- Batch ID (`etl_batch_id`) tracks each execution date
- DELETE + INSERT staging pattern ensures clean intermediate state

### Incremental Extraction
Extraction uses `updated_at` timestamp windows instead of `sale_date` filters, capturing back-dated edits, late-arriving corrections, and soft-deletes that traditional date-partition extraction misses.

### Real-Time CDC Pipeline
Debezium reads the PostgreSQL WAL (logical replication) and streams every INSERT, UPDATE, and DELETE to Kafka. The Snowflake Kafka Connector ingests events via Snowpipe Streaming with < 2 min end-to-end latency.

### Snowflake Autonomous Processing
Snowflake Streams and Tasks process CDC events automatically without external scheduling. Tasks activate only when streams detect new data, eliminating idle compute costs.

### Parameterized SQL
All runtime values (`window_start`, `window_end`, `batch_id`, `region`) use `%s` placeholders via `cursor.execute(query, params)` -- no f-string interpolation in SQL.

### Fail-Fast Validation
`BranchPythonOperator` gates the Snowflake load behind multi-layer quality checks. On failure, the pipeline skips loading and sends alerts.

---

## Quick Start

### Option A: Docker (recommended)

```bash
git clone <repo-url> && cd airflow_etl_pipeline

# Start full stack: Airflow + Kafka + Debezium + Schema Registry
docker compose up -d

# Open Airflow UI
open http://localhost:8080    # admin / admin

# Open Kafka UI (optional)
open http://localhost:8084

# Deploy CDC connectors (after services are healthy)
./scripts/deploy_connectors.sh
```

### Option B: Local

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize Airflow
airflow db init

# 3. Configure connections & variables
cp envs/env.template .env
source .env
python config/airflow_config_setup.py

# 4. Deploy DAG
cp dags/retail_sales_etl_dag.py $AIRFLOW_HOME/dags/

# 5. Validate & start
airflow dags list
airflow dags test retail_sales_etl_pipeline 2024-01-15
airflow dags unpause retail_sales_etl_pipeline
```

---

## Configuration

### Airflow Connections

| Connection ID | Type | Host | Purpose |
|--------------|------|------|---------|
| `postgres_us_east` | Postgres | us-east-db.company.com | US East regional DB |
| `postgres_us_west` | Postgres | us-west-db.company.com | US West regional DB |
| `postgres_europe` | Postgres | eu-db.company.com | Europe regional DB |
| `postgres_asia_pacific` | Postgres | apac-db.company.com | APAC regional DB |
| `postgres_default` | Postgres | staging-db.company.com | Staging DB |
| `snowflake_default` | Snowflake | -- | Data warehouse |
| `aws_default` | AWS | -- | S3 access |

### Airflow Variables

Set via **Admin -> Variables** in the Airflow UI:

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_STAGING_BUCKET` | `data-pipeline-staging` | S3 bucket for CSV staging |
| `ALERT_EMAIL_LIST` | `data-eng@company.com` | Failure notification recipients |
| `SNOWFLAKE_WAREHOUSE` | `ETL_WH` | Snowflake compute warehouse |

### Region Config (`config/region_config.json`)

Each entry generates an extraction task dynamically:

```json
{
  "regions": [
    {
      "name": "us_east",
      "conn_id": "postgres_us_east",
      "display_name": "US-EAST",
      "timezone": "US/Eastern",
      "extraction_timeout_minutes": 30,
      "retries": 3
    }
  ]
}
```

---

## CDC Setup

### 1. PostgreSQL Logical Replication

Run on each regional database (see `sql/01_postgresql_cdc_setup.sql`):

```bash
# Verify WAL level
psql -c "SHOW wal_level;"  # Must be 'logical'

# Create replication user and publication
psql -f sql/01_postgresql_cdc_setup.sql
```

### 2. Start Infrastructure

```bash
# Start full stack: Airflow + Kafka + Debezium + Schema Registry
docker compose up -d

# Verify all services are healthy
docker compose ps
```

| Service | Port | URL |
|---------|------|-----|
| Airflow UI | 8080 | http://localhost:8080 |
| Schema Registry | 8081 | http://localhost:8081 |
| Kafka Connect | 8083 | http://localhost:8083 |
| Kafka UI | 8084 | http://localhost:8084 |
| Kafka Broker | 9092 | -- |

### 3. Deploy Connectors

```bash
# Set database credentials
export POSTGRES_US_EAST_HOST=us-east-db.company.com
export POSTGRES_US_EAST_USER=debezium_replication
export POSTGRES_US_EAST_PASSWORD=...
# (repeat for all regions)

export SNOWFLAKE_ACCOUNT=my_account
export SNOWFLAKE_USER=etl_user
export SNOWFLAKE_PRIVATE_KEY=...

# Deploy all connectors
./scripts/deploy_connectors.sh

# Verify
curl localhost:8083/connectors | python3 -m json.tool
```

### 4. Snowflake Streams & Tasks

```bash
# Run SQL scripts in order
snowsql -f sql/02_snowflake_cdc_tables.sql
snowsql -f sql/03_snowflake_streams.sql
snowsql -f sql/04_snowflake_tasks.sql
```

Tasks activate automatically. Monitor with:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME IN ('PARSE_CDC_EVENTS', 'MERGE_TO_SALES_FACT')
ORDER BY SCHEDULED_TIME DESC LIMIT 20;
```

---

## Data Quality

### Validation Gates

Quality checks run **before** any data reaches Snowflake:

| Check | Threshold | On Failure |
|-------|-----------|------------|
| Minimum record count | >= 100 | Pipeline branches to failure path |
| NULL % in critical columns | < 5% | Pipeline branches to failure path |
| Quantity validation | > 0 | Filtered during transformation |
| Price validation | > 0 | Filtered during transformation |
| Amount sanity | 0.5x -- 2x of `qty * price` | Filtered during transformation |

### Validation Flow

```mermaid
graph TD
    A([Transformed Data]) --> B{Records >= 100?}
    B -->|No| F["Fail: below threshold"]
    B -->|Yes| C{NULL % <= 5%?}
    C -->|No| F
    C -->|Yes| D{Business rules valid?}
    D -->|No| F
    D -->|Yes| E["Pass: proceed to load"]

    F --> ALERT["Alert + skip load"]

    style E fill:#4caf50,color:#fff
    style F fill:#f44336,color:#fff
    style ALERT fill:#ff9800,color:#fff
```

---

## Testing

```bash
# Run full test suite
pytest tests/ -v

# Run specific test class
pytest tests/test_retail_sales_etl.py::TestRegionConfig -v
pytest tests/test_retail_sales_etl.py::TestDataTransformation -v
```

### Test Coverage

| Class | Tests | What it covers |
|-------|-------|---------------|
| `TestDAGStructure` | 5 | DAG loads, schedule, args, task count, dependencies |
| `TestRegionConfig` | 5 | Config file validity, required fields, uniqueness, task generation |
| `TestDataTransformation` | 5 | Deduplication, null handling, derived columns, partitions, regions |
| `TestDataQuality` | 3 | Record thresholds, null %, business rule validation |
| `TestExtractionLogic` | 2 | Timestamp window query, error handling |
| `TestSnowflakeOperations` | 2 | CDC MERGE structure, upsert logic |
| `TestMonitoringAndLogging` | 2 | XCom metadata, transformation metadata (with delete_markers) |
| `TestErrorScenarios` | 2 | Zero records, partial region failure |
| `TestPerformance` | 1 | Parallel vs sequential extraction benefit |
| `TestCDCLogic` | 8 | Live/delete split, delete passthrough, timestamp window, CDC upsert+delete, Debezium configs, Snowflake sink config, SQL scripts |

### CI Pipeline (`.github/workflows/ci.yml`)

Runs on every push/PR to `main`:
1. **Lint** -- `ruff check dags/ tests/ config/`
2. **DAG syntax** -- Validates DAG parses without errors
3. **Tests** -- `pytest tests/ -v`

---

## Performance

| Phase | Duration | Details |
|-------|----------|---------|
| Extraction (parallel) | ~10 min | 4 regions at ~2.5 min each |
| Transformation | ~15 min | Pandas + SQLAlchemy bulk insert |
| Validation | ~5 min | SQL-based quality checks |
| Snowflake Load | ~10 min | S3 COPY + MERGE upsert |
| **Total** | **~40 min** | **Well under 2-hour SLA** |

### Scaling Strategy

```mermaid
graph LR
    subgraph Current["Current (2M/day)"]
        A1["4 regions / Pandas"]
    end
    subgraph Scaled["10x Scale (20M/day)"]
        B1["N regions / PySpark on EMR"]
    end
    A1 -.->|"Add regions via JSON<br/>Swap Pandas for Spark"| B1

    style Current fill:#e3f2fd
    style Scaled fill:#c8e6c9
```

| Component | Current | At 10x |
|-----------|---------|--------|
| Extraction | 4 parallel regions | N parallel (config-driven) |
| Transform | Pandas (single node) | PySpark on EMR/Databricks |
| S3 staging | Single file | Partitioned files |
| Snowflake | MEDIUM warehouse | LARGE + auto-scaling |

---

## Troubleshooting

### Common Issues

**Connection timeout to regional DB**
```
Error: "could not connect to server: Connection timed out"
```
Check network/firewall rules. Increase `connect_timeout` in the Airflow connection.

**Data quality validation failure**
```
Error: "Record count 45 is below threshold 100"
```
Investigate source DB for missing data. Check regional holidays or date filter issues.

**Snowflake load timeout**
```
Error: "SQL execution timeout"
```
Scale up the Snowflake warehouse. Check for table locks.

### Recovery

```bash
# Rerun a specific date
airflow dags backfill retail_sales_etl_pipeline \
    --start-date 2024-01-15 --end-date 2024-01-15

# Clear failed tasks and retry
airflow tasks clear retail_sales_etl_pipeline \
    --task-regex '.*' \
    --start-date 2024-01-15 --end-date 2024-01-15
```

```sql
-- Rollback a batch in Snowflake, then rerun
DELETE FROM analytics.sales_fact WHERE etl_batch_id = '20240115';
```

---

## Security

| Area | Implementation |
|------|---------------|
| **Credentials** | Stored in Airflow connections (backed by Secrets Manager / Vault) |
| **SQL injection** | All queries use parameterized `%s` placeholders |
| **Network** | VPC peering for DB access, SSL/TLS for all connections |
| **Encryption** | Data at rest (S3 SSE, Snowflake), data in transit (TLS) |
| **Access control** | Snowflake RBAC roles, Airflow RBAC, least-privilege IAM |
| **Secrets in repo** | `.gitignore` excludes `.env`, credentials, and key files |

---

## Future Enhancements

- ~~**Real-time streaming**~~ -- Implemented (Debezium + Kafka + Snowpipe Streaming)
- ~~**Incremental extraction**~~ -- Implemented (timestamp-based `updated_at` window)
- ~~**Schema registry**~~ -- Implemented (Confluent Schema Registry with Avro)
- **Advanced monitoring** -- Datadog/Prometheus metrics, anomaly detection dashboards
- **PySpark transformation** -- Replace Pandas for datasets > 1M records
- **Cost optimization** -- Snowflake auto-suspend, S3 lifecycle policies
- **Multi-topic routing** -- Separate Kafka topics per region for independent scaling
- **Exactly-once semantics** -- Kafka transactions + Snowflake task idempotency tokens

---

## Contact

| | |
|---|---|
| **Maintainer** | Sanath |
| **Team** | Data Engineering |
| **Slack** | `#data-engineering` |
