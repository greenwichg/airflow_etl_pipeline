# dbt Interview Preparation Guide

Comprehensive Q&A guide for dbt (data build tool) interviews, covering fundamentals through advanced topics with real examples from this retail sales pipeline.

---

## Table of Contents

1. [Fundamentals](#1-fundamentals)
2. [Materializations](#2-materializations)
3. [Models & Refs](#3-models--refs)
4. [Testing](#4-testing)
5. [Sources & Freshness](#5-sources--freshness)
6. [Incremental Models](#6-incremental-models)
7. [Snapshots (SCD Type 2)](#7-snapshots-scd-type-2)
8. [Macros & Jinja](#8-macros--jinja)
9. [Packages](#9-packages)
10. [Seeds](#10-seeds)
11. [dbt with Airflow](#11-dbt-with-airflow)
12. [dbt with CDC](#12-dbt-with-cdc)
13. [Performance & Best Practices](#13-performance--best-practices)
14. [Advanced Scenarios](#14-advanced-scenarios)

---

## 1. Fundamentals

### Q: What is dbt and why is it used?

**A:** dbt (data build tool) is a transformation framework that lets data teams write modular SQL to transform data already loaded into a warehouse. It follows the **ELT** pattern — Extract & Load first (with tools like Airflow, Fivetran, Debezium), then Transform with dbt.

**Key value propositions:**
- **Version-controlled SQL** — models live in Git alongside application code
- **Dependency management** — `ref()` builds a DAG automatically
- **Testing** — schema tests, custom data tests, and source freshness checks
- **Documentation** — auto-generated docs from YAML and model descriptions
- **Modularity** — staging → intermediate → marts layering keeps logic DRY

### Q: What is the difference between ETL and ELT? Where does dbt fit?

**A:**
| | ETL | ELT |
|---|---|---|
| **Transform location** | Middleware (Informatica, SSIS) | Inside the warehouse |
| **Scale** | Limited by ETL server | Uses warehouse compute |
| **dbt role** | N/A | Handles the **T** (Transform) |

dbt runs SQL *inside* the warehouse (Snowflake, BigQuery, Redshift), leveraging its compute power rather than an external server.

### Q: Explain the dbt project structure.

**A:** Standard layout:
```
dbt_project/
├── dbt_project.yml          # Project config (name, materialization defaults)
├── profiles.yml             # Connection profiles (dev, prod)
├── packages.yml             # External package dependencies
├── models/
│   ├── staging/             # 1:1 with sources, cleaning & renaming
│   ├── intermediate/        # Business logic, ephemeral CTEs
│   └── marts/               # Final tables consumed by BI tools
├── macros/                  # Reusable SQL functions (Jinja)
├── tests/                   # Custom data quality tests
├── snapshots/               # SCD Type 2 history tracking
├── seeds/                   # Small CSV reference data
└── analyses/                # Ad-hoc queries (not materialized)
```

---

## 2. Materializations

### Q: What are the different materialization types?

**A:**

| Type | What it creates | When to use | Example |
|------|----------------|-------------|---------|
| **view** | `CREATE VIEW` | Staging models, light transforms | `stg_sales_transactions` |
| **table** | `CREATE TABLE` | Dimension tables, small datasets | `dim_regions` |
| **incremental** | `MERGE` / `INSERT` | Large fact tables, append-heavy | `fct_sales` |
| **ephemeral** | Inline CTE (no DB object) | Intermediate logic, shared subqueries | `int_sales_enriched` |

### Q: When would you choose incremental over table?

**A:** Use incremental when:
1. The table has **millions+ rows** — full rebuild is expensive
2. New data arrives in **append-only or timestamped** batches
3. The source has an **updated_at** or **etl_loaded_at** column to filter on

From our project:
```sql
-- fct_sales.sql (incremental)
{% if is_incremental() %}
where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}
```

### Q: What is `{{ this }}` in dbt?

**A:** `{{ this }}` refers to the **current model's database relation** (schema.table). It's used inside `is_incremental()` blocks to query the existing table for watermarks:
```sql
select max(updated_at) from {{ this }}
```

---

## 3. Models & Refs

### Q: What does `ref()` do and why is it important?

**A:** `{{ ref('model_name') }}` does two things:
1. **Resolves** the model name to its fully qualified database object (e.g., `RETAIL_DW.ANALYTICS.fct_sales`)
2. **Builds the DAG** — dbt knows that this model depends on the referenced model and will run them in the correct order

```sql
-- This creates a dependency: int_sales_enriched → stg_sales_transactions
select * from {{ ref('stg_sales_transactions') }}
```

### Q: What is the staging → intermediate → marts pattern?

**A:**

| Layer | Purpose | Materialization | Naming |
|-------|---------|----------------|--------|
| **Staging** | 1:1 with source, clean/rename/cast | view | `stg_<source>_<entity>` |
| **Intermediate** | Business logic, joins, enrichment | ephemeral | `int_<entity>_<verb>` |
| **Marts** | Final output for BI consumers | incremental/table | `fct_`, `dim_`, `agg_` |

**Rules:**
- Staging models only `ref()` sources (never other staging models)
- Marts never directly reference sources
- Intermediate models bridge the gap

### Q: What is the difference between `ref()` and `source()`?

**A:**
- `{{ source('source_name', 'table_name') }}` — references raw tables declared in `_sources.yml`; enables freshness monitoring
- `{{ ref('model_name') }}` — references another dbt model; enables DAG dependency resolution

---

## 4. Testing

### Q: What types of tests does dbt support?

**A:**

**1. Schema tests (YAML-declared):**
```yaml
columns:
  - name: sale_id
    tests:
      - unique
      - not_null
  - name: region
    tests:
      - accepted_values:
          values: ['US-EAST', 'US-WEST', 'EUROPE', 'ASIA-PACIFIC']
```

**2. Custom data tests (SQL files in `/tests`):**
```sql
-- tests/assert_no_orphan_stores.sql
select store_id from {{ ref('fct_sales') }} f
left join {{ ref('dim_stores') }} d on f.store_id = d.store_id
where d.store_id is null
```
A custom test **fails if it returns any rows**.

**3. Source freshness tests:**
```yaml
sources:
  - name: raw_cdc
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    loaded_at_field: _inserted_at
```
Run with: `dbt source freshness`

**4. Package tests (e.g., dbt_expectations):**
```yaml
- dbt_expectations.expect_column_values_to_be_between:
    min_value: 1
```

### Q: How do you handle test failures in production?

**A:**
1. **Severity levels**: `warn` vs `error` — warnings log but don't block the DAG
2. **Store failures**: `dbt test --store-failures` writes failing rows to a table for investigation
3. **Airflow integration**: `dbt test` runs as an Airflow task; failure triggers `on_failure_callback` which sends alerts

---

## 5. Sources & Freshness

### Q: Why define sources in YAML instead of hardcoding table names?

**A:**
1. **Freshness monitoring** — `dbt source freshness` checks if data is stale
2. **Documentation** — sources appear in the dbt docs site with descriptions
3. **Single point of change** — if a source table is renamed, update one YAML file instead of many models
4. **Lineage** — the DAG shows the full path from source → staging → marts

### Q: How does source freshness work?

**A:** dbt queries the `loaded_at_field` column:
```sql
SELECT max(_inserted_at) FROM STAGING.CDC_SALES_EVENTS
```
If the most recent row is older than `warn_after` or `error_after`, it fails.

---

## 6. Incremental Models

### Q: Explain the incremental strategies available.

**A:**

| Strategy | SQL | When to use |
|----------|-----|-------------|
| **append** | `INSERT INTO` | Log/event tables with no updates |
| **merge** | `MERGE INTO ... ON key WHEN MATCHED/NOT MATCHED` | Fact tables with updates (CDC) |
| **delete+insert** | `DELETE WHERE ... ; INSERT ...` | When merge is slow (large partitions) |
| **insert_overwrite** | `INSERT OVERWRITE PARTITION` | Partition-level refreshes (Spark, BigQuery) |

Our project uses `merge` because CDC requires updating existing rows:
```sql
config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_strategy='merge'
)
```

### Q: What happens on the first run of an incremental model?

**A:** On first run, `is_incremental()` returns `false`, so the full dataset is loaded (equivalent to a `CREATE TABLE AS SELECT`). Subsequent runs return `true` and only process new/changed rows.

### Q: How do you handle late-arriving data in incremental models?

**A:** Use a **lookback window** instead of strict watermarking:
```sql
{% if is_incremental() %}
where etl_loaded_at > dateadd(day, -3, (select max(etl_loaded_at) from {{ this }}))
{% endif %}
```
This re-processes the last 3 days, catching late arrivals at the cost of some reprocessing.

### Q: How do you do a full refresh of an incremental model?

**A:** `dbt run --full-refresh -s fct_sales` — drops and rebuilds the table from scratch.

---

## 7. Snapshots (SCD Type 2)

### Q: What is a dbt snapshot and when do you use it?

**A:** Snapshots implement **Slowly Changing Dimension Type 2** — they track the full history of a record over time by adding `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` columns.

**Use cases:**
- Customer address history
- Product price changes over time
- Audit trails for regulatory compliance

### Q: What snapshot strategies are available?

**A:**

| Strategy | How it detects changes | When to use |
|----------|----------------------|-------------|
| **timestamp** | Compares `updated_at` column | Source has reliable timestamps |
| **check** | Compares listed column values | No reliable timestamp available |

From our project:
```sql
{% snapshot snap_sales_fact %}
{{ config(
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}
select * from {{ ref('fct_sales') }}
{% endsnapshot %}
```

### Q: What does `invalidate_hard_deletes` do?

**A:** When a row disappears from the source (e.g., CDC delete), the snapshot sets `dbt_valid_to = current_timestamp` on the last version, marking it as historically closed. Without this flag, deleted rows stay open forever.

---

## 8. Macros & Jinja

### Q: What are dbt macros?

**A:** Macros are reusable SQL snippets written in Jinja. They're dbt's equivalent of functions:
```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
    ({{ column_name }} / 100)::number(18, {{ precision }})
{% endmacro %}

-- Usage in a model:
select {{ cents_to_dollars('amount_cents') }} as amount_dollars
```

### Q: What are common Jinja control structures used in dbt?

**A:**
```sql
-- Conditional logic
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

-- Loops
{% for region in ['US-EAST', 'US-WEST', 'EUROPE'] %}
    SUM(CASE WHEN region = '{{ region }}' THEN 1 ELSE 0 END) AS {{ region | lower | replace('-', '_') }}_count
    {% if not loop.last %},{% endif %}
{% endfor %}

-- Variables
{{ var('min_record_count', 100) }}
```

### Q: What are pre-hooks and post-hooks?

**A:** SQL that runs before or after a model:
```sql
{{ config(
    post_hook="{{ cdc_delete_post_hook(this, ref('stg_cdc_delete_markers')) }}"
) }}
```
Common uses: grants, CDC deletes, audit logging, index creation.

---

## 9. Packages

### Q: What are dbt packages and name some popular ones.

**A:** Packages are reusable dbt projects installed via `packages.yml`:

| Package | Purpose |
|---------|---------|
| **dbt_utils** | `generate_surrogate_key`, `star`, `pivot`, `date_spine` |
| **dbt_expectations** | Great Expectations-style column tests |
| **dbt_date** | Date dimension generator |
| **codegen** | Auto-generate model YAML and base models |
| **audit_helper** | Compare two relations for differences |

Install: `dbt deps`

From our project:
```sql
{{ dbt_utils.generate_surrogate_key(['region']) }} as region_key
```

---

## 10. Seeds

### Q: What are seeds and when should you use them?

**A:** Seeds are small CSV files that dbt loads as tables (`dbt seed`). Use for:
- Reference/lookup data (payment methods, country codes)
- Static mappings that change infrequently
- Test fixtures

**Don't use for:** large datasets (>1000 rows), frequently changing data, or sensitive data.

---

## 11. dbt with Airflow

### Q: How do you orchestrate dbt with Airflow?

**A:** Three common approaches:

**1. BashOperator (simplest):**
```python
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/dbt && dbt run --profiles-dir .'
)
```

**2. Cosmos (recommended for production):**
```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
dbt_tg = DbtTaskGroup(
    project_config=ProjectConfig("/opt/dbt/retail_sales"),
    profile_config=profile_config,
    render_config=RenderConfig(select=["tag:daily"])
)
```
Cosmos converts each dbt model into an individual Airflow task, giving per-model visibility and retry.

**3. dbt Cloud API:**
```python
trigger_dbt = SimpleHttpOperator(
    task_id='trigger_dbt_cloud',
    endpoint='/api/v2/accounts/{id}/jobs/{job_id}/run/',
    method='POST'
)
```

### Q: How would you structure the Airflow DAG with dbt?

**A:**
```
extract (Airflow) → dbt run staging → dbt run intermediate → dbt run marts → dbt test → dbt snapshot
```
Or with Cosmos, each dbt model becomes a task in the DAG automatically.

---

## 12. dbt with CDC

### Q: How does dbt handle CDC (Change Data Capture)?

**A:** dbt handles CDC through:

1. **Incremental merge** — `incremental_strategy='merge'` maps directly to SQL MERGE, handling inserts and updates
2. **Delete propagation** — Use a `post_hook` macro to delete rows flagged by CDC:
   ```sql
   post_hook="{{ cdc_delete_post_hook(this, ref('stg_cdc_delete_markers')) }}"
   ```
3. **Snapshots** — `invalidate_hard_deletes=True` closes historical records when source rows disappear
4. **Source freshness** — monitors that CDC events are flowing (not stale)

### Q: How do you handle the is_deleted flag in dbt?

**A:** Split at the staging layer:
- `stg_sales_transactions` — filters `WHERE is_deleted = false` (live rows)
- `stg_cdc_delete_markers` — filters `WHERE is_deleted = true` (delete markers)

The fact model merges live rows, then a post-hook deletes the marked rows.

---

## 13. Performance & Best Practices

### Q: How do you optimize dbt model performance?

**A:**
1. **Clustering** — `cluster_by=['sale_date', 'region']` on Snowflake
2. **Incremental** — only process new data, not full table scans
3. **Ephemeral** — intermediate models compile to CTEs (no extra tables)
4. **Warehouse sizing** — use `snowflake_warehouse` config per model
5. **Partial runs** — `dbt run -s fct_sales+` (only the model and downstream)
6. **Thread count** — increase `threads` in profiles.yml for parallelism

### Q: What is the dbt coding style guide?

**A:**
- **CTEs over subqueries** — each CTE = one logical step
- **One model per file** — named to match the output table
- **Staging models** — `stg_<source>_<entity>`
- **Fact/dimension naming** — `fct_`, `dim_`, `agg_`
- **Lowercase SQL** — `select`, `from`, `where` (not `SELECT`)
- **Trailing commas** — easier diffs in Git
- **Explicit column lists** — no `select *` in marts

### Q: What is the difference between `dbt run` and `dbt build`?

**A:**
- `dbt run` — runs models only
- `dbt build` — runs models + tests + snapshots + seeds in DAG order (recommended for production)

### Q: How do you select specific models to run?

**A:**
```bash
dbt run -s fct_sales               # Single model
dbt run -s +fct_sales              # Model + all upstream
dbt run -s fct_sales+              # Model + all downstream
dbt run -s tag:daily               # All models with tag
dbt run -s staging.stg_sales*      # Wildcard
dbt run --exclude dim_stores       # Exclude specific model
```

---

## 14. Advanced Scenarios

### Q: How do you implement unit testing in dbt?

**A:** dbt Core 1.8+ supports native unit tests:
```yaml
unit_tests:
  - name: test_net_amount_calculation
    model: int_sales_enriched
    given:
      - input: ref('stg_sales_transactions')
        rows:
          - {total_amount: 100, discount_amount: 10, tax_amount: 5}
    expect:
      rows:
        - {net_amount: 90, gross_profit: 85}
```

### Q: How do you manage environments (dev/staging/prod)?

**A:** Via `profiles.yml` targets:
```yaml
retail_sales:
  target: dev           # Default target
  outputs:
    dev:
      schema: DEV_ANALYTICS
      threads: 4
    prod:
      schema: ANALYTICS
      threads: 8
```
Override at runtime: `dbt run --target prod`

Each developer gets their own schema (`DEV_<username>_ANALYTICS`) via `generate_schema_name` macro override.

### Q: How do you handle schema changes / migrations?

**A:**
1. **Add a column** — just add it to the model SQL; dbt handles it
2. **Rename a column** — update the model, run `--full-refresh` once
3. **on_schema_change config** (incremental models):
   ```sql
   config(on_schema_change='sync_all_columns')
   ```
   Options: `ignore`, `append_new_columns`, `sync_all_columns`, `fail`

### Q: What is `generate_surrogate_key` and when do you use it?

**A:** Creates a deterministic hash key from one or more columns:
```sql
{{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key
```
Use for dimension tables where the natural key is a string (varchar) and you want a compact, consistent surrogate key for joins.

---

## Quick Reference Commands

```bash
# Core workflow
dbt deps                    # Install packages
dbt seed                    # Load CSV seeds
dbt run                     # Run all models
dbt test                    # Run all tests
dbt build                   # Run + test + snapshot + seed (DAG order)

# Selective execution
dbt run -s tag:daily        # Models tagged 'daily'
dbt run -s +fct_sales       # fct_sales and all ancestors
dbt test -s fct_sales       # Tests for fct_sales only

# Operations
dbt snapshot                # Run SCD Type 2 snapshots
dbt source freshness        # Check source data freshness
dbt docs generate && dbt docs serve  # Generate and view docs

# Debugging
dbt compile                 # Compile SQL without running
dbt debug                   # Test connection and config
dbt run --full-refresh      # Drop and recreate incremental models
dbt test --store-failures   # Save failing rows to a table
```
