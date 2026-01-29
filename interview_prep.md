# Interview Preparation Guide: Retail Sales ETL Pipeline

## Quick Stats (Memorize These)

**Project Overview:**
- **Scale:** 4 regional databases, ~2M records/day (~500K per region)
- **Performance:** 40-minute end-to-end pipeline (2-hour SLA)
- **Uptime:** 99.5% success rate over 6 months
- **Cost:** ~$500/month (Snowflake: $350, AWS: $150)

**Technology Stack:**
- Apache Airflow 2.5.3
- PostgreSQL (source databases)
- Snowflake (data warehouse)
- AWS S3 (staging)
- Python 3.9, Pandas, SQLAlchemy

---

## Core Architecture Questions

### Q1: "Walk me through the architecture of this pipeline"

**Answer Framework (3-minute response):**

"I designed a multi-stage ETL pipeline that processes sales data from 4 regional PostgreSQL databases into a centralized Snowflake warehouse.

**Extraction Phase:**
- Implemented parallel extraction using Airflow TaskGroups to pull data from 4 regions concurrently
- Each region runs independently with its own retry logic and error handling
- Reduces total extraction time from 40 minutes sequential to ~10 minutes parallel

**Transformation Phase:**
- Consolidated regional data into a unified staging table in PostgreSQL
- Applied business transformations: deduplication, type standardization, derived metrics
- Used Pandas for in-memory processing - efficient for our ~2M daily records

**Validation Phase:**
- Implemented a BranchPythonOperator for fail-fast data quality checks
- Validates record counts, NULL percentages, and business rules
- Routes to either load path (success) or alert path (failure)

**Load Phase:**
- Export to S3 as intermediate staging
- Use Snowflake COPY command for bulk load into staging table
- MERGE statement for upsert into final table - handles late-arriving data
- Post-load verification to ensure data consistency

**Key Design Decisions:**
- PostgreSQL intermediate staging for debugging and transformation flexibility
- S3 staging to leverage Snowflake's optimized COPY performance
- MERGE instead of truncate-insert for idempotency and handling duplicates"

**Follow-up Prep:**
- Be ready to explain *why* each stage exists
- Know the performance characteristics of each phase
- Understand trade-offs (e.g., why S3 vs direct load)

---

### Q2: "How do you handle failures in this pipeline?"

**Answer Framework (2-minute response):**

"I implemented multi-layered error handling:

**Task-Level Retries:**
- 2 retries with 5-minute delay and exponential backoff
- Handles transient failures (network glitches, database locks)
- Retry_exponential_backoff=True increases delay: 5min, 10min, 20min

**Data Quality Branching:**
- BranchPythonOperator creates fail-fast path
- If validation fails, immediately alerts and stops load
- Prevents corrupted data from reaching production

**Graceful Degradation:**
- If one region fails after retries, pipeline continues with other regions
- Sends alert but doesn't fail entire DAG
- Backfill DAG can process missing dates later

**Monitoring & Alerting:**
- Email alerts on task failures with diagnostic info
- CloudWatch logs for troubleshooting
- XCom metadata tracks extraction counts per region

**Recovery Process:**
- Batch ID tracking enables idempotent reruns
- Can clear specific date and rerun: `airflow dags backfill`
- Snowflake MERGE handles duplicate prevention on rerun

**Example Failure Scenario:**
Europe database is down → 3 extraction tasks succeed, 1 fails after retries → Transform continues with 3 regions → Alert sent → Next day, backfill processes missing Europe data → MERGE handles any overlaps"

---

### Q3: "How do you ensure data quality?"

**Answer Framework (2-minute response):**

"Implemented three-layer validation strategy:

**Layer 1: Source Extraction Filters**
```sql
WHERE is_deleted = FALSE 
  AND status = 'completed'
  AND sale_date = YESTERDAY
```
- Only valid, completed transactions
- Prevents partial/invalid data entry

**Layer 2: Transformation Validations**
- Remove duplicates (by sale_id)
- Filter invalid records: quantity > 0, price > 0
- Sanity checks: total_amount within 0.5x to 2x of (quantity * unit_price)
- NULL handling: fill discount/tax with 0, fail on critical NULLs

**Layer 3: Pre-Load Quality Gate** (BranchOperator decision point)
- Minimum record threshold: Fail if < 100 records (suggests extraction issue)
- NULL percentage: Fail if > 5% NULLs in critical columns
- Business logic: Fail if any records violate constraints
- Comparative check: Compare to historical averages (future enhancement)

**Layer 4: Post-Load Verification**
- Record count reconciliation: Source extracted vs Snowflake loaded
- Duplicate check in final table
- Date range validation

**Monitoring:**
- Track validation metrics in XCom
- Log all filtered/rejected records for investigation
- Weekly data quality reports

**Trade-offs:**
- Strict validation (< 100 records = fail) vs flexible (load what we have)
- Chose strict for production data integrity
- Alert allows investigation before fixing source"

---

### Q4: "How would you scale this to 10x the data volume?"

**Answer Framework (3-minute response):**

"Current: ~2M records/day, 40-minute runtime
Target: ~20M records/day

**Immediate Optimizations (2x-3x improvement):**

1. **Parallel Workers:**
   - Increase Airflow parallelism from 4 to 16
   - Run 4 extractions + 4 loads concurrently
   - Requires more compute resources

2. **Batch Size Optimization:**
   - Increase Snowflake COPY chunk size
   - Use multi-part uploads to S3
   - Enable Snowflake warehouse auto-scaling

3. **Transformation Efficiency:**
   - Use SQLAlchemy batch inserts (currently 1000 rows/batch)
   - Implement connection pooling
   - Partition data by region for parallel processing

**Major Architecture Changes (10x scale):**

1. **Replace Pandas with PySpark:**
   ```
   Current: In-memory Pandas (works for 2M records)
   Future: PySpark on EMR/Databricks (scales to billions)
   
   Benefits:
   - Distributed processing across cluster
   - Partition-based transformations
   - Direct write to S3 in partitioned format
   ```

2. **Partitioned Data Strategy:**
   ```
   S3 Structure:
   /sales_data/year=2024/month=01/region=us_east/*.parquet
   
   Benefits:
   - Parallel loads from multiple partitions
   - Snowflake external tables
   - Incremental processing
   ```

3. **Streaming Architecture (Lambda Architecture):**
   ```
   Real-time: Kinesis → Lambda → Snowflake (hot path)
   Batch: Current Airflow (cold path, corrections)
   
   Benefits:
   - Sub-minute latency for BI dashboards
   - Batch path handles late-arriving data
   ```

4. **Snowflake Optimizations:**
   - Scale warehouse from MEDIUM → X-LARGE during load
   - Use clustering keys: (sale_date, region)
   - Implement micro-partitions
   - Auto-suspend after 5 minutes

5. **Database Sharding:**
   - If single region > 5M records, shard by store_id
   - Multiple extraction tasks per region

**Performance Targets:**
- 10x data: 40 min → 60-90 min (with optimizations)
- With PySpark: 60-90 min → 30-40 min (distributed)

**Cost Considerations:**
- Current: $500/month
- 10x scale: ~$2500/month (Snowflake scales linearly)
- PySpark on EMR: Additional $500-1000/month

**Monitoring Changes:**
- Add per-partition metrics
- Track Snowflake warehouse utilization
- Implement auto-scaling triggers"

---

### Q5: "How do you handle schema changes in source systems?"

**Answer Framework (2-minute response):**

"Implemented flexible schema evolution strategy:

**Current Approach:**

1. **Explicit Column Selection:**
   ```python
   # Not SELECT * - explicit columns
   SELECT sale_id, customer_id, ..., updated_at
   ```
   - Breaking changes (removed column) fail fast
   - New columns don't break pipeline

2. **Envelope Pattern (Future Enhancement):**
   ```python
   # Store raw JSON + parsed columns
   raw_json = json.dumps(record)
   parsed_data = extract_known_fields(record)
   ```
   - Captures unknown fields
   - Allows retroactive parsing

3. **Backward-Compatible Changes:**
   - Add new columns with default values
   - Mark old columns deprecated, support both
   - Coordinate change windows with source teams

**Schema Change Process:**

**Non-Breaking (New Column):**
```
1. Source adds `loyalty_points` column
2. Airflow pipeline continues (explicit SELECT)
3. Submit PR: Add loyalty_points to extraction
4. Deploy with feature flag
5. Backfill historical data if needed
```

**Breaking (Rename/Remove Column):**
```
1. Source deprecates `customer_id`, adds `customer_uuid`
2. Coordinate migration window
3. Deploy dual-support code:
   - Try customer_uuid first
   - Fallback to customer_id
4. After migration period, remove fallback
```

**Data Type Changes:**
```
1. Source changes price: INT → DECIMAL
2. Transformation layer handles casting
3. Validate ranges match
4. Alert on unexpected values
```

**Monitoring:**
- Schema comparison on each run (optional check)
- Alert on unexpected columns
- Log any parsing failures

**Testing:**
- Integration tests with mocked schema changes
- Canary deployments in staging environment

**Alternative for Frequent Changes:**
- Use schema registry (Confluent Schema Registry)
- Version schema in metadata
- Automated compatibility checks"

---

## Technical Deep Dives

### Airflow Concepts

**Q: Explain TaskGroups vs SubDAGs**

"Used TaskGroup for parallel extraction instead of SubDAG:

**TaskGroups (My Choice):**
- Lightweight grouping for UI organization
- Share same DagRun context
- Better performance, simpler debugging
- Perfect for parallel extraction pattern

**SubDAGs (Deprecated):**
- Separate DAG execution
- Own deadlock detection, separate scheduler
- More overhead, harder to debug
- Avoided in modern Airflow

**Implementation:**
```python
with TaskGroup(group_id='extract_data') as extract_data:
    for region in REGIONS:
        PythonOperator(task_id=f'extract_{region}', ...)
```

Shows as: `extract_data.extract_us_east`"

---

**Q: Why BranchPythonOperator for validation?**

"BranchOperator enables fail-fast without failing entire DAG:

**Alternative 1: Raise Exception**
- Fails DAG completely
- No graceful alerting
- Harder to debug

**Alternative 2: Proceed with bad data**
- Corrupts production data
- Violates data integrity

**BranchOperator Approach:**
```python
def validate_data_quality():
    if data_quality_fails:
        return 'data_quality_failure'  # Alert path
    return 'load_to_snowflake'  # Success path
```

**Benefits:**
- Explicit branching in DAG graph
- Can add recovery logic in failure branch
- Clear separation of success/failure paths
- DAG shows as 'success' with skipped tasks (not failed)"

---

### Snowflake Specific

**Q: Why MERGE instead of truncate-and-load?**

**Answer:**

"MERGE (upsert) provides critical benefits:

**Idempotency:**
```sql
-- Can rerun same batch_id multiple times safely
MERGE INTO sales_fact target
USING sales_stage source
ON target.sale_id = source.sale_id
-- Duplicate sale_ids are updated, not inserted again
```

**Handle Late-Arriving Data:**
```
Day 1: Load 1000 records for Jan 15
Day 2: Load 50 additional records for Jan 15 (late data)
Result: 1050 total records, no duplicates
```

**Partial Reprocessing:**
```
If Europe extraction fails on Day 1:
- Load US/APAC data
Day 2:
- Reprocess Europe for Day 1
- MERGE handles overlap with existing US/APAC
```

**Audit Trail:**
```sql
-- Track updates vs inserts
SELECT 
  COUNT(*) FILTER(WHERE created_at = updated_at) as new_records,
  COUNT(*) FILTER(WHERE created_at < updated_at) as updated_records
```

**Performance:**
- Clustered table (sale_date, region)
- MERGE leverages clustering for efficient lookups
- Only ~5% of records typically updated

**Alternative Considered:**
Truncate partition + INSERT:
- Faster for full partition reload
- Loses update tracking
- Risk of data loss if partial load fails"

---

## Behavioral Scenarios

### Scenario 1: Production Incident

**"Your pipeline failed at 7 AM, business needs data by 9 AM for board meeting. Walk me through your response."**

**Structured Response (STAR Format):**

**Situation:**
"Pipeline failed during Snowflake load phase. Data quality validated, but MERGE statement timed out."

**Task:**
"Need to recover within 2 hours for board meeting. Data is in S3 staging but not in Snowflake."

**Action:**

1. **Immediate Assessment (5 min):**
   ```bash
   # Check Airflow logs
   airflow tasks logs retail_sales_etl merge_to_final 2024-01-15
   
   # Check Snowflake query history
   SELECT * FROM INFORMATION_SCHEMA.QUERY_HISTORY 
   WHERE QUERY_TEXT LIKE '%sales_fact%'
   ORDER BY START_TIME DESC;
   ```
   - Identified: Table lock from concurrent analytical query
   
2. **Quick Fix (15 min):**
   ```sql
   -- Kill blocking query (coordinate with analytics team)
   SELECT SYSTEM$CANCEL_QUERY('query_id');
   
   -- Clear failed task
   airflow tasks clear retail_sales_etl_pipeline \
     --task-regex 'merge.*' \
     --start-date 2024-01-15 \
     --yes
   
   -- Rerun from merge step
   airflow tasks run retail_sales_etl_pipeline \
     merge_to_final 2024-01-15 --local
   ```

3. **Monitoring (10 min):**
   - Watch Airflow UI for task progress
   - Verify record counts in Snowflake staging
   - Run verification queries

4. **Communication:**
   - Slack update to stakeholders: "Investigating failure, ETA 30 min"
   - Update when resolved: "Pipeline recovered, data available"

**Result:**
"Recovered in 35 minutes. Implemented warehouse reservation for future ETL loads to prevent lock contention. Added retry logic with warehouse scaling."

**Learning:**
"Improved monitoring: Added CloudWatch alarm for long-running queries. Implemented separate warehouses for ETL vs Analytics."

---

### Scenario 2: Optimization Request

**"Business wants to add 5 more regions. Current pipeline is at 80% of SLA. How do you handle?"**

**Answer:**

**Analysis:**
```
Current: 4 regions, 40 min (80% of 50 min budget)
Adding 5 regions = 9 regions total
Linear scaling: 40 * (9/4) = 90 min → Exceeds 2-hour SLA? 

Wait: Extraction is parallel (10 min regardless of region count, assuming sufficient workers)
Real bottleneck: Transformation + Load (30 min)
```

**Solution:**

1. **Immediate (No Architecture Change):**
   - Increase Airflow workers from 4 to 10
   - Optimize transformation: Use SQLAlchemy bulk operations
   - Snowflake: Scale warehouse MEDIUM → LARGE during load
   - **Result:** 40 min → 45-50 min (acceptable)

2. **Medium-Term:**
   - Partition transformation by region:
     ```python
     with TaskGroup('transform_by_region'):
         for region in REGIONS:
             transform_region(region)
     ```
   - Parallel loads to Snowflake (9 COPY commands concurrently)
   - **Result:** 40 min → 35-40 min

3. **Long-Term (If growth continues):**
   - Migrate to PySpark for transformation
   - Implement micro-batching (load every 4 hours vs daily)
   - **Result:** Scales to 50+ regions

**Communication to Business:**
"Adding 5 regions is feasible. Need:
- 2 weeks: Implement optimizations
- Resource cost: +$200/month (larger warehouse)
- Minimal risk: Changes tested in staging
- Future-proof: Supports up to 15 regions before next redesign"

---

## Common Pitfalls & How You Avoided Them

### Pitfall 1: Hardcoded Values

**Bad:**
```python
postgres_conn_id='my_postgres_connection'
snowflake_warehouse='PROD_WAREHOUSE'
```

**Your Implementation:**
```python
postgres_conn_id=f'postgres_{region}'  # Dynamic per region
snowflake_conn_id=Variable.get('SNOWFLAKE_CONN_ID')  # Configurable
```

**Why It Matters:**
- Environment portability (dev/staging/prod)
- Easy to change without code deploy
- Supports multiple deployments

---

### Pitfall 2: No Idempotency

**Bad:**
```sql
INSERT INTO sales_fact SELECT * FROM staging;
-- Rerun creates duplicates
```

**Your Implementation:**
```sql
MERGE INTO sales_fact target
USING staging source
ON target.sale_id = source.sale_id;
-- Safe to rerun
```

**Why It Matters:**
- Airflow retries don't create duplicates
- Manual reruns for debugging
- Backfilling historical data

---

### Pitfall 3: Silent Failures

**Bad:**
```python
try:
    extract_data()
except:
    pass  # Silently fails
```

**Your Implementation:**
```python
try:
    metadata = extract_data()
    ti.xcom_push('extraction_metadata', metadata)
except Exception as e:
    logger.error(f"Extraction failed: {e}")
    raise  # Fail loudly, trigger retries/alerts
```

**Why It Matters:**
- Fast failure detection
- Proper alerting
- Debugging information preserved

---

## Questions to Ask Interviewer

**Good Questions (Show Technical Depth):**

1. "What's the current scale of your data pipelines, and what challenges are you facing at that scale?"

2. "How do you handle schema evolution across your data sources?"

3. "What's your approach to data quality and validation? Do you have any interesting patterns?"

4. "Are you using Lambda Architecture or Kappa Architecture for real-time data?"

5. "How do you balance cost optimization with performance in your Snowflake/cloud data platforms?"

**Avoid These Questions:**
- "What does this team do?" (Should know from research)
- Basic tool questions ("Do you use Airflow?")
- Salary/WLB in technical round (Save for recruiter)

---

## Red Flags to Avoid

**Don't Say:**
- "I just followed a tutorial" (Shows lack of understanding)
- "We never had issues" (Unrealistic, suggests inexperience)
- "I don't know" without attempting to reason through it
- "That's not my job" (Shows lack of initiative)

**Do Say:**
- "I haven't used X, but here's how I'd approach it based on my experience with Y"
- "We had several challenges, here's how I addressed them"
- "Let me think through this..." (Shows problem-solving)
- "I took ownership of the entire pipeline, including ops"

---

## Closing Statement Template

**When asked "Any questions for me?":**

"I'm really excited about this role. Based on our conversation, I believe my experience with production ETL pipelines—especially handling multi-regional data consolidation, ensuring data quality, and optimizing for scale—aligns well with your team's challenges.

I'm particularly interested in [specific technology/project mentioned] and would love to contribute to that area. I'm eager to learn more about [specific team practice/tool mentioned].

[Ask your technical question here]"

---

## Final Preparation Checklist

- [ ] Memorize architecture diagram flow
- [ ] Know exact numbers (record counts, timing, costs)
- [ ] Prepare 2-3 STAR stories for behavioral questions
- [ ] Review Airflow concepts (XCom, TaskGroups, BranchOperator)
- [ ] Brush up on Snowflake MERGE syntax
- [ ] Practice whiteboard system design (30 min)
- [ ] Review company's tech blog / recent projects
- [ ] Prepare questions for interviewer
- [ ] Set up local environment to demo if needed
- [ ] Review this guide 1 day before interview

---

**Remember:**
- Be confident but honest about your experience
- Use specific examples from this project
- Connect your experience to their needs
- Show passion for data engineering
- Ask thoughtful questions

**Good Luck! 🚀**
