# Architecture Diagrams - Retail Sales ETL Pipeline

## 1. High-Level Data Flow

```mermaid
graph TB
    subgraph "Source Systems"
        A1[(US East<br/>PostgreSQL)]
        A2[(US West<br/>PostgreSQL)]
        A3[(Europe<br/>PostgreSQL)]
        A4[(Asia Pacific<br/>PostgreSQL)]
    end
    
    subgraph "Apache Airflow Orchestration"
        B1[Extract US East]
        B2[Extract US West]
        B3[Extract Europe]
        B4[Extract APAC]
        C[Transform & Unify Data]
        D{Data Quality<br/>Validation}
        E[Prepare Load]
    end
    
    subgraph "Staging & Load"
        F[(PostgreSQL<br/>Staging)]
        G[(S3<br/>Staging)]
        H[(Snowflake<br/>Stage Table)]
    end
    
    subgraph "Data Warehouse"
        I[(Snowflake<br/>Analytics DB)]
    end
    
    subgraph "Monitoring"
        J[CloudWatch Logs]
        K[Email Alerts]
    end
    
    A1 -->|Extract| B1
    A2 -->|Extract| B2
    A3 -->|Extract| B3
    A4 -->|Extract| B4
    
    B1 --> C
    B2 --> C
    B3 --> C
    B4 --> C
    
    C --> F
    C --> D
    
    D -->|Pass| E
    D -->|Fail| K
    
    E --> G
    G --> H
    H -->|MERGE| I
    
    B1 -.-> J
    C -.-> J
    D -.-> J
    E -.-> J
```

## 2. Detailed Airflow DAG Structure

```mermaid
graph TB
    START([Start]) --> TG[Task Group:<br/>Extract Data]
    
    subgraph TG[" "]
        E1[extract_us_east]
        E2[extract_us_west]
        E3[extract_europe]
        E4[extract_asia_pacific]
    end
    
    TG --> TRANSFORM[transform_sales_data]
    TRANSFORM --> VALIDATE{validate_data_quality<br/>BranchOperator}
    
    VALIDATE -->|Quality Pass| PREP[load_to_snowflake<br/>prepare load]
    VALIDATE -->|Quality Fail| FAIL[data_quality_failure<br/>send alerts]
    
    PREP --> STAGE[stage_data_in_snowflake<br/>COPY to staging]
    STAGE --> MERGE[merge_to_final_table<br/>UPSERT]
    MERGE --> VERIFY[verify_snowflake_load<br/>count check]
    
    VERIFY --> SUCCESS([pipeline_success])
    FAIL --> END([End])
    SUCCESS --> END
    
    style VALIDATE fill:#ffeb3b
    style FAIL fill:#f44336,color:#fff
    style SUCCESS fill:#4caf50,color:#fff
    style TG fill:#e3f2fd
```

## 3. Data Transformation Flow

```mermaid
graph LR
    subgraph "Input: Raw Regional Data"
        A[sale_id, customer_id,<br/>product_id, sale_date,<br/>quantity, unit_price,<br/>total_amount, region]
    end
    
    subgraph "Transformation Steps"
        B[1. Consolidate<br/>All Regions]
        C[2. Remove<br/>Duplicates]
        D[3. Standardize<br/>Data Types]
        E[4. Calculate<br/>Derived Columns]
        F[5. Add<br/>Partition Columns]
        G[6. Normalize<br/>Region Codes]
        H[7. Apply<br/>Business Rules]
    end
    
    subgraph "Output: Unified Data"
        I[sale_id, customer_id,<br/>product_id, sale_date,<br/>quantity, unit_price,<br/>total_amount, region,<br/>net_amount, gross_profit,<br/>sale_year, sale_month,<br/>etl_loaded_at, etl_batch_id]
    end
    
    A --> B --> C --> D --> E --> F --> G --> H --> I
    
    style B fill:#e1f5fe
    style C fill:#e1f5fe
    style D fill:#e1f5fe
    style E fill:#fff9c4
    style F fill:#fff9c4
    style G fill:#fff9c4
    style H fill:#c8e6c9
```

## 4. Data Quality Validation Flow

```mermaid
graph TD
    START([Transformed Data]) --> CHECK1{Record Count<br/>>= 100?}
    
    CHECK1 -->|No| FAIL1[❌ Below Threshold]
    CHECK1 -->|Yes| CHECK2{NULL % in<br/>Critical Columns<br/><= 5%?}
    
    CHECK2 -->|No| FAIL2[❌ Too Many NULLs]
    CHECK2 -->|Yes| CHECK3{Business Rules<br/>Valid?<br/>qty>0, price>0}
    
    CHECK3 -->|No| FAIL3[❌ Invalid Data]
    CHECK3 -->|Yes| CHECK4{Amount Sanity<br/>Check?<br/>0.5x to 2x qty*price}
    
    CHECK4 -->|No| FAIL4[❌ Suspicious Values]
    CHECK4 -->|Yes| PASS[✅ All Checks Pass]
    
    FAIL1 --> ALERT[Send Alert &<br/>Skip Load]
    FAIL2 --> ALERT
    FAIL3 --> ALERT
    FAIL4 --> ALERT
    
    PASS --> LOAD[Proceed to<br/>Snowflake Load]
    
    style PASS fill:#4caf50,color:#fff
    style FAIL1 fill:#f44336,color:#fff
    style FAIL2 fill:#f44336,color:#fff
    style FAIL3 fill:#f44336,color:#fff
    style FAIL4 fill:#f44336,color:#fff
    style ALERT fill:#ff9800,color:#fff
```

## 5. Snowflake Load Strategy (MERGE Upsert)

```mermaid
sequenceDiagram
    participant S3 as S3 Staging
    participant Stage as Snowflake<br/>Stage Table
    participant Final as Snowflake<br/>Final Table
    
    Note over S3: CSV files uploaded<br/>from transformation
    
    S3->>Stage: COPY INTO stage_table<br/>FROM S3
    
    Note over Stage: Temporary staging<br/>for current batch
    
    Stage->>Final: MERGE INTO final_table
    
    Note over Final: WHEN MATCHED:<br/>UPDATE existing records
    
    Note over Final: WHEN NOT MATCHED:<br/>INSERT new records
    
    Final-->>Final: Verify record counts
    
    Note over Final: Final table with<br/>upserted data
```

## 6. Error Handling & Retry Strategy

```mermaid
graph TD
    START([Task Execution]) --> TRY1[Attempt 1]
    
    TRY1 -->|Success| SUCCESS[✅ Task Complete]
    TRY1 -->|Failure| WAIT1[Wait 5 min]
    
    WAIT1 --> TRY2[Attempt 2<br/>Retry 1/2]
    
    TRY2 -->|Success| SUCCESS
    TRY2 -->|Failure| WAIT2[Wait 10 min<br/>exponential backoff]
    
    WAIT2 --> TRY3[Attempt 3<br/>Retry 2/2]
    
    TRY3 -->|Success| SUCCESS
    TRY3 -->|Failure| FAIL[❌ Task Failed]
    
    FAIL --> ALERT[Send Email Alert]
    ALERT --> LOG[Log Error Details]
    LOG --> NOTIFY[Notify On-Call]
    
    style SUCCESS fill:#4caf50,color:#fff
    style FAIL fill:#f44336,color:#fff
    style ALERT fill:#ff9800,color:#fff
```

## 7. Monitoring & Observability

```mermaid
graph TB
    subgraph "Data Pipeline"
        A[Extract Tasks]
        B[Transform Task]
        C[Validation Task]
        D[Load Tasks]
    end
    
    subgraph "Airflow Metadata"
        E[(XCom Store)]
        F[Task Logs]
        G[DAG Run History]
    end
    
    subgraph "Monitoring Systems"
        H[CloudWatch Logs]
        I[CloudWatch Metrics]
        J[Email Alerts]
    end
    
    subgraph "Dashboards"
        K[Grafana<br/>Real-time Metrics]
        L[Airflow UI<br/>DAG Status]
    end
    
    A --> E
    B --> E
    C --> E
    D --> E
    
    A --> F
    B --> F
    C --> F
    D --> F
    
    F --> H
    E --> I
    G --> I
    
    C -->|Failures| J
    D -->|Failures| J
    
    H --> K
    I --> K
    G --> L
    
    style H fill:#ff9800
    style I fill:#ff9800
    style K fill:#2196f3,color:#fff
    style L fill:#2196f3,color:#fff
```

## 8. Scaling Strategy Comparison

```mermaid
graph TD
    subgraph "Current Architecture<br/>2M records/day"
        A1[4 Regions<br/>Parallel Extract]
        A2[Pandas<br/>Transformation]
        A3[Single S3 File]
        A4[Snowflake MEDIUM<br/>Warehouse]
    end
    
    subgraph "10x Scale Architecture<br/>20M records/day"
        B1[9+ Regions<br/>Parallel Extract]
        B2[PySpark on EMR<br/>Distributed Transform]
        B3[Partitioned S3<br/>Multiple Files]
        B4[Snowflake LARGE<br/>Auto-scaling]
    end
    
    A1 -.->|Scale| B1
    A2 -.->|Upgrade| B2
    A3 -.->|Partition| B3
    A4 -.->|Scale| B4
    
    style A1 fill:#e3f2fd
    style A2 fill:#e3f2fd
    style A3 fill:#e3f2fd
    style A4 fill:#e3f2fd
    
    style B1 fill:#c8e6c9
    style B2 fill:#c8e6c9
    style B3 fill:#c8e6c9
    style B4 fill:#c8e6c9
```

## 9. Regional Extraction Pattern (Parallel)

```mermaid
gantt
    title Parallel Extraction Timeline (40 min → 10 min)
    dateFormat HH:mm
    axisFormat %H:%M
    
    section Sequential (Old)
    US East      :a1, 06:00, 10m
    US West      :a2, after a1, 10m
    Europe       :a3, after a2, 10m
    Asia Pacific :a4, after a3, 10m
    
    section Parallel (New)
    US East      :b1, 06:00, 10m
    US West      :b2, 06:00, 10m
    Europe       :b3, 06:00, 10m
    Asia Pacific :b4, 06:00, 10m
```

## 10. Cost Breakdown (Monthly)

```mermaid
pie title Monthly Cost: $500
    "Snowflake Compute (MEDIUM WH)" : 250
    "Snowflake Storage (1TB)" : 100
    "AWS S3 Storage" : 50
    "AWS Data Transfer" : 50
    "Airflow (EC2)" : 50
```

---

## Key Metrics Dashboard

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| **Pipeline Duration** | 40 min | < 120 min | ✅ |
| **Success Rate** | 99.5% | > 99% | ✅ |
| **Record Volume** | 2M/day | - | ✅ |
| **Data Quality** | < 0.1% errors | < 1% | ✅ |
| **Cost** | $500/month | < $1000 | ✅ |
| **Parallelism** | 4 regions | Scalable | ✅ |

---

## Technologies Used

```
┌─────────────────────────────────────────────────┐
│              Orchestration Layer                │
│  ┌───────────────────────────────────────────┐ │
│  │       Apache Airflow 2.5.3                │ │
│  │  • TaskGroups for parallel execution      │ │
│  │  • BranchOperator for validation routing  │ │
│  │  • XCom for inter-task communication      │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│               Source & Staging                  │
│  ┌───────────────────────────────────────────┐ │
│  │  PostgreSQL 12+                           │ │
│  │  • 4 regional databases (sources)         │ │
│  │  • 1 staging database (transformation)    │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│            Transformation Layer                 │
│  ┌───────────────────────────────────────────┐ │
│  │  Python 3.9 + Pandas                      │ │
│  │  • Data consolidation & cleaning          │ │
│  │  • Business logic transformation          │ │
│  │  • SQLAlchemy for bulk operations         │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│              Intermediate Storage               │
│  ┌───────────────────────────────────────────┐ │
│  │  AWS S3                                   │ │
│  │  • Staging area for Snowflake COPY       │ │
│  │  • CSV format with compression            │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│               Data Warehouse                    │
│  ┌───────────────────────────────────────────┐ │
│  │  Snowflake                                │ │
│  │  • Staging tables for COPY                │ │
│  │  • Final analytics tables with clustering │ │
│  │  • MERGE for idempotent loads             │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```
