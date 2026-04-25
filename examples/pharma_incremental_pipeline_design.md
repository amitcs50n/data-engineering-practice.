# End-to-End Incremental Data Pipeline (Pharmaceutical Sales)

## 1) High-Level Architecture

**Source**: MySQL (`pharma_sales`)  
**Orchestration**: Azure Data Factory (ADF)  
**Connectivity**: Azure Integration Runtime (Self-hosted IR if MySQL is private/on-prem; Azure IR if publicly reachable)  
**Raw Zone**: Azure Blob Storage (Parquet, partitioned by load date/hour)  
**Transformation**: Azure Databricks (PySpark)  
**Target**: Snowflake (`STG_PHARMA_SALES`, `PHARMA_SALES`)  
**Control/Audit**: Cloud control table (`pipeline_watermark_audit` in Azure SQL/Snowflake control schema) + optional stage-level log table

---

## 2) Source and Control Tables

### Where should the audit/watermark table live?

**Production recommendation: do _not_ write audit/watermark state back to on-prem/source MySQL.**

Use a **cloud control store** instead (for example Azure SQL DB, ADF metadata DB, or Snowflake control schema), while keeping source MySQL read-only from ADF.

Why this is preferred in enterprise environments:
- Preserves the **read-only source principle** (no control-plane writes into OLTP source).
- Avoids additional inbound write paths to on-prem (reduced firewall/security/governance risk).
- Separates transactional source responsibilities from pipeline orchestration metadata.
- Easier central operations for multiple pipelines and environments (dev/test/prod).

When MySQL-side watermark can still be acceptable:
- Source is cloud-managed and not on-prem.
- Security/governance explicitly allows controlled writes.
- You own source DB operations and have clear blast-radius controls.

Practical compromise (common pattern):
- Store **watermark/control state** in Azure SQL (or Snowflake control schema).
- Store **observability/run logs** in Snowflake for centralized analytics.
- Keep extraction query against MySQL strictly read-only.

### Source Table (MySQL)

```sql
CREATE TABLE pharma_sales (
    sale_id INT PRIMARY KEY,
    drug_name VARCHAR(100),
    batch_number VARCHAR(50),
    manufacturer VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2),
    sale_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);
```

### Watermark Audit Table (MySQL, optional source-side variant)

```sql
CREATE TABLE pipeline_watermark_audit (
    pipeline_name VARCHAR(200) PRIMARY KEY,
    last_watermark TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- seed value (example)
INSERT INTO pipeline_watermark_audit(pipeline_name, last_watermark)
VALUES ('pl_pharma_sales_incremental', '1900-01-01 00:00:00')
ON DUPLICATE KEY UPDATE last_watermark = last_watermark;
```

### Optional Pipeline Run Log Table

```sql
CREATE TABLE pipeline_run_log (
    run_id VARCHAR(100),
    pipeline_name VARCHAR(200),
    stage_name VARCHAR(100),
    status VARCHAR(20),            -- SUCCESS / FAILED / SKIPPED
    message TEXT,
    records_processed INT,
    max_watermark TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Alternative Cloud Control Table (recommended for on-prem MySQL sources)

```sql
-- Place this in Azure SQL DB (or equivalent cloud control database)
CREATE TABLE pipeline_watermark_audit (
    pipeline_name VARCHAR(200) PRIMARY KEY,
    last_watermark DATETIME2 NOT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
```

---

## 3) ADF Pipeline Design (Step-by-Step Activities)

Pipeline name: `pl_pharma_sales_incremental`

### Activity Flow

1. **LookupLastWatermark** (Lookup)
   - Query `pipeline_watermark_audit` in the **cloud control DB** by `pipeline_name`.
   - Returns `last_watermark`.

2. **CopyMySQLToBlobIncremental** (Copy Activity)
   - Source: MySQL table/query.
   - Sink: Blob Storage Parquet path:
     `raw/pharma_sales/load_date=@{formatDateTime(utcNow(),'yyyy-MM-dd')}/load_hour=@{formatDateTime(utcNow(),'HH')}/`
   - Uses **Azure Integration Runtime** for source connectivity.
   - Filter query uses watermark expression.

3. **GetBatchMaxWatermark** (Lookup)
   - Query MySQL for max `updated_at` in the extracted window.
   - If null (no rows), skip watermark update.

4. **IfRowsExtracted** (If Condition)
   - True branch: trigger Databricks notebook.
   - False branch: log “no new data” and end.

5. **DatabricksTransformAndLoad** (Databricks Notebook Activity)
   - Read raw parquet from current load folder.
   - Clean, standardize, deduplicate.
   - Write to Snowflake staging table.
   - Run Snowflake `MERGE` into final table.

6. **UpdateWatermark** (Stored Procedure or Script Activity)
   - Update control DB `pipeline_watermark_audit.last_watermark = batch_max_watermark` after successful Snowflake merge.

7. **LogSuccess / LogFailure**
   - Record status per stage in `pipeline_run_log`.

---

## 4) ADF Dynamic Expressions for Watermark Logic

### A) Lookup query (last watermark)

```sql
SELECT COALESCE(last_watermark, '1900-01-01 00:00:00') AS last_watermark
FROM pipeline_watermark_audit
WHERE pipeline_name = 'pl_pharma_sales_incremental';
```

### B) Copy source query with dynamic watermark

Use Copy source **Query** (dynamic content):

```adf
@concat(
  'SELECT sale_id, drug_name, batch_number, manufacturer, quantity, price, sale_date, created_at, updated_at, is_deleted ',
  'FROM pharma_sales ',
  'WHERE updated_at > ''',
  activity('LookupLastWatermark').output.firstRow.last_watermark,
  ''''
)
```

### C) Batch max watermark query

```adf
@concat(
  'SELECT MAX(updated_at) AS max_wm FROM pharma_sales WHERE updated_at > ''',
  activity('LookupLastWatermark').output.firstRow.last_watermark,
  ''''
)
```

### D) If condition to check rows copied

```adf
@greater(activity('CopyMySQLToBlobIncremental').output.rowsCopied, 0)
```

### E) Update watermark SQL (Script/Stored Proc input)

```adf
@concat(
  'UPDATE pipeline_watermark_audit SET last_watermark = ''',
  activity('GetBatchMaxWatermark').output.firstRow.max_wm,
  ''', updated_at = CURRENT_TIMESTAMP ',
  'WHERE pipeline_name = ''pl_pharma_sales_incremental'';'
)
```

---

## 5) Optional ADF Pipeline JSON Skeleton

```json
{
  "name": "pl_pharma_sales_incremental",
  "properties": {
    "activities": [
      {"name": "LookupLastWatermark", "type": "Lookup"},
      {"name": "CopyMySQLToBlobIncremental", "type": "Copy", "dependsOn": ["LookupLastWatermark"]},
      {"name": "GetBatchMaxWatermark", "type": "Lookup", "dependsOn": ["CopyMySQLToBlobIncremental"]},
      {"name": "IfRowsExtracted", "type": "IfCondition", "dependsOn": ["GetBatchMaxWatermark"]},
      {"name": "DatabricksTransformAndLoad", "type": "DatabricksNotebook", "dependsOn": ["IfRowsExtracted"]},
      {"name": "UpdateWatermark", "type": "SqlServerStoredProcedure", "dependsOn": ["DatabricksTransformAndLoad"]}
    ]
  }
}
```

---

## 6) Databricks PySpark Code (Transform + Stage Load)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Parameters from ADF
raw_path = dbutils.widgets.get("raw_path")
sf_db = dbutils.widgets.get("sf_database")
sf_schema = dbutils.widgets.get("sf_schema")
sf_wh = dbutils.widgets.get("sf_warehouse")
sf_role = dbutils.widgets.get("sf_role")

sfOptions = {
    "sfURL": dbutils.secrets.get("kv-scope", "sf-url"),
    "sfUser": dbutils.secrets.get("kv-scope", "sf-user"),
    "sfPassword": dbutils.secrets.get("kv-scope", "sf-password"),
    "sfDatabase": sf_db,
    "sfSchema": sf_schema,
    "sfWarehouse": sf_wh,
    "sfRole": sf_role
}

# 1) Read raw parquet
raw_df = spark.read.parquet(raw_path)

# 2) Standardize / trim strings
clean_df = (
    raw_df
    .withColumn("drug_name", F.upper(F.trim(F.col("drug_name"))))
    .withColumn("batch_number", F.upper(F.trim(F.col("batch_number"))))
    .withColumn("manufacturer", F.upper(F.trim(F.col("manufacturer"))))
)

# 3) Null handling
clean_df = (
    clean_df
    .fillna({
        "drug_name": "UNKNOWN_DRUG",
        "batch_number": "UNKNOWN_BATCH",
        "manufacturer": "UNKNOWN_MANUFACTURER",
        "quantity": 0,
        "price": 0.0,
        "is_deleted": False
    })
)

# 4) Dedup by sale_id; keep latest updated_at
w = Window.partitionBy("sale_id").orderBy(F.col("updated_at").desc(), F.col("created_at").desc())
dedup_df = (
    clean_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 5) Write to Snowflake staging table
(
    dedup_df.write
    .format("snowflake")
    .options(**sfOptions)
    .option("dbtable", "STG_PHARMA_SALES")
    .mode("append")
    .save()
)

# 6) Run merge SQL from Databricks to Snowflake
merge_sql = """
MERGE INTO PHARMA_SALES T
USING (
    SELECT
        sale_id, drug_name, batch_number, manufacturer, quantity, price,
        sale_date, created_at, updated_at, is_deleted
    FROM STG_PHARMA_SALES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY updated_at DESC, created_at DESC) = 1
) S
ON T.sale_id = S.sale_id
WHEN MATCHED AND S.updated_at >= T.updated_at THEN
  UPDATE SET
    T.drug_name = S.drug_name,
    T.batch_number = S.batch_number,
    T.manufacturer = S.manufacturer,
    T.quantity = S.quantity,
    T.price = S.price,
    T.sale_date = S.sale_date,
    T.created_at = S.created_at,
    T.updated_at = S.updated_at,
    T.is_deleted = S.is_deleted
WHEN NOT MATCHED THEN
  INSERT (sale_id, drug_name, batch_number, manufacturer, quantity, price, sale_date, created_at, updated_at, is_deleted)
  VALUES (S.sale_id, S.drug_name, S.batch_number, S.manufacturer, S.quantity, S.price, S.sale_date, S.created_at, S.updated_at, S.is_deleted);
"""

spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, merge_sql)

# Optional cleanup after successful merge
spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, "TRUNCATE TABLE STG_PHARMA_SALES")
```

---

## 7) Snowflake DDL + MERGE

### Staging Table

```sql
CREATE TABLE IF NOT EXISTS STG_PHARMA_SALES (
    sale_id INT,
    drug_name STRING,
    batch_number STRING,
    manufacturer STRING,
    quantity INT,
    price NUMBER(10,2),
    sale_date DATE,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    is_deleted BOOLEAN
);
```

### Final Table

```sql
CREATE TABLE IF NOT EXISTS PHARMA_SALES (
    sale_id INT PRIMARY KEY,
    drug_name STRING,
    batch_number STRING,
    manufacturer STRING,
    quantity INT,
    price NUMBER(10,2),
    sale_date DATE,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    is_deleted BOOLEAN
);
```

### MERGE Query (Upsert)

```sql
MERGE INTO PHARMA_SALES T
USING (
    SELECT
        sale_id, drug_name, batch_number, manufacturer, quantity, price,
        sale_date, created_at, updated_at, is_deleted
    FROM STG_PHARMA_SALES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY updated_at DESC, created_at DESC) = 1
) S
ON T.sale_id = S.sale_id
WHEN MATCHED AND S.updated_at >= T.updated_at THEN
  UPDATE SET
    drug_name = S.drug_name,
    batch_number = S.batch_number,
    manufacturer = S.manufacturer,
    quantity = S.quantity,
    price = S.price,
    sale_date = S.sale_date,
    created_at = S.created_at,
    updated_at = S.updated_at,
    is_deleted = S.is_deleted
WHEN NOT MATCHED THEN
  INSERT (sale_id, drug_name, batch_number, manufacturer, quantity, price, sale_date, created_at, updated_at, is_deleted)
  VALUES (S.sale_id, S.drug_name, S.batch_number, S.manufacturer, S.quantity, S.price, S.sale_date, S.created_at, S.updated_at, S.is_deleted);
```

---

## 8) Sample Pharma Dataset (15 Rows)

```csv
sale_id,drug_name,batch_number,manufacturer,quantity,price,sale_date,created_at,updated_at,is_deleted
1001,Atorvastatin,BT-A101,PharmaOne,120,14.50,2026-04-20,2026-04-20 08:10:00,2026-04-20 08:10:00,false
1002,Metformin,BT-M201,HealthCorp,90,9.99,2026-04-20,2026-04-20 09:00:00,2026-04-20 09:00:00,false
1003,Amoxicillin,BT-AM55,LifeMeds,200,5.40,2026-04-20,2026-04-20 09:15:00,2026-04-20 09:15:00,false
1004,Ibuprofen,BT-I990,PharmaOne,180,4.25,2026-04-20,2026-04-20 10:00:00,2026-04-20 10:00:00,false
1005,Paracetamol,BT-P111,GoodHealth,300,3.10,2026-04-20,2026-04-20 10:30:00,2026-04-20 10:30:00,false
1006,Azithromycin,BT-AZ01,LifeMeds,70,18.75,2026-04-21,2026-04-21 08:45:00,2026-04-21 08:45:00,false
1007,Omeprazole,BT-O222,HealthCorp,95,11.20,2026-04-21,2026-04-21 09:30:00,2026-04-21 09:30:00,false
1008,Cetirizine,BT-C900,GoodHealth,210,6.50,2026-04-21,2026-04-21 10:10:00,2026-04-21 10:10:00,false
1009,Losartan,BT-L411,PharmaOne,85,12.80,2026-04-21,2026-04-21 11:20:00,2026-04-21 11:20:00,false
1010,Insulin,BT-IN77,EndoPharm,45,32.00,2026-04-21,2026-04-21 12:00:00,2026-04-21 12:00:00,false
1002,Metformin,BT-M201,HealthCorp,110,9.99,2026-04-22,2026-04-20 09:00:00,2026-04-22 08:00:00,false
1004,Ibuprofen,BT-I990,PharmaOne,0,4.25,2026-04-22,2026-04-20 10:00:00,2026-04-22 09:15:00,true
1011,Levothyroxine,BT-LT02,ThyroCare,60,16.40,2026-04-22,2026-04-22 10:05:00,2026-04-22 10:05:00,false
1012,Amlodipine,BT-AMD9,CardioLabs,130,8.75,2026-04-22,2026-04-22 10:25:00,2026-04-22 10:25:00,false
1013,Clopidogrel,BT-CL33,CardioLabs,75,21.90,2026-04-22,2026-04-22 11:40:00,2026-04-22 11:40:00,false
```

---

## 9) Why This Design

1. **Watermark incremental load (updated_at)**
   - Minimizes data scanned versus full load.
   - Easier to implement than CDC in many brownfield systems.
   - Supports inserts + updates naturally.

2. **ADF for orchestration**
   - Native connectors, scheduling, retries, monitoring.
   - Centralized dependency control and parameterization.

3. **Databricks for transformation**
   - Scales for larger datasets.
   - Strong data engineering APIs (Spark SQL, window functions, quality rules).

4. **Snowflake MERGE instead of overwrite**
   - Preserves history of unchanged rows and avoids full table rewrite cost.
   - Supports idempotent upserts and late-arriving updates.

---

## 10) Trade-offs and Alternatives

- **Watermark vs CDC**
  - Watermark is simple, low overhead, and quick to deploy.
  - CDC provides delete capture and exact change ordering but requires binlog/log-based setup and operational complexity.

- **Blob + Databricks vs direct MySQL→Snowflake copy**
  - Blob raw zone enables replay, audit, and reprocessing.
  - Direct load is simpler but loses durable landing zone benefits.

- **Soft delete (`is_deleted`) vs physical delete**
  - Soft delete preserves traceability and supports compliance/audits.

---

## 11) Step-by-Step Execution Flow

1. Trigger starts pipeline (schedule/event/manual).
2. ADF Lookup reads `last_watermark` from `pipeline_watermark_audit`.
3. Copy activity executes incremental query: `updated_at > last_watermark`.
4. Data lands in Blob as Parquet in partitioned raw folder.
5. ADF gets `MAX(updated_at)` of extracted batch.
6. If rows exist, Databricks reads raw files, cleans + dedups.
7. Databricks writes to Snowflake staging table.
8. Snowflake `MERGE` upserts into final table by `sale_id`.
9. On merge success, ADF updates watermark to batch max.
10. Pipeline logs success/failure per stage.

---

## 12) Error Handling and Retry Strategy

- **Retry transient errors** (network timeout, temporary DB unavailable, Snowflake session issue):
  - ADF activity retry: 3 attempts, exponential backoff.
- **Do not retry invalid data errors**:
  - Schema mismatch, non-parseable timestamps, invalid numeric formats should fail fast and route to quarantine.
- **Stage-level logging**:
  - Insert status into `pipeline_run_log` for each activity.
- **Dead-letter/quarantine**:
  - Bad records written to `quarantine/pharma_sales/` with reason codes.

---

## 13) Edge Cases and Improvements

1. **Late-arriving data**
   - Use overlap window: extract `updated_at > last_watermark - interval 'N minutes'` and rely on MERGE dedup.
2. **Duplicate records**
   - Dedup in Databricks and in Snowflake merge source (`ROW_NUMBER`).
3. **Schema changes**
   - Add schema evolution checks; maintain contract tests.
4. **Idempotency**
   - Deterministic merge key (`sale_id`) + update condition on `updated_at` ensures repeated runs are safe.
5. **Partial failures**
   - Watermark updates only after successful merge (two-phase commit style orchestration logic).

---

## 14) Production Considerations

1. **Scheduling**
   - Hourly for near-real-time ops, daily for finance/reports.
2. **Monitoring & alerting**
   - ADF alerts via Azure Monitor/Action Groups.
   - Track SLA metrics: run duration, rows copied, rows merged, failure rate.
3. **Blob partitioning**
   - `load_date=YYYY-MM-DD/load_hour=HH` for efficient pruning and replay.
4. **Performance optimization**
   - Pushdown filter in source SQL (`updated_at > watermark`).
   - Use Parquet compression (Snappy), compact small files.
   - Tune Databricks shuffle partitions and Snowflake warehouse sizing.
5. **Security**
   - Store credentials in Azure Key Vault.
   - Use managed identity/service principals.
   - Enforce TLS, private endpoints, least-privilege RBAC/roles.

---

## 15) Recommended Enhancements (Future)

- Move from watermark to log-based CDC when strict delete/ordering requirements increase.
- Introduce Delta Lake bronze/silver/gold medallion layers for richer lineage.
- Add data quality framework (e.g., Great Expectations/Deequ).
- Add metadata-driven multi-table framework for reuse across domains.
