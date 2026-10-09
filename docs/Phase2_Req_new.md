firstly he mentioned that the project should be done completely in Databricks 
Batch processing: you have to first try and confirm whether it's running or not 
Backfill check: agar hum apna data silver aur bronze se totally remove kr dein aur usko rebuild krna chahein to wo source, staging aur bronze 3no ke through hona chahiye valid 
Full load ko ek sath nhi kr skte kiyunke agar ek hi row 3-4 dafa change ho rhi hai tou u wouldn't know konsa change sab se recent huwa/valid modification check
file log me saari details daalni hain ke konsi files create update huwi kya cheez add huwi kya remove 
2 logs hon ge file log aur error log 
timestamp ke 2 cols bana skte hain ek created ek updated 
fully auditable pipeline


---

## Additional Requirements from Instructor (Handwritten Notes)

### Notebook Structure
- **Notebook 1:** Staging
- **Notebook 2:** Bronze  
- **Notebook 3:** Silver

### Key Requirements

1. **Staging Layer (New Requirement)**
   - Staging → Bronze → Silver pipeline flow
   - Data must flow through staging before Bronze

2. **Schema Management**
   - Schemas should be kept separate
   - Should be called/imported from a dedicated location
   - Do NOT hardcode schemas in notebooks

3. **Data Processing Columns**
   - **Source Columns:** Need to track source details
   - Should use external (source) file/data columns
   - Leverage external source column structure

4. **Execution Tracking**
   - Track notebook execution status
   - Log which notebook ran when

5. **Backup Strategy**  
   - Must have backup capabilities
   - Able to restore data if needed
   - Backfill process: Source → Staging → Bronze (3-layer recovery)

6. **File & Error Logging (Critical)**
   - **Two separate logs required:**
     - **File Log:** Track all file operations (created, updated, added, removed)
     - **Error Log:** Track all errors and failures
   - Log must capture:
     - What files were created
     - What was updated
     - What was added
     - What was removed/deleted

7. **Timestamp Strategy**
   - Use **2 timestamp columns:**
     - `created_timestamp` - when record was first created
     - `updated_timestamp` - when record was last modified
   - This enables full audit trail

8. **Modification Tracking**
   - Full Load cannot be done all at once
   - If a row changes 3-4 times, need to track which change is most recent
   - **Valid modification check** required
   - Must identify the most recent valid modification

9. **Auditable Pipeline (Non-Negotiable)**
   - Pipeline must be **fully auditable**
   - Every change must be traceable
   - Complete lineage tracking from source to silver

10. **Data Quality**
    - Track data issues at each layer
    - Quarantine bad records
    - Error resolution tracking

---

## Revised Architecture with Staging Layer

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCE DATA (DBFS Volumes)                                 │
│  - usgs_earthquake_full_load.json                           │
│  - usgs_earthquake_incremental.json                         │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGING LAYER (Notebook 1)                                 │
│  - Initial data validation                                  │
│  - File log: track source files processed                   │
│  - Error log: capture file-level issues                     │
│  - Temporary holding area                                   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Notebook 2)                                  │
│  - Raw data preservation                                    │
│  - SCD Type 2 with created_timestamp & updated_timestamp    │
│  - File log: track Bronze operations                        │
│  - Error log: capture parsing errors                        │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Notebook 3)                                  │
│  - Cleansed, typed data                                     │
│  - Modification tracking (most recent valid change)         │
│  - File log: track Silver transformations                   │
│  - Error log: capture transformation errors                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Schema File Requirements

**Location:** `/Workspace/seismo/schemas.py` (separate from notebooks)

**Usage Pattern:**
```python
# In each notebook
%run ./schemas

# Use imported schemas
BRONZE_SCHEMA
SILVER_SCHEMA
STAGING_SCHEMA
FILE_LOG_SCHEMA
ERROR_LOG_SCHEMA
```

---

## Updated Logging Requirements

### File Log Schema (Required for ALL layers)
```python
StructType([
    StructField("log_id", LongType(), False),  # Auto-increment
    StructField("layer", StringType(), False),  # STAGING, BRONZE, SILVER
    StructField("operation_type", StringType(), False),  # CREATE, UPDATE, DELETE
    StructField("file_name", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("records_affected", LongType(), True),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
    StructField("operation_status", StringType(), False),  # SUCCESS, FAILED
])
```

### Error Log Schema (Required for ALL layers)
```python
StructType([
    StructField("error_id", LongType(), False),  # Auto-increment
    StructField("error_timestamp", TimestampType(), False),
    StructField("layer", StringType(), False),  # STAGING, BRONZE, SILVER
    StructField("batch_id", StringType(), True),
    StructField("error_type", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("failed_record", StringType(), True),
    StructField("resolution_status", StringType(), True),  # PENDING, RESOLVED, IGNORED
])
```

---

## Critical Changes from Original Design

| Original Design | New Requirement | Reason |
|----------------|-----------------|---------|
| 2 Notebooks (Bronze, Silver) | 3 Notebooks (Staging, Bronze, Silver) | Better separation of concerns |
| Schemas in notebooks | Separate schema file | Reusability and maintainability |
| 1 timestamp column | 2 timestamp columns (created, updated) | Full audit trail |
| Single execution log | File log + Error log (per layer) | Granular tracking |
| Direct Bronze ingestion | Staging → Bronze flow | Data validation before persistence |
| Modification tracking optional | Modification tracking mandatory | Compliance requirement |

---

## Action Items

- [ ] Create Staging layer notebook (00_Staging_Ingestion.py)
- [ ] Update Bronze notebook to read from Staging
- [ ] Add created_timestamp and updated_timestamp to all tables
- [ ] Implement file_log table (per layer)
- [ ] Implement error_log table (per layer)
- [ ] Add modification tracking logic (identify most recent valid change)
- [ ] Test backfill scenario: delete all data and rebuild through Source → Staging → Bronze
- [ ] Verify full auditability of pipeline
