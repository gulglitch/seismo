# Phase 3 Setup Guide — GitHub, Databricks Automation, Power BI

This guide covers everything you need to run Phase 3 end to end:

1. Getting the code into Databricks from GitHub
2. First run (building the Gold layer)
3. Automating the pipeline with a Databricks Job
4. Connecting Power BI to the Gold layer
5. Collecting evidence for the report (Spark UI DAG, FinOps numbers)

---

## 1. Connect the GitHub repo to Databricks (Git folder)

1. In Databricks: **Workspace → Home → Create → Git folder**.
2. Git repository URL: `https://github.com/gulglitch/seismo` · Provider: **GitHub** · click **Create Git folder**.
3. To push from Databricks, add credentials once: **Settings → Linked accounts → Git integration → GitHub**.
   Either link with the GitHub App/OAuth, or paste a GitHub *fine-grained personal access token* with
   **Contents: Read and write** on the `seismo` repo.
4. In the Git folder, use the **branch dropdown** to switch to `phase3-gold-layer` (or `main` once it's merged).
   Use the **Git…** button to pull, commit and push.

> Notebooks run from inside the Git folder, so `%run ./config/constants` and `dbutils.notebook.run("./03_Gold_Transformations")`
> resolve relative to `notebooks/`. Don't move single notebooks out of the folder.

## 2. First run

All notebooks use the `seismo` schema and the raw Volume `/Volumes/workspace/seismo/raw/` (same as Phase 2).

| Step | Notebook | Widgets |
|---|---|---|
| 1 | `00_Setup` (only if tables don't exist yet) | — |
| 2 | `05_Run_Pipeline` | `ingestion_type = FULL_LOAD`, `source_path` = your full-load file |
| 3 | `05_Run_Pipeline` | `ingestion_type = INCREMENTAL_LOAD`, `source_path` blank (picks the newest incremental file) |
| 4 | `05_Run_Pipeline` again, same settings | Proves idempotency: Bronze/Silver `SKIPPED`, Gold steps `SKIPPED`, 0 rows |

If Bronze and Silver are already loaded from Phase 2, just run **`03_Gold_Transformations`** once (blank widgets).
It runs a full build the first time (no watermark yet) and incremental builds after that.

Screenshot the **Proof** cells at the bottom of `03_Gold_Transformations` (execution log, row counts,
referential integrity = 0 orphans, Silver vs Gold counts, shallow/deep ratio) and the before/after file-count
table in `04_Gold_Optimize`.

## 3. Automate it (Gold refreshes whenever a new file lands)

### Option A — Jobs UI (easiest)

1. **Jobs & Pipelines → Create → Job**. Name: `seismo-medallion-pipeline`.
2. Task: type **Notebook**, source **Workspace**, path `…/seismo/notebooks/05_Run_Pipeline`, compute **Serverless**.
   Parameters: `ingestion_type = INCREMENTAL_LOAD`, `run_optimize = true`.
3. **Schedules & Triggers → Add trigger → File arrival**. Storage location: `/Volumes/workspace/seismo/raw/`.
   Under advanced settings, set *Minimum time between triggers* to `60` seconds.
4. Set **Max concurrent runs = 1**, and add your email under **Notifications → On failure**.
5. (Optional) Create a second job `seismo-fetch-usgs-incremental`: notebook `00_Fetch_USGS_Data`,
   `fetch_type = INCREMENTAL_LOAD`, `days_back = 1`, on a **Scheduled** trigger (daily at 02:15 UTC).
   Every file it writes starts the pipeline job, so the whole chain runs on its own:
   **USGS API → raw Volume → Bronze → Silver → Gold → Optimize**.

### Option B — Databricks Asset Bundle (same jobs, as code)

The repo already contains `databricks.yml` and `jobs/seismo_pipeline_job.yml`.

```bash
pip install databricks-cli            # or: brew install databricks/tap/databricks
databricks auth login --host https://<your-workspace>.cloud.databricks.com
# set the host in databricks.yml, then:
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run seismo_pipeline -t dev
```

Screenshot the job's **Runs** tab (graph + durations) for the report's automation section.

## 4. Connect Power BI to the Gold layer

### 4.1 Get connection details

1. Databricks → **SQL Warehouses** → open your warehouse (a Serverless *2X-Small* is enough) → **Connection details**.
   Copy the **Server hostname** and **HTTP path**.
2. Authentication: use **OAuth / Microsoft Entra ID** if it's offered, otherwise a **personal access token**
   (**Settings → Developer → Access tokens → Generate**). Treat the token like a password, and don't commit it.

### 4.2 Load the tables

1. Power BI Desktop → **Get data → Azure Databricks** (or **Databricks** if you're on AWS/GCP or Free Edition).
2. Paste the Server hostname and HTTP path. Choose **Import** (the data is small; refreshes are fast and visuals are instant).
3. In the Navigator, open catalog `workspace` → schema `seismo` and tick:
   `gold_fact_seismic_event`, `gold_fact_daily_location_summary`, `gold_dim_date`, `gold_dim_location`,
   `gold_dim_magnitude_class`, `gold_dim_depth_class`, `gold_vw_pipeline_runs`, `gold_vw_error_summary`,
   `gold_vw_quarantine_summary`.

### 4.3 Model view (the semantic layer)

Create the relationships listed in [`Gold_Data_Dictionary.md` → Relationships](Gold_Data_Dictionary.md#relationships-configure-exactly-these-in-power-bi--model-view):
every fact → dimension relationship is **Many-to-one (\*:1), single cross-filter direction**. Power BI may
auto-detect some of them; delete any it guessed wrongly (for example on `*_timestamp` columns).

Then:
- Mark `gold_dim_date` as the date table (`full_date`).
- Sort by column: `month_name` by `month`, `year_month` by `year_month_key`, `day_name` by `day_of_week`,
  `magnitude_class` by `sort_order`, `depth_class` by `sort_order`.
- Hide all key columns (`*_key`) and the audit timestamps in Report view.

### 4.4 Starter DAX measures

```DAX
Total Events      = SUM ( gold_fact_seismic_event[event_count] )
Active Events     = CALCULATE ( [Total Events], gold_fact_seismic_event[is_active] = TRUE () )
Avg Magnitude     = AVERAGE ( gold_fact_seismic_event[magnitude] )
Max Magnitude     = MAX ( gold_fact_seismic_event[magnitude] )
Energy (TJ)       = SUM ( gold_fact_daily_location_summary[total_energy_joules] ) / 1e12
Shallow:Deep      = DIVIDE ( SUM ( gold_fact_daily_location_summary[shallow_count] ),
                             SUM ( gold_fact_daily_location_summary[deep_count] ) )
Events Last 30d   = CALCULATE ( [Active Events],
                       DATESINPERIOD ( gold_dim_date[full_date], MAX ( gold_dim_date[full_date] ), -30, DAY ) )
Locations Affected = DISTINCTCOUNT ( gold_fact_seismic_event[location_key] )
Pipeline Success % = DIVIDE ( CALCULATE ( COUNTROWS ( gold_vw_pipeline_runs ), gold_vw_pipeline_runs[status] = "SUCCESS" ),
                              COUNTROWS ( gold_vw_pipeline_runs ) )
```

### 4.5 Suggested pages

| Page | Visuals | Source |
|---|---|---|
| Overview | KPI cards (Total Events, Avg/Max Magnitude, Events Last 30d, Locations Affected); bubble map (lat/long, size = magnitude, colour = depth_class) | event fact + dims |
| Trends (Q1, Q3) | Combo chart: events (columns) vs energy (line) by `year_month`; small multiples by `location_label` | daily summary + date |
| Depth & Magnitude (Q2, Q4) | Stacked bar of shallow/intermediate/deep by location; magnitude-class histogram; depth vs magnitude scatter | daily summary, event fact |
| Top Regions | Top 10 bar by events, coloured by avg magnitude | daily summary + location |
| Pipeline Health | Runs by layer & status, duration trend, rows processed per run, errors by type, quarantined records by reason | `gold_vw_*` views |

Save as `seismo_dashboard.pbix`, and commit it to the repo (e.g. `bi/seismo_dashboard.pbix`) or publish it and share the link.

## 5. Evidence for the report

### Spark UI DAG (most complex transformation)

1. Run `03_Gold_Transformations` interactively.
2. Under the `run_gold` cell, expand **Spark Jobs** and open the job for the `gold_fact_seismic_event` MERGE
   (or the `gold_fact_daily_location_summary` aggregation).
3. Screenshot the **DAG visualization** of the stage graph, and the **SQL / DataFrame** tab's query plan.
4. Things to point out:
   - `BroadcastHashJoin` / `BroadcastExchange` — `gold_dim_location` is broadcast to every executor, so the fact isn't shuffled for the join.
   - `Exchange hashpartitioning(event_id)` — the **shuffle** that MERGE needs to match source rows to target rows.
   - `HashAggregate → Exchange hashpartitioning(date_key, location_key) → HashAggregate` — partial aggregation before the shuffle, then final aggregation after it (daily summary).
   - `PushedFilters` / *files pruned* on the Delta scan — **predicate pushdown** of `updated_timestamp > watermark`
     (Silver) and `date_key BETWEEN …` (Gold), plus data skipping from Liquid Clustering. `04_Gold_Optimize` prints an `EXPLAIN FORMATTED` you can quote.

### FinOps numbers (all from the logs)

```sql
-- Typical run duration per layer (seconds)
SELECT layer, load_type, COUNT(*) AS runs,
       ROUND(AVG(execution_duration_sec), 1) AS avg_sec,
       MAX(execution_duration_sec) AS max_sec
FROM seismo.pipeline_execution_log
WHERE status IN ('SUCCESS', 'PARTIAL')
GROUP BY layer, load_type
ORDER BY layer;

-- End-to-end wall clock per run
SELECT batch_id, execution_start, execution_duration_sec, error_message AS stage_breakdown
FROM seismo.pipeline_execution_log
WHERE layer = 'END_TO_END'
ORDER BY execution_start DESC;
```

If your workspace has system tables, `system.billing.usage` gives actual DBUs per job run
(filter by `usage_metadata.job_id`). Multiply the DBUs by your SKU's list price to get cost.
For a yearly estimate: **runs/year (8,760 if hourly) × avg DBUs per run × $/DBU**.
