# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03_Gold_Transformations — `silver_seismic_events` → Gold Star Schema
# MAGIC
# MAGIC Builds a business-ready **star schema** from Silver, refreshed incrementally with `MERGE`.
# MAGIC
# MAGIC | Table | Type | Grain | Refresh strategy |
# MAGIC |---|---|---|---|
# MAGIC | `gold_dim_date` | Dimension | 1 row per calendar day | MERGE (insert missing days, contiguous range) |
# MAGIC | `gold_dim_location` | Dimension | 1 row per (country, state_province) | MERGE (insert new members) |
# MAGIC | `gold_dim_magnitude_class` | Dimension (static) | 1 row per Richter class | MERGE seed |
# MAGIC | `gold_dim_depth_class` | Dimension (static) | 1 row per depth class | MERGE seed |
# MAGIC | `gold_fact_seismic_event` | Transaction fact | 1 row per USGS event | MERGE on `event_id` (watermark on Silver `updated_timestamp`) |
# MAGIC | `gold_fact_daily_location_summary` | Aggregate fact | 1 row per (day, location) | Recompute affected date range, `replaceWhere` overwrite |
# MAGIC
# MAGIC **Parameters (widgets)**
# MAGIC | widget | meaning |
# MAGIC |---|---|
# MAGIC | `batch_id` | Silver batch that triggered this run (lineage only). Blank → latest Silver batch |
# MAGIC | `full_refresh` | `true` = ignore the watermark and re-process every Silver row |
# MAGIC | `database` | schema/database holding all tables |
# MAGIC
# MAGIC **Guarantees:** incremental (only Silver rows changed since the last Gold run are read) · idempotent
# MAGIC (re-running with no new Silver data writes 0 rows) · every step is written to `pipeline_execution_log`
# MAGIC with rows processed / inserted / updated and execution time.

# COMMAND ----------

# MAGIC %run ./config/schemas

# COMMAND ----------

# MAGIC %run ./config/constants

# COMMAND ----------

# MAGIC %run ./config/utils

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
dbutils.widgets.text("database", "seismo")

DB = dbutils.widgets.get("database")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"USE SCHEMA {DB}")

SILVER = "silver_seismic_events"
QUARANTINE = "silver_seismic_events_quarantine"
EXEC_LOG = "pipeline_execution_log"
GOLD_ERR = "error_log"
LAYER = "SILVER_TO_GOLD"
NOTEBOOK = "03_Gold_Transformations"

DIM_DATE = GOLD_DIM_DATE
DIM_LOCATION = GOLD_DIM_LOCATION
DIM_MAG = GOLD_DIM_MAGNITUDE_CLASS
DIM_DEPTH = GOLD_DIM_DEPTH_CLASS
FACT_EVENT = GOLD_FACT_SEISMIC_EVENT
FACT_DAILY = GOLD_FACT_DAILY_SUMMARY
UNKNOWN = GOLD_UNKNOWN_KEY

# COMMAND ----------

# MAGIC %md ## Static dimension definitions
# MAGIC Class boundaries match the Silver enrichment rules (Phase 2), but the Gold keys are derived from the
# MAGIC **numeric** value so a NULL magnitude/depth maps to the `Unknown` member (`-1`) instead of a real class.

# COMMAND ----------

# (key, name, min inclusive, max exclusive, label, typical effects)
MAGNITUDE_CLASSES = [
    (1, "Minor",    None, 3.0,  "< 3.0",   "Usually not felt; recorded by seismographs"),
    (2, "Light",    3.0,  4.0,  "3.0-3.9", "Often felt, rarely causes damage"),
    (3, "Moderate", 4.0,  5.0,  "4.0-4.9", "Noticeable shaking; minor damage possible"),
    (4, "Strong",   5.0,  6.0,  "5.0-5.9", "Damage to poorly constructed buildings"),
    (5, "Major",    6.0,  7.0,  "6.0-6.9", "Destructive within ~100 km of the epicentre"),
    (6, "Great",    7.0,  None, "7.0+",    "Serious damage over large areas; tsunami risk if offshore"),
]

# (key, name, min km inclusive, max km exclusive, label, description)
DEPTH_CLASSES = [
    (1, "Shallow",      None,  70.0,  "0-69 km",    "Crustal events; strongest surface shaking"),
    (2, "Intermediate", 70.0,  300.0, "70-299 km",  "Subducting slab events"),
    (3, "Deep",         300.0, None,  "300+ km",    "Deep-focus events; widely felt, rarely damaging"),
]

def class_key_expr(col_name, classes):
    """CASE expression: numeric value -> class key, NULL -> UNKNOWN (-1)."""
    expr = F.when(F.col(col_name).isNull(), F.lit(UNKNOWN))
    for key, _, lo, hi, _, _ in classes:
        cond = F.lit(True)
        if lo is not None:
            cond = cond & (F.col(col_name) >= lo)
        if hi is not None:
            cond = cond & (F.col(col_name) < hi)
        expr = expr.when(cond, F.lit(key))
    return expr.otherwise(F.lit(UNKNOWN))

# COMMAND ----------

# MAGIC %md ## Location conformance
# MAGIC Silver splits `place` on the last comma, so `country` holds values like `CA`, `Alaska`,
# MAGIC `south of the Fiji Islands` or `Kermadec Islands region`. Gold conforms these into a low-cardinality
# MAGIC `(country, state_province)` dimension; the distance-specific text stays on the fact as `nearest_place`.

# COMMAND ----------

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "PR": "Puerto Rico", "DC": "District of Columbia",
}
# Upper-cased code AND full name both resolve to the full state name.
# "Georgia" spelled out is the country in the Caucasus in USGS data; only the code GA means the US state.
_us_lookup = {**{k: v for k, v in US_STATES.items()},
              **{v.upper(): v for v in US_STATES.values() if v != "Georgia"}}
US_STATE_MAP = F.create_map(*[F.lit(x) for kv in _us_lookup.items() for x in kv])

COUNTRY_ALIASES = {"MX": "Mexico", "B.C.": "Mexico", "U.S. VIRGIN ISLANDS": "U.S. Virgin Islands"}
COUNTRY_ALIAS_MAP = F.create_map(*[F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv])

_DIRECTIONS = "north|south|east|west|northeast|northwest|southeast|southwest|northern|southern|eastern|western|central"
# "south of the Fiji Islands" -> "Fiji Islands"
DIRECTION_PREFIX = rf"(?i)^(.*\s)?({_DIRECTIONS})\s+of\s+(the\s+)?"
# USGS writes qualifiers in lower case: "northern Mid-Atlantic Ridge" -> "Mid-Atlantic Ridge"
# (case-sensitive so proper names like "South Sandwich Islands" are kept)
LOWERCASE_QUALIFIER = r"^((northern|southern|eastern|western|central)\s+)+"
# "off the coast of Oregon" / "near the east coast of Honshu" -> "Oregon" / "Honshu"
COAST_PREFIX = r"(?i)^(off|near)\s+the\s+(.*\s+)?coast\s+of\s+(the\s+)?"
OCEANIC_PATTERN = r"(?i)(ridge|rise|ocean|\bsea\b|trench|basin|gulf|strait|passage|fracture zone|triple junction)"

def conform_location(df):
    """Adds country_c, state_province_c, location_type_c, location_label_c and nearest_place."""
    raw = F.trim(F.coalesce(F.col("country"), F.lit("")))
    c = F.regexp_replace(raw, DIRECTION_PREFIX, "")
    c = F.regexp_replace(c, COAST_PREFIX, "")
    c = F.regexp_replace(c, LOWERCASE_QUALIFIER, "")
    c = F.trim(F.regexp_replace(c, r"(?i)\s+(region|earthquake)$", ""))
    # try_element_at: NULL for a missing key even with ANSI mode on (serverless default)
    us_state = F.try_element_at(US_STATE_MAP, F.upper(c))
    alias = F.try_element_at(COUNTRY_ALIAS_MAP, F.upper(c))

    country = (F.when(c == "", F.lit("Unknown"))
                .when(us_state.isNotNull(), F.lit("United States"))
                .when(alias.isNotNull(), alias)
                .otherwise(c))
    state = F.when(us_state.isNotNull(), us_state).otherwise(F.lit("N/A"))
    loc_type = (F.when(us_state.isNotNull(), F.lit("United States"))
                 .when(c == "", F.lit("Unknown"))
                 .when(c.rlike(OCEANIC_PATTERN), F.lit("Oceanic / Remote"))
                 .otherwise(F.lit("Country / Territory")))

    return (df
            .withColumn("country_c", country)
            .withColumn("state_province_c", state)
            .withColumn("location_type_c", loc_type)
            .withColumn("location_label_c",
                        F.when(F.col("state_province_c") == "N/A", F.col("country_c"))
                         .otherwise(F.concat_ws(", ", F.col("state_province_c"), F.col("country_c"))))
            .withColumn("nearest_place",
                        F.trim(F.regexp_replace(F.coalesce("region", "place"), r"^[0-9.]+\s*km\s+[NSEW]{1,3}\s+of\s+", ""))))

# COMMAND ----------

# MAGIC %md ## DDL Statements (Idempotent Table Creation)
# MAGIC Fact tables use **Liquid Clustering** on their join/filter keys (`date_key`, `location_key`) instead of
# MAGIC Hive-style partitions: at ~15k events/year, `PARTITIONED BY` would create thousands of tiny files.
# MAGIC If the runtime does not support `CLUSTER BY`, the table is created unclustered and
# MAGIC `04_Gold_Optimize` falls back to `OPTIMIZE ... ZORDER BY`.

# COMMAND ----------

def create_table(name, columns_ddl, comment, cluster_by=None):
    base = f"CREATE TABLE IF NOT EXISTS {name} ({columns_ddl}) USING DELTA"
    if cluster_by:
        try:
            spark.sql(f"{base} CLUSTER BY ({', '.join(cluster_by)}) COMMENT '{comment}'")
            return
        except Exception as e:
            print(f"  CLUSTER BY not supported for {name} ({type(e).__name__}); creating unclustered")
    spark.sql(f"{base} COMMENT '{comment}'")

create_table(DIM_DATE, """
  date_key        INT     COMMENT 'Surrogate key yyyyMMdd (PK)',
  full_date       DATE    COMMENT 'Calendar date (UTC)',
  year            INT,
  quarter         INT,
  quarter_label   STRING  COMMENT 'Q1..Q4',
  month           INT,
  month_name      STRING,
  month_short     STRING,
  year_month      STRING  COMMENT 'yyyy-MM',
  year_month_key  INT     COMMENT 'yyyyMM (sort key for year_month)',
  day_of_month    INT,
  day_of_year     INT,
  day_of_week     INT     COMMENT 'ISO: 1 = Monday ... 7 = Sunday',
  day_name        STRING,
  week_of_year    INT,
  is_weekend      BOOLEAN
""", "Gold dimension: contiguous calendar (UTC)")

create_table(DIM_LOCATION, """
  location_key      BIGINT  COMMENT 'Surrogate key (PK); -1 = Unknown',
  country           STRING  COMMENT 'Conformed country / territory / oceanic area',
  state_province    STRING  COMMENT 'US state when applicable, otherwise N/A',
  location_label    STRING  COMMENT 'Display name',
  location_type     STRING  COMMENT 'United States / Country / Territory / Oceanic / Remote / Unknown',
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP
""", "Gold dimension: conformed event location")

create_table(DIM_MAG, """
  magnitude_class_key INT    COMMENT 'Surrogate key (PK); -1 = Unknown',
  magnitude_class     STRING COMMENT 'Minor / Light / Moderate / Strong / Major / Great',
  magnitude_range     STRING,
  min_magnitude       DOUBLE COMMENT 'Inclusive lower bound (NULL = open)',
  max_magnitude       DOUBLE COMMENT 'Exclusive upper bound (NULL = open)',
  typical_effects     STRING,
  sort_order          INT
""", "Gold dimension: Richter magnitude classes (static)")

create_table(DIM_DEPTH, """
  depth_class_key INT    COMMENT 'Surrogate key (PK); -1 = Unknown',
  depth_class     STRING COMMENT 'Shallow / Intermediate / Deep',
  depth_range     STRING,
  min_depth_km    DOUBLE COMMENT 'Inclusive lower bound (NULL = open)',
  max_depth_km    DOUBLE COMMENT 'Exclusive upper bound (NULL = open)',
  description     STRING,
  sort_order      INT
""", "Gold dimension: focal depth classes (static)")

create_table(FACT_EVENT, """
  event_id                 STRING    COMMENT 'USGS event id (PK, degenerate dimension)',
  date_key                 INT       COMMENT 'FK -> gold_dim_date',
  location_key             BIGINT    COMMENT 'FK -> gold_dim_location',
  magnitude_class_key      INT       COMMENT 'FK -> gold_dim_magnitude_class',
  depth_class_key          INT       COMMENT 'FK -> gold_dim_depth_class',
  event_time               TIMESTAMP COMMENT 'UTC origin time',
  event_hour_utc           INT,
  magnitude                DOUBLE,
  magnitude_type           STRING,
  depth_km                 DOUBLE,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  place                    STRING    COMMENT 'Original USGS place string',
  nearest_place            STRING    COMMENT 'place without the "NN km DIR of" prefix',
  felt_reports             INT,
  cdi                      DOUBLE,
  mmi                      DOUBLE,
  significance             INT,
  alert_level              STRING,
  tsunami_flag             BOOLEAN,
  status                   STRING,
  event_type               STRING,
  network_code             STRING,
  is_active                BOOLEAN   COMMENT 'FALSE = soft-deleted by USGS',
  is_significant           BOOLEAN   COMMENT 'USGS significance >= 600',
  energy_joules            DOUBLE    COMMENT 'Gutenberg-Richter: 10^(1.5*M + 4.8)',
  energy_tnt_tons          DOUBLE    COMMENT 'energy_joules / 4.184e9',
  event_count              INT       COMMENT 'Always 1 (additive measure)',
  source_batch_id          STRING    COMMENT 'Silver/Bronze batch lineage',
  silver_updated_timestamp TIMESTAMP COMMENT 'Silver updated_timestamp (incremental watermark)',
  gold_created_timestamp   TIMESTAMP,
  gold_updated_timestamp   TIMESTAMP
""", "Gold fact: one row per seismic event (current version)", cluster_by=["date_key", "location_key"])

create_table(FACT_DAILY, """
  date_key               INT     COMMENT 'FK -> gold_dim_date',
  location_key           BIGINT  COMMENT 'FK -> gold_dim_location',
  event_count            BIGINT,
  avg_magnitude          DOUBLE,
  max_magnitude          DOUBLE,
  min_magnitude          DOUBLE,
  avg_depth_km           DOUBLE,
  total_energy_joules    DOUBLE,
  shallow_count          BIGINT,
  intermediate_count     BIGINT,
  deep_count             BIGINT,
  strong_plus_count      BIGINT  COMMENT 'magnitude >= 5.0',
  significant_count      BIGINT,
  tsunami_count          BIGINT,
  felt_reports_total     BIGINT,
  max_significance       INT,
  gold_updated_timestamp TIMESTAMP
""", "Gold aggregate fact: active events per day and location", cluster_by=["date_key", "location_key"])

print("Gold tables ready:", ", ".join(GOLD_TABLES))

# COMMAND ----------

# MAGIC %md ## Helpers (logging, metrics)

# COMMAND ----------

def write_exec_log(start, end, batch_id, load_type, target, status, m, err_msg=None):
    row = (start, end, NOTEBOOK, batch_id, LAYER, load_type, target, status,
           int(m.get("total", 0)), int(m.get("inserted", 0)), int(m.get("updated", 0)),
           int(m.get("errored", 0)), err_msg[:2000] if err_msg else None,
           int((end - start).total_seconds()), utc_now())
    append_with_identity(spark.createDataFrame([row], EXECUTION_LOG_SCHEMA), EXEC_LOG)

def write_error_log(batch_id, err, target):
    ts = utc_now()
    row = (ts, LAYER, NOTEBOOK, batch_id, "GOLD_TRANSFORMATION",
           f"{target}: {str(err)}"[:2000], None, traceback.format_exc()[:8000],
           "UNRESOLVED", None, None, ts, None)
    append_with_identity(spark.createDataFrame([row], ERROR_LOG_SCHEMA), GOLD_ERR)

def table_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]

def commit_metrics(table, version_before):
    """operationMetrics of the commit made after version_before (empty dict if nothing was committed)."""
    h = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    if h["version"] == version_before:
        return {}
    return {k: int(v) for k, v in (h["operationMetrics"] or {}).items() if str(v).isdigit()}

def merge_counts(table, version_before):
    om = commit_metrics(table, version_before)
    return om.get("numTargetRowsInserted", 0), om.get("numTargetRowsUpdated", 0)

def run_step(target, batch_id, load_type, fn):
    """Runs one Gold step, times it and writes one pipeline_execution_log row (SUCCESS or FAILED)."""
    start = utc_now()
    try:
        m = fn()
    except Exception as e:
        write_error_log(batch_id, e, target)
        write_exec_log(start, utc_now(), batch_id, load_type, target, "FAILED",
                       {"errored": 1}, str(e))
        raise
    end = utc_now()
    write_exec_log(start, end, batch_id, load_type, target, "SUCCESS", m)
    log_file_operation(layer=LAYER_GOLD, operation_type=OP_UPDATE, file_name=target,
                       batch_id=batch_id, records_affected=int(m.get("inserted", 0) + m.get("updated", 0)),
                       status=STATUS_SUCCESS, file_path=f"table://{target}")
    m["duration_sec"] = round((end - start).total_seconds(), 2)
    print(f"  [{target}] processed={m.get('total', 0)} inserted={m.get('inserted', 0)} "
          f"updated={m.get('updated', 0)} ({m['duration_sec']}s)")
    return m

# COMMAND ----------

# MAGIC %md ## Dimension builders

# COMMAND ----------

def seed_static_dim(table, key_col, classes, name_col, range_col, lo_col, hi_col, desc_col):
    rows = [(k, n, r, lo, hi, d, k) for k, n, lo, hi, r, d in classes]
    rows.append((UNKNOWN, "Unknown", "Unknown", None, None, "Value not reported", 99))
    cols = [key_col, name_col, range_col, lo_col, hi_col, desc_col, "sort_order"]
    schema = spark.table(table).select(*cols).schema
    src = spark.createDataFrame(rows, schema)
    v0 = table_version(table)
    (DeltaTable.forName(spark, table).alias("t")
     .merge(src.alias("s"), f"t.{key_col} = s.{key_col}")
     .whenMatchedUpdateAll(condition=" OR ".join(f"NOT (t.{c} <=> s.{c})" for c in cols[1:]))
     .whenNotMatchedInsertAll()
     .execute())
    ins, upd = merge_counts(table, v0)
    return {"total": len(rows), "inserted": ins, "updated": upd}


def build_dim_date(src):
    """Insert every missing day so the calendar stays contiguous from the earliest to the latest event."""
    def _run():
        rng = src.agg(F.min(F.to_date("event_time")).alias("lo"), F.max(F.to_date("event_time")).alias("hi")).first()
        cur = spark.table(DIM_DATE).agg(F.min("full_date").alias("lo"), F.max("full_date").alias("hi")).first()
        los = [d for d in (rng["lo"], cur["lo"]) if d is not None]
        his = [d for d in (rng["hi"], cur["hi"]) if d is not None]
        if not los:
            return {"total": 0, "inserted": 0, "updated": 0}
        lo, hi = min(los), max(his)

        days = (spark.sql(f"SELECT explode(sequence(DATE'{lo}', DATE'{hi}', INTERVAL 1 DAY)) AS full_date")
                .select(
                    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
                    "full_date",
                    F.year("full_date").alias("year"),
                    F.quarter("full_date").alias("quarter"),
                    F.concat(F.lit("Q"), F.quarter("full_date")).alias("quarter_label"),
                    F.month("full_date").alias("month"),
                    F.date_format("full_date", "MMMM").alias("month_name"),
                    F.date_format("full_date", "MMM").alias("month_short"),
                    F.date_format("full_date", "yyyy-MM").alias("year_month"),
                    F.date_format("full_date", "yyyyMM").cast("int").alias("year_month_key"),
                    F.dayofmonth("full_date").alias("day_of_month"),
                    F.dayofyear("full_date").alias("day_of_year"),
                    (((F.dayofweek("full_date") + 5) % 7) + 1).alias("day_of_week"),
                    F.date_format("full_date", "EEEE").alias("day_name"),
                    F.weekofyear("full_date").alias("week_of_year"),
                    F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
                ))
        v0 = table_version(DIM_DATE)
        (DeltaTable.forName(spark, DIM_DATE).alias("t")
         .merge(days.alias("s"), "t.date_key = s.date_key")
         .whenNotMatchedInsertAll()
         .execute())
        ins, _ = merge_counts(DIM_DATE, v0)
        return {"total": (hi - lo).days + 1, "inserted": ins, "updated": 0}
    return _run


def build_dim_location(src_conformed, load_ts):
    """SCD Type 1 dimension: new (country, state_province) members get max(key)+n surrogate keys."""
    def _run():
        dim = spark.table(DIM_LOCATION)
        if dim.filter(F.col("location_key") == UNKNOWN).limit(1).count() == 0:
            unknown = spark.createDataFrame(
                [(UNKNOWN, "Unknown", "N/A", "Unknown", "Unknown", load_ts, load_ts)], dim.schema)
            unknown.write.format("delta").mode("append").saveAsTable(DIM_LOCATION)

        members = (src_conformed
                   .select(F.col("country_c").alias("country"),
                           F.col("state_province_c").alias("state_province"),
                           F.col("location_label_c").alias("location_label"),
                           F.col("location_type_c").alias("location_type"))
                   .dropDuplicates(["country", "state_province"]))
        n_members = members.count()

        existing = spark.table(DIM_LOCATION).select("country", "state_province")
        new = members.join(existing, ["country", "state_province"], "left_anti")
        max_key = spark.table(DIM_LOCATION).agg(F.max("location_key")).first()[0] or 0
        max_key = max(int(max_key), 0)
        w = Window.orderBy("country", "state_province")
        new_keyed = (new.withColumn("location_key", (F.row_number().over(w) + F.lit(max_key)).cast("bigint"))
                     .withColumn("created_timestamp", F.lit(load_ts))
                     .withColumn("updated_timestamp", F.lit(load_ts)))

        v0 = table_version(DIM_LOCATION)
        (DeltaTable.forName(spark, DIM_LOCATION).alias("t")
         .merge(new_keyed.alias("s"), "t.country = s.country AND t.state_province = s.state_province")
         .whenNotMatchedInsertAll()
         .execute())
        ins, _ = merge_counts(DIM_LOCATION, v0)
        return {"total": n_members, "inserted": ins, "updated": 0}
    return _run

# COMMAND ----------

# MAGIC %md ## Fact builders

# COMMAND ----------

def build_fact_event(src_conformed, load_ts, full_refresh):
    """MERGE on event_id. Small dimensions are broadcast; new/changed events only (watermark)."""
    def _run():
        loc = F.broadcast(spark.table(DIM_LOCATION).select("location_key", "country", "state_province"))
        staged = (src_conformed
                  .join(loc, (F.col("country_c") == loc.country) & (F.col("state_province_c") == loc.state_province), "left")
                  .select(
                      "event_id",
                      F.date_format("event_time", "yyyyMMdd").cast("int").alias("date_key"),
                      F.coalesce("location_key", F.lit(UNKNOWN)).cast("bigint").alias("location_key"),
                      class_key_expr("magnitude", MAGNITUDE_CLASSES).alias("magnitude_class_key"),
                      class_key_expr("depth_km", DEPTH_CLASSES).alias("depth_class_key"),
                      "event_time",
                      F.hour("event_time").alias("event_hour_utc"),
                      "magnitude", "magnitude_type", "depth_km", "latitude", "longitude",
                      "place", "nearest_place", "felt_reports", "cdi", "mmi", "significance",
                      "alert_level", "tsunami_flag", "status", "event_type", "network_code",
                      F.coalesce("is_active", F.lit(True)).alias("is_active"),
                      (F.coalesce("significance", F.lit(0)) >= 600).alias("is_significant"),
                      F.pow(F.lit(10.0), F.col("magnitude") * 1.5 + 4.8).alias("energy_joules"),
                      (F.pow(F.lit(10.0), F.col("magnitude") * 1.5 + 4.8) / F.lit(4.184e9)).alias("energy_tnt_tons"),
                      F.lit(1).alias("event_count"),
                      "source_batch_id",
                      F.col("updated_timestamp").alias("silver_updated_timestamp"),
                      F.lit(load_ts).alias("gold_created_timestamp"),
                      F.lit(load_ts).alias("gold_updated_timestamp"),
                  ))
        n = staged.count()
        if n == 0:
            return {"total": 0, "inserted": 0, "updated": 0}

        update_cols = [c for c in staged.columns if c not in ("event_id", "gold_created_timestamp")]
        cond = None if full_refresh else "s.silver_updated_timestamp > t.silver_updated_timestamp"
        v0 = table_version(FACT_EVENT)
        (DeltaTable.forName(spark, FACT_EVENT).alias("t")
         .merge(staged.alias("s"), "t.event_id = s.event_id")
         .whenMatchedUpdate(condition=cond, set={c: f"s.{c}" for c in update_cols})
         .whenNotMatchedInsertAll()
         .execute())
        ins, upd = merge_counts(FACT_EVENT, v0)
        return {"total": n, "inserted": ins, "updated": upd}
    return _run


def build_fact_daily(date_lo, date_hi, load_ts):
    """Re-aggregates every (day, location) in the affected date range and atomically replaces that slice."""
    def _run():
        if date_lo is None:
            return {"total": 0, "inserted": 0, "updated": 0}
        predicate = f"date_key >= {date_lo} AND date_key <= {date_hi}"
        events = spark.table(FACT_EVENT).filter(predicate).filter("is_active = true")
        n_events = events.count()

        daily = (events.groupBy("date_key", "location_key")
                 .agg(F.count("*").alias("event_count"),
                      F.avg("magnitude").alias("avg_magnitude"),
                      F.max("magnitude").alias("max_magnitude"),
                      F.min("magnitude").alias("min_magnitude"),
                      F.avg("depth_km").alias("avg_depth_km"),
                      F.sum("energy_joules").alias("total_energy_joules"),
                      F.sum((F.col("depth_class_key") == 1).cast("long")).alias("shallow_count"),
                      F.sum((F.col("depth_class_key") == 2).cast("long")).alias("intermediate_count"),
                      F.sum((F.col("depth_class_key") == 3).cast("long")).alias("deep_count"),
                      F.sum((F.col("magnitude") >= 5.0).cast("long")).alias("strong_plus_count"),
                      F.sum(F.col("is_significant").cast("long")).alias("significant_count"),
                      F.sum(F.col("tsunami_flag").cast("long")).alias("tsunami_count"),
                      F.sum(F.coalesce("felt_reports", F.lit(0))).cast("long").alias("felt_reports_total"),
                      F.max("significance").alias("max_significance"))
                 .withColumn("gold_updated_timestamp", F.lit(load_ts)))

        before = spark.table(FACT_DAILY).filter(predicate).count()
        (daily.select(*spark.table(FACT_DAILY).columns)
         .write.format("delta").mode("overwrite")
         .option("replaceWhere", predicate)
         .saveAsTable(FACT_DAILY))
        # counted from the table: numOutputRows also includes rows Delta rewrote outside the predicate
        written = spark.table(FACT_DAILY).filter(predicate).count()
        # records_processed = fact rows aggregated; inserted = net new summary rows; updated = rows replaced
        return {"total": n_events, "inserted": max(written - before, 0), "updated": min(before, written)}
    return _run

# COMMAND ----------

def pending_summary_dates():
    """Fact rows written after the last successful summary refresh (non-empty only if a run failed mid-way)."""
    last = spark.table(FACT_DAILY).agg(F.max("gold_updated_timestamp")).first()[0]
    f = spark.table(FACT_EVENT)
    if last is not None:
        f = f.filter(F.col("gold_updated_timestamp") > F.lit(last))
    return f.select("date_key")


def date_range(df):
    r = df.agg(F.min("date_key").alias("lo"), F.max("date_key").alias("hi")).first()
    return r["lo"], r["hi"]

# COMMAND ----------

# MAGIC %md ## BI views over the audit tables
# MAGIC Thin views so Power BI can chart pipeline health without touching raw log schemas.

# COMMAND ----------

def build_audit_views():
    spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_VW_PIPELINE_RUNS} AS
    SELECT execution_id, execution_start, execution_end,
           CAST(date_format(execution_start, 'yyyyMMdd') AS INT) AS date_key,
           notebook_name, batch_id, layer, load_type, source_param AS target_or_source, status,
           records_processed, records_inserted, records_updated, records_errored,
           execution_duration_sec, error_message
    FROM {EXEC_LOG}""")

    spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_VW_ERROR_SUMMARY} AS
    SELECT error_id, error_timestamp,
           CAST(date_format(error_timestamp, 'yyyyMMdd') AS INT) AS date_key,
           layer, notebook_name, batch_id, error_type, resolution_status,
           substring(error_message, 1, 300) AS error_message
    FROM {GOLD_ERR}""")

    spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_VW_QUARANTINE_SUMMARY} AS
    SELECT batch_id, quarantine_reason, reprocessing_status,
           CAST(date_format(quarantine_timestamp, 'yyyyMMdd') AS INT) AS date_key,
           COUNT(*) AS quarantined_records
    FROM {QUARANTINE}
    GROUP BY batch_id, quarantine_reason, reprocessing_status,
             CAST(date_format(quarantine_timestamp, 'yyyyMMdd') AS INT)""")

# COMMAND ----------

# MAGIC %md ## Core Transformation: `run_gold`

# COMMAND ----------

def run_gold(batch_id, full_refresh=False):
    run_start = utc_now()
    load_ts = run_start

    # 1) Incremental watermark: Silver stamps updated_timestamp on every insert/update it merges
    watermark = None
    if not full_refresh:
        watermark = spark.table(FACT_EVENT).agg(F.max("silver_updated_timestamp")).first()[0]
    load_type = LOAD_TYPE_FULL if watermark is None else LOAD_TYPE_INCREMENTAL

    src = spark.table(SILVER)
    if watermark is not None:
        src = src.filter(F.col("updated_timestamp") > F.lit(watermark))
    n_src = src.count()
    print(f"Gold run batch_id={batch_id} load_type={load_type} watermark={watermark} silver_rows={n_src}")

    log_file_operation(layer=LAYER_GOLD, operation_type=OP_READ, file_name=SILVER, batch_id=batch_id,
                       records_affected=n_src, status=STATUS_SUCCESS, file_path=f"table://{SILVER}")

    results = {}
    results[DIM_MAG] = run_step(DIM_MAG, batch_id, load_type, lambda: seed_static_dim(
        DIM_MAG, "magnitude_class_key", MAGNITUDE_CLASSES, "magnitude_class", "magnitude_range",
        "min_magnitude", "max_magnitude", "typical_effects"))
    results[DIM_DEPTH] = run_step(DIM_DEPTH, batch_id, load_type, lambda: seed_static_dim(
        DIM_DEPTH, "depth_class_key", DEPTH_CLASSES, "depth_class", "depth_range",
        "min_depth_km", "max_depth_km", "description"))

    if n_src == 0:
        for t in (DIM_DATE, DIM_LOCATION, FACT_EVENT):
            write_exec_log(utc_now(), utc_now(), batch_id, load_type, t, STATUS_SKIPPED,
                           {"total": 0}, "No Silver rows changed since last Gold run")
        date_lo, date_hi = date_range(pending_summary_dates())
        if date_lo is None:
            write_exec_log(utc_now(), utc_now(), batch_id, load_type, FACT_DAILY, STATUS_SKIPPED,
                           {"total": 0}, "No Silver rows changed since last Gold run")
        else:
            results[FACT_DAILY] = run_step(FACT_DAILY, batch_id, load_type, build_fact_daily(date_lo, date_hi, load_ts))
        build_audit_views()
        print("SKIPPED: Gold already up to date with Silver.")
        return {"silver_rows": 0, "load_type": load_type, "steps": results}

    conformed = conform_location(src)

    # 2) Dimensions first so every fact row finds its keys
    results[DIM_DATE] = run_step(DIM_DATE, batch_id, load_type, build_dim_date(src))
    results[DIM_LOCATION] = run_step(DIM_LOCATION, batch_id, load_type, build_dim_location(conformed, load_ts))

    # 3) Affected days = new event dates + the dates those events had before (event_time can be revised)
    #    + anything a previously failed run left un-aggregated. Computed BEFORE the fact MERGE.
    new_dates = src.select(F.date_format("event_time", "yyyyMMdd").cast("int").alias("date_key"))
    old_dates = (spark.table(FACT_EVENT).join(src.select("event_id"), "event_id", "left_semi")
                 .select("date_key"))
    date_lo, date_hi = date_range(new_dates.unionByName(old_dates).unionByName(pending_summary_dates()))

    # 4) Facts
    results[FACT_EVENT] = run_step(FACT_EVENT, batch_id, load_type, build_fact_event(conformed, load_ts, full_refresh))
    results[FACT_DAILY] = run_step(FACT_DAILY, batch_id, load_type, build_fact_daily(date_lo, date_hi, load_ts))

    build_audit_views()
    total_sec = round((utc_now() - run_start).total_seconds(), 2)
    print(f"[SUCCESS] Gold {batch_id}: silver_rows={n_src} affected_dates={date_lo}..{date_hi} total={total_sec}s")
    return {"silver_rows": n_src, "load_type": load_type, "date_range": [date_lo, date_hi],
            "duration_sec": total_sec, "steps": results}

# COMMAND ----------

# MAGIC %md ## Execution

# COMMAND ----------

# DBTITLE 1,Run
batch_id_param = dbutils.widgets.get("batch_id").strip()
full_refresh_param = dbutils.widgets.get("full_refresh") == "true"

if not batch_id_param:
    latest = spark.sql(f"SELECT source_batch_id FROM {SILVER} ORDER BY updated_timestamp DESC LIMIT 1").head()
    batch_id_param = latest["source_batch_id"] if latest else "GOLD_" + utc_now().strftime("%Y%m%d_%H%M%S")
    print(f"batch_id widget was empty; using latest Silver batch: {batch_id_param}")

gold_result = run_gold(batch_id_param, full_refresh_param)

# COMMAND ----------

# MAGIC %md ## Proof cells (screenshot these for the submission)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proof 1: Gold steps in the execution log (rows processed/inserted/updated + seconds per step)
# MAGIC SELECT execution_id, batch_id, layer, load_type, source_param AS gold_table, status,
# MAGIC        records_processed, records_inserted, records_updated, execution_duration_sec
# MAGIC FROM pipeline_execution_log
# MAGIC WHERE layer = 'SILVER_TO_GOLD'
# MAGIC ORDER BY execution_id DESC LIMIT 12;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proof 2: Row counts of the star schema
# MAGIC SELECT 'gold_fact_seismic_event' AS table_name, COUNT(*) AS row_count FROM gold_fact_seismic_event
# MAGIC UNION ALL SELECT 'gold_fact_daily_location_summary', COUNT(*) FROM gold_fact_daily_location_summary
# MAGIC UNION ALL SELECT 'gold_dim_date', COUNT(*) FROM gold_dim_date
# MAGIC UNION ALL SELECT 'gold_dim_location', COUNT(*) FROM gold_dim_location
# MAGIC UNION ALL SELECT 'gold_dim_magnitude_class', COUNT(*) FROM gold_dim_magnitude_class
# MAGIC UNION ALL SELECT 'gold_dim_depth_class', COUNT(*) FROM gold_dim_depth_class;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proof 3: Referential integrity — every FK resolves to a dimension row (all counts must be 0)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END)            AS orphan_date_keys,
# MAGIC   SUM(CASE WHEN l.location_key IS NULL THEN 1 ELSE 0 END)        AS orphan_location_keys,
# MAGIC   SUM(CASE WHEN m.magnitude_class_key IS NULL THEN 1 ELSE 0 END) AS orphan_magnitude_keys,
# MAGIC   SUM(CASE WHEN p.depth_class_key IS NULL THEN 1 ELSE 0 END)     AS orphan_depth_keys
# MAGIC FROM gold_fact_seismic_event f
# MAGIC LEFT JOIN gold_dim_date d            ON f.date_key = d.date_key
# MAGIC LEFT JOIN gold_dim_location l        ON f.location_key = l.location_key
# MAGIC LEFT JOIN gold_dim_magnitude_class m ON f.magnitude_class_key = m.magnitude_class_key
# MAGIC LEFT JOIN gold_dim_depth_class p     ON f.depth_class_key = p.depth_class_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proof 4: Gold matches Silver — active events and no duplicate event_id in the fact
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM silver_seismic_events)                                   AS silver_rows,
# MAGIC   (SELECT COUNT(*) FROM gold_fact_seismic_event)                                 AS fact_rows,
# MAGIC   (SELECT COUNT(*) FROM gold_fact_seismic_event WHERE is_active)                 AS fact_active_rows,
# MAGIC   (SELECT SUM(event_count) FROM gold_fact_daily_location_summary)                AS summary_event_total,
# MAGIC   (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM gold_fact_seismic_event)      AS duplicate_event_ids;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proof 5: Business question — shallow vs deep ratio by location (top 15 most active)
# MAGIC SELECT l.location_label,
# MAGIC        SUM(s.event_count)                                         AS events,
# MAGIC        ROUND(AVG(s.avg_magnitude), 2)                             AS avg_mag,
# MAGIC        SUM(s.shallow_count)                                       AS shallow,
# MAGIC        SUM(s.deep_count)                                          AS deep,
# MAGIC        ROUND(SUM(s.shallow_count) / NULLIF(SUM(s.deep_count), 0), 2) AS shallow_to_deep_ratio
# MAGIC FROM gold_fact_daily_location_summary s
# MAGIC JOIN gold_dim_location l ON s.location_key = l.location_key
# MAGIC GROUP BY l.location_label
# MAGIC ORDER BY events DESC
# MAGIC LIMIT 15;

# COMMAND ----------

dbutils.notebook.exit(json.dumps(gold_result, default=str))
