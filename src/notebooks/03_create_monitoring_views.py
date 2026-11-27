# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Create the monitoring views
# MAGIC
# MAGIC Creates the Unity Catalog views the **Lakebase fleet monitoring** dashboard reads. It only issues
# MAGIC `CREATE OR REPLACE VIEW`, so it is safe to re-run at any time.
# MAGIC
# MAGIC | View | Source | Grain | Used by the dashboard for |
# MAGIC |---|---|---|---|
# MAGIC | `v_lakebase_cost` | `system.billing.usage` × `system.billing.list_prices` | project × hour × cost type | Compute cost, storage cost |
# MAGIC | `v_billing_status` | `v_lakebase_cost` | one row | Billing data freshness note |
# MAGIC | `v_compute_samples` | `metrics_samples` (notebook 02) | project × sample | CU utilization, database load |
# MAGIC | `v_latency_by_op` | `query_latency` (notebook 02) | project × operation | P95 latency by operation |
# MAGIC | `cdf_events` | each project's `lb_orders_history` (Lakebase CDF) | replicated change | base for the CDF views |
# MAGIC | `v_cdf_timeseries` | `cdf_events` | project × 5 s | CDF replication latency over time |
# MAGIC | `v_project_summary` | all of the above | project | KPI tiles and the detail table |
# MAGIC
# MAGIC **Time scope.** Cost covers everything billed since each project was created. The performance views
# MAGIC (`v_compute_samples`, `v_latency_by_op`, `v_cdf_timeseries`) show the **latest load-test run**.
# MAGIC
# MAGIC **Requirements.** `SELECT` on `system.billing.usage` and `system.billing.list_prices`. Billing records reach
# MAGIC the system table a few hours after the usage happens, so the cost of a brand-new project is empty at first.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "UC catalog")
dbutils.widgets.text("monitoring_schema", "lakebase_monitoring", "UC schema for monitoring tables")

CATALOG = dbutils.widgets.get("catalog")
MON_SCHEMA = dbutils.widgets.get("monitoring_schema")
MON = f"{CATALOG}.{MON_SCHEMA}"

projects = [r.asDict() for r in spark.table(f"{MON}.projects").orderBy("project_id").collect()]
print([p["project_id"] for p in projects])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost, from the billing system tables
# MAGIC Lakebase usage is recorded in `system.billing.usage` with `billing_origin_product = 'LAKEBASE'`:
# MAGIC
# MAGIC | `usage_type` | Unit | What it is | `product_features.lakebase` |
# MAGIC |---|---|---|---|
# MAGIC | `COMPUTE_TIME` | DBU | Compute of each endpoint, in 10-minute records | `compute_type` |
# MAGIC | `STORAGE_SPACE` | DSU | Storage of each branch, in hourly records | `storage_type` |
# MAGIC
# MAGIC Each record carries the project in `usage_metadata.project_id`. That value is the project **UID**, not the
# MAGIC project name, which is why notebook 01 stores `project_uid` in the registry. Usage is priced with
# MAGIC `system.billing.list_prices` (`pricing.effective_list.default`), so the amounts are **list price in USD**
# MAGIC and do not reflect negotiated discounts or commitments.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_lakebase_cost AS
SELECT p.project_id,
       u.usage_date,
       date_trunc('HOUR', u.usage_start_time) AS usage_hour,
       CASE u.usage_type WHEN 'COMPUTE_TIME' THEN 'Compute' WHEN 'STORAGE_SPACE' THEN 'Storage' ELSE u.usage_type END AS cost_type,
       coalesce(u.product_features.lakebase.compute_type, u.product_features.lakebase.storage_type) AS usage_detail,
       u.sku_name,
       u.usage_unit,
       CAST(sum(u.usage_quantity) AS DOUBLE) AS usage_quantity,
       CAST(sum(u.usage_quantity * lp.pricing.effective_list.default) AS DOUBLE) AS list_cost_usd
FROM system.billing.usage u
JOIN {MON}.projects p
  ON u.usage_metadata.project_id = p.project_uid AND u.workspace_id = p.workspace_id
LEFT JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud AND u.usage_unit = lp.usage_unit
 AND u.usage_end_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.billing_origin_product = 'LAKEBASE'
GROUP BY ALL
""")

# One row that says how fresh the billing data is, so an empty cost chart is never ambiguous.
spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_billing_status AS
SELECT CASE
         WHEN count(*) = 0 THEN 'No billing records yet for these projects. Usage reaches system.billing.usage a few hours after it happens.'
         ELSE concat('Billing records available through ', date_format(max(usage_hour) + INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm'), ' UTC')
       END AS billing_status,
       count(DISTINCT project_id) AS projects_with_billing,
       coalesce(sum(list_cost_usd), 0) AS total_list_cost_usd
FROM {MON}.v_lakebase_cost
""")

display(spark.sql(f"""
SELECT project_id, cost_type, usage_unit, round(sum(usage_quantity), 4) AS usage_quantity,
       round(sum(list_cost_usd), 4) AS list_cost_usd, min(usage_hour) AS first_hour, max(usage_hour) AS last_hour
FROM {MON}.v_lakebase_cost GROUP BY ALL ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDF replication latency
# MAGIC [Lakebase Change Data Feed](https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf) captures every insert, update and delete from the Postgres write-ahead
# MAGIC log and stores it as a new row in a Unity Catalog managed Delta table, **batched and flushed every ~15 seconds**.
# MAGIC Replication latency is therefore expected to range from a few seconds up to about 15 seconds, depending on
# MAGIC where in the batch window a change was committed.
# MAGIC
# MAGIC `cdf_events` unions the `lb_orders_history` table of every project. For each replicated change:
# MAGIC
# MAGIC | Column | Definition |
# MAGIC |---|---|
# MAGIC | `source_ts` | Commit time in Postgres: `created_at` for inserts, `updated_at` for updates (both set with `clock_timestamp()`) |
# MAGIC | `delta_written_at` | When the Delta data file holding the row was written (`_metadata.file_modification_time`) |
# MAGIC | `replication_latency_s` | `delta_written_at − source_ts`: how long until the change was available in the Delta table |
# MAGIC | `processed_at` | The feed's `_timestamp` column: when the change was processed, before the batch is flushed |
# MAGIC | `processing_latency_s` | `processed_at − source_ts`: typically milliseconds, **not** the time to reach Delta |
# MAGIC
# MAGIC Deletes and update pre-images carry no fresh source timestamp, so they are counted but have no latency.

# COMMAND ----------

selects = []
for p in projects:
    selects.append(f"""
    SELECT '{p['project_id']}' AS project_id, id, _pg_change_type AS change_type, _pg_lsn AS pg_lsn,
           to_timestamp(concat(date_format(_timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), 'Z')) AS processed_at,
           _metadata.file_modification_time AS delta_written_at,
           CASE _pg_change_type WHEN 'insert' THEN created_at WHEN 'update_postimage' THEN updated_at END AS source_ts
    FROM {p['uc_catalog']}.{p['uc_schema']}.lb_orders_history""")

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.cdf_events AS
SELECT project_id, id, change_type, pg_lsn, source_ts, processed_at, delta_written_at,
       (unix_micros(delta_written_at) - unix_micros(source_ts)) / 1e6 AS replication_latency_s,
       (unix_micros(processed_at) - unix_micros(source_ts)) / 1e6 AS processing_latency_s
FROM ({' UNION ALL '.join(selects)})
""")

display(spark.sql(f"""
SELECT project_id, change_type, count(*) AS events,
       round(percentile_approx(replication_latency_s, 0.50), 2) AS p50_latency_s,
       round(percentile_approx(replication_latency_s, 0.95), 2) AS p95_latency_s,
       round(max(replication_latency_s), 2) AS max_latency_s,
       round(percentile_approx(processing_latency_s, 0.95), 3) AS p95_processing_latency_s
FROM {MON}.cdf_events GROUP BY ALL ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance views (latest load-test run)
# MAGIC `cu_utilization` is the **database load divided by the vCPUs allocated to the compute**, capped at 100%.
# MAGIC Database load is the rate of change of `pg_stat_database.active_time`: seconds of statement execution per
# MAGIC second of wall clock, i.e. the average number of busy sessions.

# COMMAND ----------

LATEST = f"(SELECT max(run_id) FROM {MON}.metrics_samples)"

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_compute_samples AS
WITH s AS (
  SELECT m.*, p.min_cu, p.max_cu,
         lag(m.sampled_at)     OVER (PARTITION BY m.project_id ORDER BY m.sampled_at) AS prev_ts,
         lag(m.active_time_ms) OVER (PARTITION BY m.project_id ORDER BY m.sampled_at) AS prev_active_ms
  FROM {MON}.metrics_samples m
  JOIN {MON}.projects p USING (project_id)
  WHERE m.run_id = {LATEST}
),
d AS (
  SELECT *, (unix_micros(sampled_at) - unix_micros(prev_ts)) / 1e6 AS interval_s
  FROM s WHERE prev_ts IS NOT NULL
)
SELECT run_id, project_id, sampled_at,
       -- 10-second bucket shared by all projects, so their series line up on the same x values in charts
       timestamp_seconds(floor(unix_seconds(sampled_at) / 10) * 10) AS ts,
       num_cpus AS allocated_vcpu, min_cu, max_cu,
       (active_time_ms - prev_active_ms) / 1000 / interval_s AS db_load,
       least(1.0, (active_time_ms - prev_active_ms) / 1000 / interval_s / num_cpus) AS cu_utilization,
       storage_bytes / power(1024, 3) AS storage_gb,
       cdf_lag_bytes / power(1024, 2) AS cdf_lag_mb,
       connections, cdf_status
FROM d
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_latency_by_op AS
SELECT run_id, project_id, phase, op, count(*) AS operations, sum(rows) AS rows,
       percentile_approx(latency_ms, 0.50) AS p50_ms,
       percentile_approx(latency_ms, 0.95) AS p95_ms,
       percentile_approx(latency_ms, 0.99) AS p99_ms,
       max(latency_ms) AS max_ms
FROM {MON}.query_latency
WHERE run_id = {LATEST}
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_cdf_timeseries AS
SELECT project_id,
       -- bucketed by the commit time in Postgres
       timestamp_seconds(floor(unix_seconds(source_ts) / 5) * 5) AS ts,
       count(*) AS events,
       avg(replication_latency_s) AS avg_latency_s,
       percentile_approx(replication_latency_s, 0.50) AS p50_latency_s,
       percentile_approx(replication_latency_s, 0.95) AS p95_latency_s,
       max(replication_latency_s) AS max_latency_s
FROM {MON}.cdf_events
WHERE replication_latency_s IS NOT NULL
  AND source_ts >= (SELECT min(sampled_at) FROM {MON}.metrics_samples WHERE run_id = {LATEST})
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {MON}.v_project_summary AS
WITH compute AS (
  SELECT project_id, max(run_id) AS run_id, max(max_cu) AS max_cu, max(allocated_vcpu) AS peak_allocated_vcpu,
         avg(cu_utilization) AS avg_cu_utilization, max(cu_utilization) AS peak_cu_utilization,
         max(db_load) AS peak_db_load,
         max_by(storage_gb, sampled_at) AS storage_gb,
         max(cdf_lag_mb) AS peak_cdf_lag_mb
  FROM {MON}.v_compute_samples GROUP BY project_id
),
lat AS (
  SELECT project_id,
         percentile_approx(CASE WHEN phase = 'read' THEN latency_ms END, 0.95) AS p95_read_ms,
         percentile_approx(CASE WHEN op = 'update_row' THEN latency_ms END, 0.95) AS p95_update_ms,
         percentile_approx(CASE WHEN op = 'insert_batch' THEN latency_ms END, 0.95) AS p95_insert_batch_ms,
         sum(CASE WHEN op = 'insert_batch' THEN rows END) AS rows_ingested,
         count(*) AS operations
  FROM {MON}.query_latency WHERE run_id = {LATEST} GROUP BY project_id
),
cdf AS (
  SELECT project_id, count(*) AS cdf_events,
         percentile_approx(replication_latency_s, 0.50) AS p50_cdf_latency_s,
         percentile_approx(replication_latency_s, 0.95) AS p95_cdf_latency_s,
         max(replication_latency_s) AS max_cdf_latency_s
  FROM {MON}.cdf_events
  WHERE replication_latency_s IS NOT NULL
    AND source_ts >= (SELECT min(sampled_at) FROM {MON}.metrics_samples WHERE run_id = {LATEST})
  GROUP BY project_id
),
cost AS (
  SELECT project_id,
         sum(CASE WHEN cost_type = 'Compute' THEN list_cost_usd END) AS compute_cost_usd,
         sum(CASE WHEN cost_type = 'Storage' THEN list_cost_usd END) AS storage_cost_usd,
         sum(CASE WHEN cost_type = 'Compute' THEN usage_quantity END) AS compute_dbus,
         sum(CASE WHEN cost_type = 'Storage' THEN usage_quantity END) AS storage_dsus,
         max(usage_hour) AS billed_through
  FROM {MON}.v_lakebase_cost GROUP BY project_id
)
SELECT p.project_id, c.run_id, p.min_cu, p.max_cu,
       -- 0 means nothing has been billed yet; v_billing_status says how fresh the billing data is
       coalesce(b.compute_cost_usd, 0) AS compute_cost_usd, coalesce(b.storage_cost_usd, 0) AS storage_cost_usd,
       coalesce(b.compute_dbus, 0) AS compute_dbus, coalesce(b.storage_dsus, 0) AS storage_dsus, b.billed_through,
       c.avg_cu_utilization, c.peak_cu_utilization, c.peak_db_load, c.peak_allocated_vcpu, c.storage_gb, c.peak_cdf_lag_mb,
       l.p95_read_ms, l.p95_update_ms, l.p95_insert_batch_ms, l.rows_ingested, l.operations,
       f.cdf_events, f.p50_cdf_latency_s, f.p95_cdf_latency_s, f.max_cdf_latency_s
FROM {MON}.projects p
LEFT JOIN compute c USING (project_id)
LEFT JOIN lat l USING (project_id)
LEFT JOIN cdf f USING (project_id)
LEFT JOIN cost b USING (project_id)
""")

display(spark.table(f"{MON}.v_project_summary").orderBy("project_id"))

# COMMAND ----------

import json

VIEWS = ["v_lakebase_cost", "v_billing_status", "cdf_events", "v_compute_samples", "v_latency_by_op", "v_cdf_timeseries", "v_project_summary"]
row_counts = {v: spark.table(f"{MON}.{v}").count() for v in VIEWS}
print(row_counts)
dbutils.notebook.exit(json.dumps(row_counts))
