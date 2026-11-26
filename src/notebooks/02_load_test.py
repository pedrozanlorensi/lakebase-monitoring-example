# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Load test the Lakebase projects
# MAGIC
# MAGIC Runs the same workload against every project in the registry (created by notebook 01), all projects in parallel:
# MAGIC
# MAGIC | Phase | Workload | Default |
# MAGIC |---|---|---|
# MAGIC | `ingest` | Batched multi-row `INSERT`s from several writer connections | 100,000 rows per project |
# MAGIC | `read` | Mix of point lookups, index lookups, range scans and aggregates | 20,000 queries per project |
# MAGIC | `mutate` | Single-row `UPDATE`s and `DELETE`s (these produce update/delete events in the CDF feed) | 5,000 updates, 500 deletes |
# MAGIC
# MAGIC While the workload runs, a sampler connection per project records, every few seconds:
# MAGIC - `num_cpus`: vCPUs currently allocated to the compute (`neon_utils.num_cpus()`, an integer)
# MAGIC - `active_time_ms`: cumulative time sessions spent executing statements (`pg_stat_database.active_time`).
# MAGIC   Its rate of change is the **database load** in "busy sessions", which is compared with the allocated vCPUs
# MAGIC   to get compute utilization
# MAGIC - `storage_bytes`: logical size of the whole cluster (`neon.pg_cluster_size()`)
# MAGIC - connections, committed transactions and the CDF lag in WAL bytes (`wal2delta.tables`)
# MAGIC
# MAGIC **Outputs** (Delta, in `<catalog>.<monitoring_schema>`), all keyed by `run_id` and `project_id`:
# MAGIC - `query_latency`: one row per client-side operation, with latency in ms
# MAGIC - `metrics_samples`: time series sampled from each project
# MAGIC - `pg_stat_statements_snapshot`: server-side statement statistics at the end of the run
# MAGIC
# MAGIC Cost is **not** measured here: it comes from `system.billing.usage` and `system.billing.list_prices`, joined in
# MAGIC notebook **03 · Create the monitoring views**. Run it afterwards to build the views the dashboard reads.

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]>=3.2"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "UC catalog")
dbutils.widgets.text("monitoring_schema", "lakebase_monitoring", "UC schema for monitoring tables")
dbutils.widgets.text("rows_per_project", "100000", "Rows to ingest per project")
dbutils.widgets.text("batch_size", "500", "Rows per INSERT batch")
dbutils.widgets.text("writer_threads", "4", "Writer connections per project")
dbutils.widgets.text("reader_threads", "4", "Reader connections per project")
dbutils.widgets.text("reads_per_thread", "5000", "Read queries per reader")
dbutils.widgets.text("update_rows", "5000", "Single-row updates per project")
dbutils.widgets.text("delete_rows", "500", "Single-row deletes per project")
dbutils.widgets.text("sample_interval_s", "5", "Metrics sampling interval (s)")

CATALOG = dbutils.widgets.get("catalog")
MON_SCHEMA = dbutils.widgets.get("monitoring_schema")
ROWS = int(dbutils.widgets.get("rows_per_project"))
BATCH = int(dbutils.widgets.get("batch_size"))
WRITERS = int(dbutils.widgets.get("writer_threads"))
READERS = int(dbutils.widgets.get("reader_threads"))
READS_PER_THREAD = int(dbutils.widgets.get("reads_per_thread"))
UPDATE_ROWS = int(dbutils.widgets.get("update_rows"))
DELETE_ROWS = int(dbutils.widgets.get("delete_rows"))
SAMPLE_S = float(dbutils.widgets.get("sample_interval_s"))

MON = f"{CATALOG}.{MON_SCHEMA}"

# COMMAND ----------

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ME = w.current_user.me().user_name
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

projects = [r.asDict() for r in spark.table(f"{MON}.projects").orderBy("project_id").collect()]
print("run_id:", RUN_ID)
print("projects:", [p["project_id"] for p in projects])


def pg_connect(p, autocommit=True):
    """Connect with a short-lived OAuth database credential (valid 1 hour)."""
    cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": p["endpoint_name"]})
    return psycopg.connect(
        host=p["host"],
        dbname="databricks_postgres",
        user=ME,
        password=cred["token"],
        sslmode="require",
        autocommit=autocommit,
        connect_timeout=30,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workload definition

# COMMAND ----------

REGIONS = ["NA", "LATAM", "EMEA", "APAC", "ANZ"]
STATUSES = ["new", "paid", "shipped", "delivered", "cancelled"]
N_CUSTOMERS = 5000

INSERT_SQL = """
INSERT INTO public.orders (customer_id, region, status, amount, note)
SELECT * FROM unnest(%s::int[], %s::text[], %s::text[], %s::numeric[], %s::text[])
"""

READ_QUERIES = {
    # op name -> (sql, parameter factory taking (rng, max_id))
    "read_point": (
        "SELECT * FROM public.orders WHERE id = %s",
        lambda rng, max_id: (rng.randint(1, max_id),),
    ),
    "read_customer": (
        "SELECT id, status, amount, created_at FROM public.orders WHERE customer_id = %s ORDER BY created_at DESC LIMIT 50",
        lambda rng, max_id: (rng.randint(1, N_CUSTOMERS),),
    ),
    "read_range": (
        "SELECT id, customer_id, amount FROM public.orders WHERE id BETWEEN %s AND %s",
        lambda rng, max_id: (lambda a: (a, a + 500))(rng.randint(1, max(1, max_id - 500))),
    ),
    "read_aggregate": (
        "SELECT region, status, count(*), sum(amount) FROM public.orders WHERE customer_id BETWEEN %s AND %s GROUP BY region, status",
        lambda rng, max_id: (lambda a: (a, a + 250))(rng.randint(1, N_CUSTOMERS - 250)),
    ),
}
READ_MIX = ["read_point"] * 5 + ["read_customer"] * 3 + ["read_range"] * 1 + ["read_aggregate"] * 1

latency_rows = []   # (run_id, project_id, phase, op, started_at, latency_ms, rows)
sample_rows = []    # see smp_schema below
lock = threading.Lock()


def record(project_id, phase, op, started, t0, rows):
    latency_ms = (time.perf_counter() - t0) * 1000.0
    with lock:
        latency_rows.append((RUN_ID, project_id, phase, op, started, latency_ms, rows))


def writer(p, n_rows, seed):
    rng = random.Random(seed)
    with pg_connect(p) as conn:
        done = 0
        while done < n_rows:
            n = min(BATCH, n_rows - done)
            params = (
                [rng.randint(1, N_CUSTOMERS) for _ in range(n)],
                [rng.choice(REGIONS) for _ in range(n)],
                ["new"] * n,
                [round(rng.uniform(5, 2500), 2) for _ in range(n)],
                [f"load-test {RUN_ID} batch row {done + i}" for i in range(n)],
            )
            started, t0 = datetime.now(timezone.utc), time.perf_counter()
            conn.execute(INSERT_SQL, params)
            record(p["project_id"], "ingest", "insert_batch", started, t0, n)
            done += n


def reader(p, n_queries, max_id, seed):
    rng = random.Random(seed)
    with pg_connect(p) as conn:
        for _ in range(n_queries):
            op = rng.choice(READ_MIX)
            sql, make_params = READ_QUERIES[op]
            started, t0 = datetime.now(timezone.utc), time.perf_counter()
            rows = conn.execute(sql, make_params(rng, max_id)).fetchall()
            record(p["project_id"], "read", op, started, t0, len(rows))


def mutator(p, ids_to_update, ids_to_delete, seed):
    rng = random.Random(seed)
    with pg_connect(p) as conn:
        for i in ids_to_update:
            started, t0 = datetime.now(timezone.utc), time.perf_counter()
            cur = conn.execute(
                "UPDATE public.orders SET status = %s, updated_at = clock_timestamp() WHERE id = %s",
                (rng.choice(STATUSES[1:]), i),
            )
            record(p["project_id"], "mutate", "update_row", started, t0, cur.rowcount)
        for i in ids_to_delete:
            started, t0 = datetime.now(timezone.utc), time.perf_counter()
            cur = conn.execute("DELETE FROM public.orders WHERE id = %s", (i,))
            record(p["project_id"], "mutate", "delete_row", started, t0, cur.rowcount)


SAMPLE_SQL = """
SELECT num_cpus()::float8,
       pg_cluster_size(),
       pg_database_size(current_database()),
       (SELECT sum(numbackends) FROM pg_stat_database),
       (SELECT sum(xact_commit) FROM pg_stat_database),
       (SELECT sum(active_time) FROM pg_stat_database),
       (SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), committed_lsn::pg_lsn)) FROM wal2delta.tables),
       (SELECT string_agg(DISTINCT status, ',') FROM wal2delta.tables)
"""


def sampler(p, stop):
    """Sample compute allocation, storage, connections and CDF lag on a dedicated connection."""
    conn = None
    while not stop.is_set():
        try:
            if conn is None or conn.closed:
                conn = pg_connect(p)
            r = conn.execute(SAMPLE_SQL).fetchone()
            with lock:
                sample_rows.append(
                    (RUN_ID, p["project_id"], datetime.now(timezone.utc), float(r[0]), int(r[1]), int(r[2]), int(r[3]),
                     int(r[4]), float(r[5]), float(r[6]) if r[6] is not None else None, r[7])
                )
        except Exception as e:
            print(f"[sampler {p['project_id']}] {str(e).splitlines()[0]}")
            conn = None
        stop.wait(SAMPLE_S)
    if conn is not None and not conn.closed:
        conn.close()


def run_project(p):
    pid = p["project_id"]
    seed = hash(pid) % 10_000
    stop = threading.Event()
    s = threading.Thread(target=sampler, args=(p, stop), daemon=True)
    s.start()
    try:
        time.sleep(SAMPLE_S * 2)  # baseline samples before load
        # --- ingest
        per_writer = [ROWS // WRITERS + (1 if i < ROWS % WRITERS else 0) for i in range(WRITERS)]
        with ThreadPoolExecutor(WRITERS) as pool:
            list(pool.map(lambda a: writer(p, a[1], seed + a[0]), enumerate(per_writer)))
        with pg_connect(p) as conn:
            conn.execute("ANALYZE public.orders")
            min_id, max_id, total = conn.execute("SELECT min(id), max(id), count(*) FROM public.orders").fetchone()
        # --- read
        with ThreadPoolExecutor(READERS) as pool:
            list(pool.map(lambda i: reader(p, READS_PER_THREAD, max_id, seed + 100 + i), range(READERS)))
        # --- mutate (operate on rows inserted by this run: the highest ids)
        rng = random.Random(seed + 999)
        candidates = rng.sample(range(max(min_id, max_id - ROWS + 1), max_id + 1), min(ROWS, UPDATE_ROWS + DELETE_ROWS))
        mutator(p, candidates[:UPDATE_ROWS], candidates[UPDATE_ROWS:UPDATE_ROWS + DELETE_ROWS], seed + 7)
        time.sleep(SAMPLE_S * 6)  # cool-down samples, lets CDF lag drain
        return pid, total
    finally:
        stop.set()
        s.join(timeout=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the load test (all projects in parallel)

# COMMAND ----------

t_start = time.time()
with ThreadPoolExecutor(len(projects)) as pool:
    results = list(pool.map(run_project, projects))
elapsed = time.time() - t_start
for pid, total in results:
    print(f"{pid}: {total:,} rows in public.orders")
print(f"elapsed: {elapsed:,.0f}s | latency records: {len(latency_rows):,} | metric samples: {len(sample_rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist results

# COMMAND ----------

lat_schema = "run_id string, project_id string, phase string, op string, started_at timestamp, latency_ms double, rows int"
spark.createDataFrame(latency_rows, lat_schema).write.mode("append").saveAsTable(f"{MON}.query_latency")

smp_schema = (
    "run_id string, project_id string, sampled_at timestamp, num_cpus double, storage_bytes long, db_size_bytes long, "
    "connections int, xact_commit long, active_time_ms double, cdf_lag_bytes double, cdf_status string"
)
spark.createDataFrame(sample_rows, smp_schema).write.mode("append").saveAsTable(f"{MON}.metrics_samples")

# Server-side view of the same workload: pg_stat_statements (cumulative since the extension was created)
PGSS_SQL = """
SELECT left(query, 300), calls, total_exec_time, mean_exec_time, stddev_exec_time, max_exec_time, rows
FROM pg_stat_statements
WHERE query ILIKE '%%public.orders%%' AND query NOT ILIKE '%%pg_stat_statements%%'
ORDER BY total_exec_time DESC LIMIT 25
"""
pgss_rows = []
for p in projects:
    with pg_connect(p) as conn:
        for r in conn.execute(PGSS_SQL).fetchall():
            pgss_rows.append((RUN_ID, p["project_id"], datetime.now(timezone.utc), r[0], int(r[1]),
                              float(r[2]), float(r[3]), float(r[4]), float(r[5]), int(r[6])))
pgss_schema = (
    "run_id string, project_id string, captured_at timestamp, query string, calls long, total_exec_ms double, "
    "mean_exec_ms double, stddev_exec_ms double, max_exec_ms double, rows long"
)
spark.createDataFrame(pgss_rows, pgss_schema).write.mode("append").saveAsTable(f"{MON}.pg_stat_statements_snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for CDF to catch up
# MAGIC Every insert of this run should be visible in each project's `lb_orders_history` Delta table before the
# MAGIC monitoring views are (re)built by notebook 03.

# COMMAND ----------

inserts_sql = " UNION ALL ".join(
    f"SELECT '{p['project_id']}' AS project_id, count_if(_pg_change_type = 'insert') AS n "
    f"FROM {p['uc_catalog']}.{p['uc_schema']}.lb_orders_history"
    for p in projects
)
deadline = time.time() + 600
while True:
    counts = {r["project_id"]: r["n"] for r in spark.sql(inserts_sql).collect()}
    behind = {pid: n for pid, n in counts.items() if n < ROWS}
    if not behind or time.time() > deadline:
        break
    print("waiting for CDF, inserts replicated so far:", behind)
    time.sleep(20)
print("replicated inserts per project:", counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run summary

# COMMAND ----------

summary = spark.sql(f"""
SELECT project_id, op, count(*) AS operations, sum(rows) AS rows,
       round(percentile_approx(latency_ms, 0.5), 2)  AS p50_ms,
       round(percentile_approx(latency_ms, 0.95), 2) AS p95_ms,
       round(max(latency_ms), 2) AS max_ms
FROM {MON}.query_latency WHERE run_id = '{RUN_ID}' GROUP BY ALL ORDER BY 1, 2
""")
display(summary)

# COMMAND ----------

import json

cpu = spark.sql(f"""
SELECT project_id, min(num_cpus) AS min_cpus, max(num_cpus) AS max_cpus, count(*) AS samples,
       max(storage_bytes) AS storage_bytes, max(db_size_bytes) AS db_size_bytes,
       round((max(active_time_ms) - min(active_time_ms)) / 1000, 1) AS active_s,
       max(cdf_lag_bytes) AS max_cdf_lag_bytes
FROM {MON}.metrics_samples WHERE run_id = '{RUN_ID}' GROUP BY ALL ORDER BY 1
""").collect()

dbutils.notebook.exit(json.dumps({
    "run_id": RUN_ID,
    "elapsed_s": round(elapsed),
    "rows_in_orders": dict(results),
    "replicated_inserts": counts,
    "latency_records": len(latency_rows),
    "samples": [r.asDict() for r in cpu],
}, default=str))
