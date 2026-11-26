# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Create Lakebase projects with CDF enabled
# MAGIC
# MAGIC Creates N Lakebase Autoscaling projects (default 5), a load-test table in each one,
# MAGIC and turns on **Lakehouse Sync (CDF)** so every change in Postgres is replicated to a
# MAGIC Delta history table in Unity Catalog.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Create the projects (Postgres 17, autoscaling min/max CU from the widgets) |
# MAGIC | 2 | Create the `public.orders` table with `REPLICA IDENTITY FULL` + monitoring extensions |
# MAGIC | 3 | Create one UC schema per project and a CDF config that replicates `public` into it |
# MAGIC | 4 | Write the project registry to `<catalog>.<monitoring_schema>.projects` (read by notebooks 02 and 03). It stores each project's `project_uid`, the key that `system.billing.usage` uses in `usage_metadata.project_id` |
# MAGIC
# MAGIC The notebook is idempotent: re-running it skips anything that already exists.

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]>=3.2"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "UC catalog (not default storage)")
dbutils.widgets.text("monitoring_schema", "lakebase_monitoring", "UC schema for monitoring tables")
dbutils.widgets.text("project_prefix", "lb-monitoring-demo", "Lakebase project id prefix")
dbutils.widgets.text("num_projects", "5", "Number of projects")
dbutils.widgets.text("min_cu", "0.5", "Autoscaling min CU")
dbutils.widgets.text("max_cu", "4", "Autoscaling max CU")

CATALOG = dbutils.widgets.get("catalog")
MON_SCHEMA = dbutils.widgets.get("monitoring_schema")
PREFIX = dbutils.widgets.get("project_prefix")
NUM_PROJECTS = int(dbutils.widgets.get("num_projects"))
MIN_CU = float(dbutils.widgets.get("min_cu"))
MAX_CU = float(dbutils.widgets.get("max_cu"))

PROJECT_IDS = [f"{PREFIX}-{i:02d}" for i in range(1, NUM_PROJECTS + 1)]
print(PROJECT_IDS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers
# MAGIC The Lakebase REST API is called directly (`/api/2.0/postgres/...`) so the notebook does not
# MAGIC depend on a specific `databricks-sdk` version.

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists, DatabricksError

w = WorkspaceClient()
ME = w.current_user.me().user_name
API = "/api/2.0/postgres"


def api(method, path, **kw):
    return w.api_client.do(method, f"{API}/{path}", **kw)


def wait_operation(op, timeout_s=900):
    """Poll a long-running Lakebase operation until done."""
    name = op.get("name")
    deadline = time.time() + timeout_s
    while not op.get("done"):
        if time.time() > deadline:
            raise TimeoutError(f"operation {name} did not finish in {timeout_s}s")
        time.sleep(5)
        op = api("GET", name)
    if op.get("error"):
        raise RuntimeError(f"operation {name} failed: {op['error']}")
    return op


def get_endpoint(project_id):
    eps = api("GET", f"projects/{project_id}/branches/production/endpoints").get("endpoints", [])
    return next(e for e in eps if e["status"]["endpoint_type"] == "ENDPOINT_TYPE_READ_WRITE")


def get_database(project_id):
    dbs = api("GET", f"projects/{project_id}/branches/production/databases").get("databases", [])
    return next(d for d in dbs if d["status"]["postgres_database"] == "databricks_postgres")


def pg_connect(endpoint, autocommit=True):
    """Open a Postgres connection with a short-lived OAuth database credential."""
    cred = api("POST", "credentials", body={"endpoint": endpoint["name"]})
    return psycopg.connect(
        host=endpoint["status"]["hosts"]["host"],
        dbname="databricks_postgres",
        user=ME,
        password=cred["token"],
        sslmode="require",
        autocommit=autocommit,
        connect_timeout=30,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Create the projects

# COMMAND ----------

def ensure_project(project_id):
    try:
        api("GET", f"projects/{project_id}")
        return project_id, "exists"
    except NotFound:
        pass
    op = api(
        "POST",
        "projects",
        query={"project_id": project_id},
        body={
            "spec": {
                "display_name": project_id,
                "pg_version": 17,
                "default_endpoint_settings": {
                    "autoscaling_limit_min_cu": MIN_CU,
                    "autoscaling_limit_max_cu": MAX_CU,
                },
            }
        },
    )
    wait_operation(op)
    return project_id, "created"


with ThreadPoolExecutor(max_workers=NUM_PROJECTS) as pool:
    for pid, state in pool.map(ensure_project, PROJECT_IDS):
        print(f"{pid}: {state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create the load-test table
# MAGIC `REPLICA IDENTITY FULL` is required by Lakehouse Sync. Column types are limited to the ones CDF supports.

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS public.orders (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  INT NOT NULL,
    region       TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    note         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS orders_customer_idx ON public.orders (customer_id);
CREATE INDEX IF NOT EXISTS orders_created_idx  ON public.orders (created_at);
"""

# pg_stat_statements: server-side statement stats | neon_utils: num_cpus() | neon: pg_cluster_size()
EXTENSIONS = ["pg_stat_statements", "neon_utils", "neon"]


def setup_postgres(project_id):
    ep = get_endpoint(project_id)
    info = {"project_id": project_id}
    with pg_connect(ep) as conn:
        conn.execute(DDL)
        for ext in EXTENSIONS:
            try:
                conn.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
                info[ext] = "ok"
            except Exception as e:  # extension not offered on this platform version
                info[ext] = f"unavailable: {str(e).splitlines()[0]}"
        info["pg_version"] = conn.execute("SHOW server_version").fetchone()[0]
        try:
            info["num_cpus_now"] = float(conn.execute("SELECT num_cpus()").fetchone()[0])
        except Exception as e:
            info["num_cpus_now"] = f"n/a: {str(e).splitlines()[0]}"
    return info


with ThreadPoolExecutor(max_workers=NUM_PROJECTS) as pool:
    setup_results = list(pool.map(setup_postgres, PROJECT_IDS))
for info in setup_results:
    print(info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Enable CDF (Lakehouse Sync) per project
# MAGIC Each project replicates into its own UC schema, because every project produces a table with the same
# MAGIC name (`lb_orders_history`).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MON_SCHEMA}")


def uc_schema_for(project_id):
    return project_id.replace("-", "_")


def ensure_cdf(project_id):
    uc_schema = uc_schema_for(project_id)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{uc_schema}")
    db = get_database(project_id)
    try:
        existing = api("GET", f"{db['name']}/cdf-configs").get("cdf_configs", [])
    except NotFound:
        existing = []
    if existing:
        return project_id, uc_schema, "exists", existing[0].get("name")
    try:
        op = api(
            "POST",
            f"{db['name']}/cdf-configs",
            query={"cdf_config_id": "public_to_uc"},
            body={"catalog": CATALOG, "schema": uc_schema, "postgres_schema": "public"},
        )
        if "/operations/" in str(op.get("name", "")):
            op = wait_operation(op)
        return project_id, uc_schema, "created", (op.get("response") or op).get("name")
    except ResourceAlreadyExists:
        return project_id, uc_schema, "exists", None


cdf_results = [ensure_cdf(p) for p in PROJECT_IDS]
for r in cdf_results:
    print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Project registry

# COMMAND ----------

from datetime import datetime, timezone

WORKSPACE_ID = str(w.get_workspace_id())

rows = []
for pid in PROJECT_IDS:
    project = api("GET", f"projects/{pid}")
    ep = get_endpoint(pid)
    db = get_database(pid)
    st = ep["status"]
    rows.append(
        (
            pid,
            project["uid"],
            WORKSPACE_ID,
            ep["name"],
            ep["uid"],
            st["hosts"]["host"],
            db["name"],
            CATALOG,
            uc_schema_for(pid),
            float(st.get("autoscaling_limit_min_cu", MIN_CU)),
            float(st.get("autoscaling_limit_max_cu", MAX_CU)),
            datetime.now(timezone.utc),
        )
    )

schema = (
    "project_id string, project_uid string, workspace_id string, endpoint_name string, endpoint_uid string, "
    "host string, database_name string, "
    "uc_catalog string, uc_schema string, min_cu double, max_cu double, registered_at timestamp"
)
(
    spark.createDataFrame(rows, schema)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{MON_SCHEMA}.projects")
)
display(spark.table(f"{CATALOG}.{MON_SCHEMA}.projects"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify CDF from the Postgres side
# MAGIC `wal2delta.tables` lists the tables Lakehouse Sync is tracking in each project.

# COMMAND ----------

wal2delta = {}
for pid in PROJECT_IDS:
    with pg_connect(get_endpoint(pid)) as conn:
        try:
            cur = conn.execute("SELECT * FROM wal2delta.tables")
            cols = [c.name for c in cur.description]
            wal2delta[pid] = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception as e:
            wal2delta[pid] = "wal2delta not ready: " + str(e).splitlines()[0]
    print(pid, wal2delta[pid])

# COMMAND ----------

import json

dbutils.notebook.exit(
    json.dumps({"postgres_setup": setup_results, "cdf": cdf_results, "wal2delta": wal2delta}, default=str)
)
