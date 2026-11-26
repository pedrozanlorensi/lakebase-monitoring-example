# lakebase-monitoring-example

An example of how to monitor a fleet of Lakebase projects from one place. It creates several Lakebase
Autoscaling projects with change data feed (Lakehouse Sync) enabled, runs a write and read load test against
them, and builds an AI/BI dashboard that answers five questions across all projects:

| Question | Where the answer comes from |
|---|---|
| Storage cost | `system.billing.usage` × `system.billing.list_prices` |
| Compute cost | `system.billing.usage` × `system.billing.list_prices` |
| CU utilization | Database load sampled from each project during the load test |
| P95 query latency | Client-side timing of every statement in the load test |
| CDF replication latency | Lakehouse Sync history tables in Unity Catalog |

## What is in the repo

```
databricks.yml                                   Declarative Automation Bundle definition and variables
resources/
  lakebase_monitoring.job.yml                    Job that runs the three notebooks in order
  lakebase_fleet_monitoring.dashboard.yml        AI/BI dashboard resource
src/notebooks/
  01_create_lakebase_projects.py                 Projects, load-test table, CDF config, project registry
  02_load_test.py                                Ingest, read and mutate workload, with metric sampling
  03_create_monitoring_views.py                  Unity Catalog views read by the dashboard
src/dashboards/
  lakebase_fleet_monitoring.lvdash.json          Dashboard definition
```

### 01 · Create Lakebase projects

Creates `num_projects` projects named `<project_prefix>-01`, `-02`, ... (Postgres 17, autoscaling 0.5 to 4 CU).
In each project it creates a `public.orders` table with `REPLICA IDENTITY FULL` and a CDF config that
replicates the `public` schema to its own Unity Catalog schema as `lb_orders_history`. It then writes the
registry table `<catalog>.<monitoring_schema>.projects`, including each project's UID. The notebook is
idempotent.

### 02 · Load test

Runs the same workload on every project in parallel:

| Phase | Workload | Default per project |
|---|---|---|
| Ingest | Multi-row `INSERT` batches from 4 writer connections | 100,000 rows |
| Read | Point lookups, index lookups, range scans, aggregates from 4 reader connections | 20,000 queries |
| Mutate | Single-row `UPDATE` and `DELETE` | 5,000 updates, 500 deletes |

It records the latency of every operation (`query_latency`) and samples each project every 5 seconds
(`metrics_samples`): allocated vCPUs, cumulative active session time, logical size, connections and the CDF
backlog. Every run appends another 100,000 rows to each project.

### 03 · Create monitoring views

| View | Source | Grain |
|---|---|---|
| `v_lakebase_cost` | `system.billing.usage`, `system.billing.list_prices` | project × hour × cost type |
| `v_compute_samples` | `metrics_samples` | project × sample |
| `v_latency_by_op` | `query_latency` | project × operation |
| `v_latency_timeseries` | `query_latency` | project × 5 seconds |
| `cdf_events` | each project's `lb_orders_history` | replicated change |
| `v_cdf_timeseries` | `cdf_events` | project × 5 seconds |
| `v_project_summary` | all of the above | project |

## How each metric is defined

**Storage cost and compute cost.** Lakebase usage is recorded in `system.billing.usage` with
`billing_origin_product = 'LAKEBASE'`. Compute is `usage_type = 'COMPUTE_TIME'` in DBUs, one record per endpoint
every 10 minutes. Storage is `usage_type = 'STORAGE_SPACE'` in DSUs, one record per branch every hour. Each
record identifies the project in `usage_metadata.project_id`, which holds the project **UID** rather than its
name; the registry table maps one to the other. Usage is multiplied by `pricing.effective_list.default` from
`system.billing.list_prices`, so amounts are list price in USD and do not include negotiated discounts. Cost
covers everything billed since each project was created, not only the load-test window.

**CU utilization.** Database load divided by the vCPUs allocated to the compute, capped at 100%. Database load
is the rate of change of `pg_stat_database.active_time`, that is, the average number of sessions executing a
statement. Allocated vCPUs come from `neon_utils.num_cpus()`. This is sampled by notebook 02, so it covers the
load-test window.

**P95 query latency.** The 95th percentile of the client-side latency of each statement, measured in notebook 02
from just before the statement is sent to just after the result is fetched. It includes the network round trip
between the notebook compute and the Lakebase endpoint.

**CDF replication latency.** For every replicated insert and update, the time between the commit in Postgres
(`created_at` or `updated_at`, both set with `clock_timestamp()`) and the `_timestamp` that Lakehouse Sync wrote
to the history table. Deletes and update pre-images are counted as events but carry no source timestamp, so they
have no latency.

## Requirements

- A Unity Catalog workspace with Lakebase Autoscaling and Lakehouse Sync (CDF) available.
- `SELECT` on `system.billing.usage` and `system.billing.list_prices`.
- A catalog that does not use default storage, where you can create schemas.
- Permission to create Lakebase projects, and a SQL warehouse for the dashboard.
- Databricks CLI 0.281.0 or later.

## Deploy and run

```bash
databricks bundle deploy \
  --var catalog=<catalog> \
  --var warehouse_id=<sql-warehouse-id> \
  --var project_prefix=<prefix>

databricks bundle run lakebase_monitoring \
  --var catalog=<catalog> \
  --var warehouse_id=<sql-warehouse-id> \
  --var project_prefix=<prefix>
```

The job creates the projects, runs the load test and builds the views. Then open the **Lakebase fleet
monitoring** dashboard. To run the load test again without touching the projects:

```bash
databricks bundle run lakebase_monitoring --only load_test,create_monitoring_views ...
```

## Things to know

- **Billing data lags.** Usage records reach `system.billing.usage` a few hours after the usage happens. Right
  after the first run the cost tiles are empty; they fill in without re-running anything, because
  `v_lakebase_cost` is a view over the system tables.
- **Performance views show the latest run.** `v_compute_samples`, `v_latency_*`, `v_cdf_timeseries` and the
  performance columns of `v_project_summary` are scoped to the most recent load-test run.
- **The projects keep running.** The `production` branch suspends after 24 hours of inactivity by default, so
  the projects bill until they are deleted.
- **Lakebase telemetry system tables.** Workspaces with the *Lakebase Observability in System Tables* preview
  enabled also get compute and database telemetry in `system.lakebase` (for example `compute_counters` and
  `compute_gauges`). This example does not depend on that preview.

## Clean up

```bash
databricks bundle destroy --var catalog=<catalog> --var warehouse_id=<sql-warehouse-id>

for i in 01 02 03 04 05; do
  databricks postgres delete-project projects/<prefix>-$i
done
```

Then drop the Unity Catalog schemas: `<catalog>.<monitoring_schema>` and one schema per project (the project id
with hyphens replaced by underscores).
