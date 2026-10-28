# PRD: Twinlake

Last updated: 2026-10-27

## Overview

One codebase runs in two places: developers build and test on an on-prem laptop stack, and only merged code runs in Databricks Free Edition. Only config changes between the two, so every developer writes pipelines the same way.

**Problem.** Without a shared framework, each developer picks their own structure, write logic, quality checks and orchestration. Code that works locally breaks in the cloud, and reviews turn into style debates.

**Purpose.** A learning and portfolio project (non-commercial), which fits Databricks Free Edition's terms.

### Goals

1. Same code runs unchanged in dev (on-prem) and prod (cloud); only `APP_ENV` and config differ.
2. Every pipeline follows one template: repo layout, job class, naming, quality rules, orchestration file.
3. Fast local loop with no JVM and no cloud cost.
4. Bad data is caught and quarantined before it reaches gold tables.
5. Prod changes only through a pull request and CI deploy.

### Non-goals

- Commercial production workloads (would need a paid Databricks workspace).
- Real-time streaming in the first version.
- dbt or Airflow (decided against; see Orchestration).
- Multi-developer environments beyond Git branches for now.

### Decisions

| Topic | Decision |
| --- | --- |
| Prod platform | Databricks Free Edition |
| Dev storage | RustFS (S3-compatible) |
| Dev compute | LakeSail (Sail) via Spark Connect |
| Table format | Delta Lake, 3-part names, Unity Catalog OSS in dev (Hive Metastore as fallback), `catalog.schema.table` |
| Transformation | PySpark jobs + SQL files (no dbt) |
| Orchestration | `pipeline.yml` → local runner (dev), optional Dagster, Lakeflow Jobs (prod) |
| Data quality | `twinlake/quality`, YAML rules per table |
| CI/CD | GitHub Actions |
| Dev setup | Single laptop, Windows + WSL2, work isolated by Git branches |
| First data | Your bronze OLTP DDL, refined for business needs, with generated sample data |

### Success criteria

- [ ] A pipeline passes locally on Sail and runs in Databricks with zero code changes.
- [ ] A new pipeline needs only job files, one `pipeline.yml` and one quality YAML per table.
- [ ] Every pull request runs lint, quality-rule lint, Sail compatibility check and tests.
- [ ] Rows failing error-level rules never reach silver or gold tables.

## Architecture

Each layer has a dev tool and a prod tool, but code only touches the shared contract in the middle, so the same files run in both.

![Architecture: dev vs prod per layer](diagrams/01-architecture.svg)

Read across a row: the dev and prod tools differ, the contract between them does not. Developers work on branches locally; only CI deploys to prod after a merge. Secrets follow the same pattern: `.env`/Vault in dev, Databricks secret scopes in prod, both read through `get_secret()`.

### Tool choices

- **RustFS over MinIO:** Apache 2.0, 1.0 GA in September 2026; MinIO ended maintenance of its open-source edition. Start fresh rather than migrating MinIO data, since on-disk compatibility is still preview.
- **Sail over JVM Spark:** Rust-native, no JVM, instant startup, drop-in for PySpark over Spark Connect, native Delta. Databricks serverless is also Spark Connect, so one API rule covers both.
- **Databricks Free over Fabric:** Free Edition does not expire; the Fabric trial lasts 60 days.

**Dev serving: Metabase on Trino.** Metabase runs as one container and is quick to set up for a single developer; Superset needs several services. Trino reads the gold Delta tables on RustFS. Both start only under a `serving` Docker Compose profile, so they don't take laptop memory during normal pipeline work. In prod, gold tables are served through the Databricks SQL warehouse and dashboards.

## Data flow

Data moves bronze → silver → gold, and a quality check guards each write into silver and gold, so failing rows never reach the served layer.

![Data flow: bronze, silver, gold with quality gates](diagrams/02-data-flow.svg)

Warn-level rules only count failures; error-level rows go to quarantine. A failed dataset-level rule, or a quarantine share above the table's threshold, stops the job before it writes.

| Layer | Contents | Write pattern | Typical job |
| --- | --- | --- | --- |
| Bronze | Raw copy of OLTP tables, plus `_ingested_at`, `_source` | Append only | PySpark |
| Silver | Cleaned, typed, deduplicated entities | Merge on business key | PySpark |
| Gold | Business-ready KPI tables; the only served layer | Overwrite or overwrite by partition | SQL |
| Quarantine | Rows that failed error-level rules, with reasons | Append | Framework |

The exact bronze tables come from your OLTP DDL; silver and gold models are designed after the DDL review (see Prerequisites).

## Framework requirements

Developers write only business logic: a `transform()` method or a single SQL query. Reading, writing, quality checks, logging and environment handling live in `twinlake/`, owned by the platform team.

### Repo template

```
project/
├── conf/
│   ├── base.yml / dev.yml / prod.yml   # env config (catalog=dev|prod)
│   └── quality/<table>.yml             # data quality rules
├── pipelines/<domain>/
│   ├── pipeline.yml                    # tasks, deps, schedule
│   ├── bronze_*.py / silver_*.py       # PySpark jobs (BaseJob)
│   └── gold_*.sql                      # SQL jobs (SqlJob)
├── twinlake/
│   ├── context.py                      # env, spark session, config, secrets
│   ├── io.py                           # read_table / write_table
│   ├── base_job.py                     # BaseJob + SqlJob
│   ├── runner.py                       # local pipeline runner
│   └── quality/                        # spec.py, engine.py, lint.py
├── orchestration/
│   ├── to_bundle.py                    # pipeline.yml → databricks.yml
│   └── to_dagster.py                   # optional
├── tests/
├── .github/workflows/ci.yml
└── .pre-commit-config.yaml             # ruff, sqlfluff, yaml lint
```

### Functional requirements

| ID | Requirement |
| --- | --- |
| FR-1 | `Context` picks the Spark session from `APP_ENV`: Sail at `sc://localhost:50051` in dev, the Databricks session in prod |
| FR-2 | Table names are written without catalog (`silver.sales`); the framework prefixes `dev.` or `prod.` |
| FR-3 | `BaseJob` handles read, transform, quality, write; developers set `source`, `target`, `write_mode`, `merge_keys`, `quality` |
| FR-4 | `SqlJob` runs a `.sql` file holding one `SELECT`; target, write mode and quality file sit in a header comment |
| FR-5 | Write modes: `append` (bronze only), `overwrite`, `overwrite_partition`, `merge` |
| FR-6 | Secrets come only through `get_secret()`: `.env`/Vault in dev, secret scopes in prod |
| FR-7 | Databricks-only APIs (e.g. `dbutils`) are wrapped in `twinlake/` with a local fallback |
| FR-8 | Code uses only Spark Connect-safe APIs: no RDDs, `sparkContext`, JVM calls or `cache()` |

### PySpark job example

```python
class SilverSales(BaseJob):
    source = "bronze.sales_raw"
    target = "silver.sales"
    write_mode = "merge"
    merge_keys = ["order_id"]
    quality = "conf/quality/silver_sales.yml"

    def transform(self, df):
        return df.dropDuplicates(["order_id"]).filter("amount >= 0")
```

### SQL job example

```sql
-- target: gold.sales_daily
-- write_mode: overwrite
-- quality: conf/quality/gold_sales_daily.yml
SELECT order_date, status, COUNT(*) AS orders, SUM(amount) AS revenue
FROM silver.sales
GROUP BY order_date, status
```

Use SQL when the logic is a single query; use PySpark when it needs loops, UDFs or several steps.

### Conventions

- **Naming:** `snake_case`; tables as `<layer>.<domain>_<entity>`.
- **Audit columns** on every table: `_ingested_at`, `_source`.
- **Idempotent jobs:** merge or overwrite-by-partition; blind append only in bronze.
- **No hand-written orchestrator code**; everything is generated from `pipeline.yml`.

## Orchestration

Each pipeline is defined once in `pipeline.yml`; generators turn it into a local run, an optional Dagster job, or a Databricks Lakeflow job.

![Orchestration: one pipeline.yml, three runtimes](diagrams/03-orchestration.svg)

Dashed boxes are optional. Prod scheduling lives in Lakeflow, so the dev side only needs to run tasks in the right order.

```yaml
name: sales
schedule: "0 2 * * *"        # used in prod only
tasks:
  - key: bronze_ingest
    file: bronze_ingest.py
  - key: silver_clean
    file: silver_clean.py
    depends_on: [bronze_ingest]
    retries: 2
  - key: gold_daily
    file: gold_daily.sql
    depends_on: [silver_clean]
```

| ID | Requirement |
| --- | --- |
| OR-1 | `pipeline.yml` is a subset of the Databricks job format: `key`, `file`, `depends_on`, `retries`, `schedule` |
| OR-2 | `runner.py` sorts tasks by `depends_on`, applies retries, supports `--from` and `--only` |
| OR-3 | `to_bundle.py` generates `databricks.yml`; CI validates and deploys it |
| OR-4 | Developers never hand-write Airflow, Dagster or bundle code |

**Why not Airflow:** its scheduler, metadata database and webserver would go unused in dev, and its DAGs differ from Lakeflow jobs. If needed later, a `to_airflow.py` generator can be added without touching pipelines.

## Data quality requirements

Each table has one YAML rule file; the shared engine in `twinlake/quality` applies it identically on Sail and Databricks. Developers never write quality code.

```yaml
table: silver.sales
fail_if_quarantine_ratio_above: 0.05
checks:
  - {name: order_id_not_null, rule: not_null, columns: [order_id, order_date]}
  - {name: amount_non_negative, rule: between, columns: [amount], params: {min: 0}}
  - {name: status_valid, rule: in_set, columns: [status], params: {values: [NEW, PAID, SHIPPED, CANCELLED]}}
  - {name: email_format, rule: regex, columns: [email], params: {pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'}, severity: warn}
  - {name: ship_after_order, rule: expr, params: {expr: "ship_date IS NULL OR ship_date >= order_date"}}
  - {name: order_id_unique, rule: unique, columns: [order_id]}
  - {name: not_empty, rule: row_count, params: {min: 1}}
```

| Check type | Behavior |
| --- | --- |
| Row check, `severity: error` | Failing rows go to `quarantine.<table>` with `_dq_errors` listing failed rules |
| Row check, `severity: warn` | Counted in the report; rows pass through |
| Dataset check (`unique`, `row_count`), error | Fails the job |
| Quarantine ratio above threshold | Fails the job |

| ID | Requirement |
| --- | --- |
| DQ-1 | Built-in rules: `not_null`, `in_set`, `between`, `regex`, `expr`, `unique`, `row_count`; NULL counts as pass except in `not_null` |
| DQ-2 | New team rules are added once with `@register_rule` and become usable from YAML |
| DQ-3 | Rule files are linted in CI without Spark: `python -m twinlake.quality.lint conf/quality/*.yml` |
| DQ-4 | Engine uses only DataFrame APIs (no `cache()`), so it runs on serverless |
| DQ-5 | Every run produces a summary: rows checked, rows quarantined, failures per check |

Status: module built (`spec.py`, `engine.py`, `lint.py`); YAML validation tested; Spark tests written but not yet run against Sail.

## CI/CD and promotion

GitHub Actions is the only path to prod: every pull request runs the full check suite, and a merge to `main` deploys the bundle.

![CI/CD: only green pull requests reach prod](diagrams/04-ci-cd.svg)

A failing check sends the work back to the branch; nobody edits code in the Databricks workspace.

| Trigger | Steps |
| --- | --- |
| Pull request | ruff + sqlfluff · quality-rule lint · Sail compatibility check · `pytest` on in-process Sail |
| Merge to `main` | Same checks → `databricks bundle validate` → `databricks bundle deploy -t prod` |

- Databricks host and token live in GitHub Actions secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.
- `main` is protected: merges only through a pull request with green checks.

## Prerequisites

Development of the first pipeline is blocked on the bronze OLTP DDL; everything else can be prepared in parallel.

### Needed from you

| # | Item | Why it's needed |
| --- | --- | --- |
| 1 | Bronze OLTP DDL (`CREATE TABLE` statements, keys, relationships) | Base for the data model, sample data and first pipeline |
| 2 | Business context: what the business does, main flow (e.g. order → payment → shipment) | To review the DDL and propose missing tables or columns |
| 3 | Business questions and KPIs for the gold layer | Gold tables are designed from these |
| 4 | Sample data volume and history (e.g. 10k customers, 1M orders, 2 years) | Sizes the generator to fit the free-tier quota |
| 5 | How rows change: updated in place, soft deletes, `updated_at` columns | Decides the bronze → silver load pattern |
| 6 | Prod refresh frequency (daily, hourly) | Sets `schedule` in `pipeline.yml` |
| 7 | GitHub repo name | Repo template and CI go there |

### Accounts

- [ ] GitHub account and repository
- [ ] Databricks Free Edition account (LinkedIn verification optional, for higher limits and outbound internet)
- [ ] Databricks token stored as GitHub Actions secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

### Laptop setup (Windows + WSL2)

| Item | Requirement |
| --- | --- |
| RAM | 16 GB recommended, 8 GB minimum; cap WSL memory in `.wslconfig` |
| Disk | About 30 GB free |
| WSL2 | Ubuntu LTS |
| Docker | Docker Desktop (WSL2 backend) or Docker Engine inside WSL |
| Repo location | Inside WSL (`~/projects/...`), not `/mnt/c/...` |
| Editor | VS Code + WSL extension |
| Python | Managed with `uv`, pinned to the Databricks serverless version |
| Tools | `git`, `make`, Databricks CLI, `pre-commit`; no Java needed |

### Git branch workflow

- `main` is what runs in prod; changes arrive only by pull request.
- All work happens on `feature/<name>` branches.
- One local `dev` catalog for now; if more developers join, each branch can get a schema prefix such as `dev.<branch>_silver`.

## Risks, constraints and roadmap

The biggest risk is behavior drift between Sail and Databricks serverless; pinned versions and tests on every merge job contain it.

### Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Sail and Databricks behave differently | Job passes locally, fails in prod | Pin versions; Sail compatibility check in CI; test every MERGE job |
| Delta MERGE support in Sail is newer | Wrong results on upserts | Dedicated tests per merge job; keep dev tables written only by Sail |
| Free-tier quota exceeded | Prod compute paused for the day | Lean pipelines, staggered schedules, backfills on-prem |
| Restricted outbound internet in prod | Cloud cannot pull from sources | Land files into a Unity Catalog volume via CLI/API |
| RustFS is a young project | Storage bugs in dev | Dev data is reproducible from the generator; no MinIO data migration |
| Single developer | Knowledge in one head | This PRD, ADRs and the repo template |

### Constraints (Databricks Free Edition)

- Serverless only; one 2X-Small SQL warehouse.
- Max 5 concurrent job tasks; one active pipeline per type.
- Python and SQL only; no custom storage locations.
- Non-commercial use only; Databricks may delete long-inactive accounts.

### Roadmap

1. **Foundation:** laptop setup, `docker-compose.yml` (RustFS + Sail), `context.py`, `io.py`, `BaseJob`/`SqlJob`, quality tests passing on Sail.
2. **Data model:** DDL review, improved bronze model, silver/gold design from KPIs, sample data generator.
3. **First pipeline:** bronze → silver → gold end to end locally with quality rules.
4. **Orchestration:** `runner.py`, `to_bundle.py`; first deploy to Databricks.
5. **CI/CD:** GitHub Actions on pull request and merge; `main` protected.
6. **Optional:** Dagster UI, serving dashboards, more domains.
