# CLAUDE.md: Twinlake

Project memory for Claude Code. Read `docs/PRD.md` for the full spec; this file holds the rules to follow on every change.

## What this is

Twinlake: a lakehouse framework for a learning/portfolio project. One codebase runs in two places:

- **Dev (on-prem):** laptop, Windows + WSL2, Docker. RustFS (S3) + Sail (Spark Connect) + Delta + Unity Catalog OSS. Dashboards: Trino + Metabase.
- **Prod (cloud):** Databricks Free Edition. Serverless compute, Unity Catalog, Lakeflow Jobs via Asset Bundles.

Only config (`APP_ENV=dev|prod`, `conf/*.yml`) changes between environments. Code must never branch on environment outside `twinlake/`.

## Hard rules

1. **Spark Connect only.** Sail and Databricks serverless are both Spark Connect. Never use RDDs, `sparkContext`, `_jvm`, `cache()`/`persist()`, or `dbutils` outside `twinlake/`.
2. **Table names without catalog** in pipeline code: `silver.sales`. The framework adds `dev.` or `prod.`. Never use raw storage paths (`s3://`, `/dbfs`, `file://`) in pipeline code.
3. **Developers write only business logic:** a `transform(df)` in a `BaseJob` subclass, or one `SELECT` in a `.sql` file with a header comment (`-- target:`, `-- write_mode:`, `-- quality:`).
4. **Write modes:** `append` (bronze only), `overwrite`, `overwrite_partition`, `merge`. Every job must be safe to rerun.
5. **Data quality is YAML only:** one file per table in `conf/quality/`. New rule types go in `twinlake/quality/engine.py` via `@register_rule`.
6. **Orchestration is generated** from `pipelines/<domain>/pipeline.yml`. Never hand-write Airflow, Dagster or `databricks.yml` job code.
7. **Naming:** `snake_case`; tables `<layer>.<domain>_<entity>`; audit columns `_ingested_at`, `_source` on every table.
8. **Python and SQL only** (Databricks Free Edition has no R/Scala).
9. **Commercial use is out of scope** (Free Edition terms).

## Current state

| Area | Status |
|---|---|
| `twinlake/quality/` (spec, engine, lint) | Built. YAML validation tested. Spark tests **pass on Sail** (no engine changes needed); compatibility check: all APIs supported |
| `tests/test_quality.py` | 5/5 pass on in-process Sail (Python 3.12, `pysail==0.7.2`, `pyspark[connect]==4.2.1`, pinned in `pyproject.toml`) |
| `twinlake/context.py`, `io.py`, `base_job.py` | To do |
| `twinlake/runner.py`, `orchestration/to_bundle.py` | To do |
| `docker-compose.yml` (RustFS, Sail, UC OSS; `serving` profile: Trino, Metabase) | To do |
| `.github/workflows/ci.yml`, `.pre-commit-config.yaml` | To do |
| Data model + sample data generator | Blocked: waiting for the user's bronze OLTP DDL |

## Next tasks, in order

1. ~~Create the venv and run the quality tests on Sail; fix anything Sail rejects.~~ Done.
2. `docker-compose.yml` with RustFS, Sail, Unity Catalog OSS; Trino + Metabase under a `serving` profile.
3. `twinlake/context.py` (session by `APP_ENV`, config merge, `table()`, `get_secret()`), `io.py`, `base_job.py` (`BaseJob`, `SqlJob`).
4. `twinlake/runner.py`: topological sort on `depends_on`, retries, `--from`, `--only`.
5. `orchestration/to_bundle.py`: `pipeline.yml` → `databricks.yml`.
6. CI workflow: ruff, sqlfluff, quality lint, Sail compatibility check, pytest; deploy on merge to `main`.
7. When the DDL arrives: review it, propose the improved model, build the sample data generator and the first pipeline.

## Commands

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest -q                                                   # tests on in-process Sail
python -m twinlake.quality.lint conf/quality/*.yml         # lint rule files, no Spark
python -m pysail.examples.spark.compatibility_check pipelines/   # Sail API check
node docs/diagrams/build.mjs docs/diagrams/src docs/diagrams     # rebuild SVGs (needs: npm i -D esbuild)
```

## Environment notes

- Keep the repo inside the WSL filesystem (`~/projects/...`), not `/mnt/c/...`.
- Sail and PySpark are pinned in `pyproject.toml`; rerun `pytest` and the compatibility check before bumping either. Delta MERGE support in Sail is newer, so every merge job needs a test.
- Databricks Free limits: 5 concurrent job tasks, one 2X-Small SQL warehouse, restricted outbound internet, daily compute quota.

## Git

- `main` = prod; protected; changes only by pull request with green CI.
- Work on `feature/<name>` branches.
