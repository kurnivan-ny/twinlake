# Twinlake

> One codebase, two lakehouses: build on-prem, run on Databricks.

One codebase, two runtimes: build and test on a laptop stack (RustFS, Sail, Delta), run in production on Databricks Free Edition. Only configuration changes between the two.

![Architecture](docs/diagrams/01-architecture.svg)

## Why

Every developer writes pipelines the same way. Business logic lives in a `transform()` method or a single SQL file; reading, writing, data quality, orchestration and environment handling live in `twinlake/`.

## Highlights

- **Dev/prod parity** over Spark Connect: Sail locally, Databricks serverless in prod.
- **YAML data quality** with row-level quarantine and job-failing dataset checks.
- **Generated orchestration** from one `pipeline.yml` per pipeline.
- **CI/CD** with GitHub Actions and Databricks Asset Bundles.

![Data flow](docs/diagrams/02-data-flow.svg)

## Quick start

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest -q
```

## Docs

- [Product requirements (PRD)](docs/PRD.md)
- [Portfolio notes](docs/PORTFOLIO.md)

## Status

Work in progress: the data quality module is built; the rest of the framework, the local stack and the first pipeline are next. See `CLAUDE.md` for the task list.
