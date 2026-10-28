# Portfolio notes

How to make this project count on a CV. Built is worth more than designed.

## Must have before sharing

- [ ] One pipeline running end to end: bronze → silver → gold, deployed to Databricks, with a Metabase dashboard.
- [ ] README with the architecture diagram, one-command local start, dashboard screenshots.
- [ ] ADRs for the main decisions, including why not Airflow and why not dbt.

## Strong additions

- [ ] Incremental loads using `updated_at`.
- [ ] SCD Type 2 dimension in silver or gold.
- [ ] Run log table: pipeline, task, rows in/out, rows quarantined, duration; plus a Metabase pipeline-health page.

## Numbers to measure

- Local run time on Sail vs JVM Spark for the same pipeline.
- Number of tests and coverage.
- Share of bad rows caught by quality rules in the sample data.
- Time from merge to prod deploy.

## Draft CV bullets (fill in real numbers)

- Designed and built a lakehouse framework where the same PySpark/SQL code runs on an on-prem stack (Sail, RustFS, Delta) and Databricks, with only configuration changing between environments.
- Built a YAML-driven data quality engine that quarantines failing rows and fails jobs on dataset-level errors; caught X% invalid rows in test data.
- Automated testing and deployment with GitHub Actions and Databricks Asset Bundles; `main` deploys only after lint, compatibility checks and N tests pass.
