# Workflow: ng_banking_transaction_lakehouse_pipeline

Two ways to run this:

1. **Databricks Asset Bundle (recommended):** `databricks bundle deploy -t dev` then
   `databricks bundle run ng_banking_transaction_lakehouse_pipeline -t dev` creates/updates
   the job below from `databricks.yml` + `resources/pipeline_job.yml`.
2. **Manual Workflow UI:** create a job with the five tasks and dependencies described here.

Every notebook (except `00_pipeline_config`, which is `%run` rather than a task) takes a
`catalog` base parameter, so the same job definition can run against separate `dev`/`staging`/`prod`
Unity Catalog catalogs without editing notebook code — as configured today, all three
`databricks.yml` targets happen to point at the one catalog that's actually been provisioned
(`ng_banking_lakehouse`); the per-target override is there for whenever a second catalog exists.

**One-time setup, not a job task:** `notebooks/00b_setup_volume_structure.py` creates the
Volume's `incoming/`, `processed/`, `archive/`, `schema/`, and `checkpoints/` folders. Run it
once per catalog (idempotent to re-run), after `sql/01_create_tables.sql` and before the first
pipeline run.

## Task dependency

```
01_ingest_transactions -> 02_validate_raw_transactions -> 03_transform_transactions
    -> 04_quality_gate -> 05_build_transaction_marts
```

`04_quality_gate` is an **enforced** stop: if any CRITICAL check in
`04b_transaction_quality_gate.py` fails, the task (and therefore the job) fails, and
`05_build_transaction_marts` does not run. `04_transaction_quality_checks.sql` is a
separate, read-only profiling notebook for ad hoc / dashboard use — it is not on the
critical path and doesn't need to be a job task.

## Notebook mapping

- `00_ingest_setup` = `notebooks/00_pipeline_config.py` (imported via `%run`, not a task)
- `01_ingest_transactions` = `notebooks/01_transaction_ingestion.py`
- `02_validate_raw_transactions` = `notebooks/02_transaction_raw_validation.sql`
- `03_transform_transactions` = `notebooks/03_transaction_incremental_transform.py`
- `04_quality_gate` = `notebooks/04b_transaction_quality_gate.py`
- `05_build_transaction_marts` = `notebooks/05_transaction_mart.py`

## Operational notes

- **Schedule:** daily after source file arrival, 02:00 Africa/Lagos (see `resources/pipeline_job.yml`).
- **Retries:** ingestion and transform get automatic retries (transient cluster/IO issues);
  the quality gate does not — a CRITICAL failure means the *data* is suspect, and retrying
  the same data will fail the same way. Fix the mapping/source issue and re-run manually.
- Do not advance the watermark until curated and rejected writes succeed (enforced inside
  `03_transaction_incremental_transform.py` itself, not by the orchestrator).
- Do not mark affected dates processed until all marts succeed (enforced inside
  `05_transaction_mart.py`).
- Failure notifications go to the email configured in the `notification_email` bundle
  variable — set per target in `databricks.yml`.

## Weekly archive job (separate from the job above)

`resources/archive_job.yml` defines a second, independent job —
`ng_banking_transaction_archive` — running `notebooks/06_archive_processed_files.py` on its
own schedule (Sundays 03:00 Africa/Lagos). It sweeps `transaction_files/processed/` into
`transaction_files/archive/YYYY/MM/DD/`, dated by each file's own timestamp rather than the
day the archive job happens to run. It's deployed by the same `databricks bundle deploy`
command (picked up automatically via `databricks.yml`'s `include: resources/*.yml`) but is
deliberately not a task in the daily job — archiving isn't on the critical path for curated
data or the marts, so it shouldn't be able to block or be blocked by them.
