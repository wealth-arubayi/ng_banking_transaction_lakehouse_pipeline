# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,08 · Sweep Incoming Files (manual)
# MAGIC %md
# MAGIC # 08 · Sweep Incoming Files (manual, on-demand)
# MAGIC Manual housekeeping utility: moves files that have **already been ingested** by
# MAGIC Auto Loader but are still sitting in `incoming/` — typically because the
# MAGIC `incoming/ -> processed/` move step in notebook 01 logged a WARNING instead
# MAGIC of failing hard.
# MAGIC
# MAGIC This notebook is **deliberately unscheduled** (see `resources/sweep_job.yml`).
# MAGIC Trigger it manually via the Jobs UI or `databricks bundle run` when you notice
# MAGIC files accumulating in `incoming/` after a pipeline run.
# MAGIC
# MAGIC **Safe by design:** it only moves files whose `SOURCE_FILE_NAME` already exists
# MAGIC in `transaction_raw.transaction_events` — files that haven't been ingested yet
# MAGIC are left untouched so Auto Loader can still pick them up on the next run.
# MAGIC
# MAGIC Idempotent: `dbutils.fs.mv` on a file that's already gone is a no-op error
# MAGIC that's caught and skipped.

# COMMAND ----------

# DBTITLE 1,Run shared config
# MAGIC %run ./00_pipeline_config

# COMMAND ----------

# DBTITLE 1,List incoming files and find already-ingested ones
NOTEBOOK = "08_sweep_incoming_files"

log_event(NOTEBOOK, "START", source=VOL_SOURCE, target=VOL_PROCESSED)

# --- List everything currently sitting in incoming/ ---
try:
    incoming_files = dbutils.fs.ls(VOL_SOURCE)
except Exception as exc:
    fail_notebook(NOTEBOOK, "COULD_NOT_LIST_INCOMING", path=VOL_SOURCE, error=str(exc))

# Filter to files only (skip subdirectories like schema/, checkpoints/ if somehow present)
incoming_files = [f for f in incoming_files if not f.isDir()]

if not incoming_files:
    log_event(NOTEBOOK, "SUCCESS", reason="NO_FILES_IN_INCOMING", files_seen=0, files_moved=0)
    dbutils.notebook.exit("SUCCESS — nothing to sweep")

# --- Query the raw table for files that have already been ingested ---
ingested_file_names = set(
    row["SOURCE_FILE_NAME"]
    for row in spark.sql(
        f"SELECT DISTINCT SOURCE_FILE_NAME FROM {TBL_RAW_EVENTS}"
    ).collect()
)

# COMMAND ----------

# DBTITLE 1,Move ingested files to processed/
moved, skipped_not_ingested, skipped_already_moved, failures = 0, 0, 0, []

for f in incoming_files:
    file_name = f.name

    # Only move files that have actually been ingested into the raw table.
    # Files not yet seen by Auto Loader are left in incoming/ so the next
    # pipeline run can still pick them up.
    if file_name not in ingested_file_names:
        skipped_not_ingested += 1
        log_event(NOTEBOOK, "INFO", reason="FILE_NOT_YET_INGESTED", file=file_name)
        continue

    dest_path = f"{VOL_PROCESSED}{file_name}"
    try:
        dbutils.fs.mv(f.path, dest_path)
        moved += 1
    except Exception as exc:
        # The most common non-fatal error here is trying to move a file that
        # was already moved by a prior sweep or by notebook 01 itself — that's
        # fine, just skip it. Anything else is logged for investigation.
        error_str = str(exc)
        if "NoSuchFileException" in error_str or "not found" in error_str.lower():
            skipped_already_moved += 1
        else:
            failures.append(file_name)
            log_event(NOTEBOOK, "WARNING", reason="COULD_NOT_MOVE_TO_PROCESSED",
                      file=file_name, error=error_str)

# COMMAND ----------

# DBTITLE 1,Log summary and exit
log_event(
    NOTEBOOK, "SUCCESS",
    files_seen_in_incoming=len(incoming_files),
    files_moved_to_processed=moved,
    files_skipped_not_ingested=skipped_not_ingested,
    files_skipped_already_moved=skipped_already_moved,
    files_failed=len(failures),
)

if failures:
    print(f"WARNING: {len(failures)} file(s) could not be moved: {failures}")

if skipped_not_ingested:
    print(f"INFO: {skipped_not_ingested} file(s) left in incoming/ — not yet ingested by Auto Loader.")

dbutils.notebook.exit("SUCCESS")