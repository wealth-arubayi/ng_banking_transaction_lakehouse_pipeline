# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Archive Processed Files (weekly)
# MAGIC Sweeps `processed/` into `archive/YYYY/MM/week_NN/`, run on its own **weekly** schedule
# MAGIC (see `resources/archive_job.yml`) — independent of the daily ingest -> mart job, since
# MAGIC archiving is housekeeping, not part of the data's critical path.
# MAGIC
# MAGIC Each file is dated by **its own last-modified timestamp in `processed/`** (i.e. roughly
# MAGIC when it was originally ingested), not by today's date — so a week's worth of files
# MAGIC accumulated in `processed/` still land in the correct weekly folder rather than all
# MAGIC piling into one "archive run day" bucket.
# MAGIC
# MAGIC The week number (`week_01` \u2013 `week_05`) is the week **within the file's month**
# MAGIC (day 1\u20137 = week 1, day 8\u201314 = week 2, etc.), so files group naturally by the
# MAGIC calendar week they were ingested.
# MAGIC
# MAGIC Idempotent: `dbutils.fs.mv` only ever moves a file that's still sitting in `processed/`,
# MAGIC and dated folders are created on demand — safe to re-run if a previous run partially failed.

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

from datetime import datetime, timezone

NOTEBOOK = "06_archive_processed_files"

log_event(NOTEBOOK, "START", source=VOL_PROCESSED, target=VOL_ARCHIVE_ROOT)

try:
    files = dbutils.fs.ls(VOL_PROCESSED)
except Exception as exc:
    fail_notebook(NOTEBOOK, "COULD_NOT_LIST_PROCESSED", path=VOL_PROCESSED, error=str(exc))

files = [f for f in files if not f.isDir()]

# COMMAND ----------

archived, skipped, failures = 0, 0, []

for f in files:
    try:
        # modificationTime is epoch millis; this is when the file was written into
        # processed/ by 01_transaction_ingestion.py, i.e. its true processing date.
        file_dt = datetime.fromtimestamp(f.modificationTime / 1000, tz=timezone.utc)
        year, month, day = file_dt.strftime("%Y"), file_dt.strftime("%m"), file_dt.day
        # Week within the month: day 1-7 = week 1, day 8-14 = week 2, etc.
        week_of_month = (day - 1) // 7 + 1
        dest_dir = f"{VOL_ARCHIVE_ROOT}{year}/{month}/week_{week_of_month:02d}/"
        dest_path = f"{dest_dir}{f.name}"

        dbutils.fs.mkdirs(dest_dir)  # no-op if it already exists
        dbutils.fs.mv(f.path, dest_path)
        archived += 1
    except Exception as exc:
        failures.append(f.name)
        log_event(NOTEBOOK, "WARNING", reason="COULD_NOT_ARCHIVE_FILE",
                   file=f.name, error=str(exc))

# COMMAND ----------

log_event(
    NOTEBOOK, "SUCCESS",
    files_seen_in_processed=len(files),
    files_archived=archived,
    files_failed=len(failures),
)

if failures:
    # Housekeeping failures shouldn't page anyone at 2am, but they should be visible
    # in the run history rather than silently swallowed.
    print(f"WARNING: {len(failures)} file(s) could not be archived: {failures}")

dbutils.notebook.exit("SUCCESS")