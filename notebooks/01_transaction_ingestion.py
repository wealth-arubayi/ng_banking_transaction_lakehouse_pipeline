# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Transaction Ingestion (Bronze)
# MAGIC Auto Loader ingests CSV transaction extracts from a Unity Catalog Volume into
# MAGIC `transaction_raw.transaction_events`, tracking schema evolution defensively via
# MAGIC `rescuedDataColumn` rather than failing the stream on an unexpected column.
# MAGIC
# MAGIC **Idempotency:** Auto Loader's own checkpoint (`cloudFiles.schemaLocation` +
# MAGIC the stream checkpoint) guarantees each source file is only ever ingested once,
# MAGIC so re-running this notebook after a partial failure is always safe.

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F

NOTEBOOK = "01_transaction_ingestion"

# COMMAND ----------

from datetime import datetime, timezone

run_start_ts = datetime.now(timezone.utc)
run_start_iso = run_start_ts.strftime("%Y-%m-%d %H:%M:%S")

log_event(NOTEBOOK, "START", source=VOL_SOURCE, target=TBL_RAW_EVENTS)

try:
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaLocation", VOL_SCHEMA_LOC)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("quote", '"')
        .option("escape", '"')
        .load(VOL_SOURCE)
    )
except Exception as exc:
    fail_notebook(NOTEBOOK, "COULD_NOT_OPEN_AUTOLOADER_SOURCE", source=VOL_SOURCE, error=str(exc))

# COMMAND ----------

enriched = (
    raw_stream
    .withColumn("RAW_INGEST_TIMESTAMP", F.current_timestamp())
    .withColumn("SOURCE_FILE_NAME", F.element_at(F.split(F.input_file_name(), "/"), -1))
    .withColumn("SOURCE_FILE_PATH", F.input_file_name())
    .withColumn("SOURCE_SYSTEM", F.lit("BANKING_TRANSACTION_SOURCE"))
    .withColumn("SOURCE_BATCH_ID", F.date_format(F.current_timestamp(), "yyyyMMddHHmmssSSS"))
)

# COMMAND ----------

query = (
    enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", VOL_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TBL_RAW_EVENTS)
)
query.awaitTermination()

# COMMAND ----------

progress = query.lastProgress or {}
rows_ingested = int(progress.get("numInputRows", 0)) if progress else 0

# Files actually committed to transaction_raw in THIS run, by RAW_INGEST_TIMESTAMP
# (precise run-start filter, not a rolling window) — this is also the authoritative
# list for the incoming/ -> processed/ move below, since Auto Loader's own checkpoint
# already guarantees each of these was durably written before we get here.
ingested_files = [
    row["SOURCE_FILE_PATH"]
    for row in spark.sql(
        f"SELECT DISTINCT SOURCE_FILE_PATH FROM {TBL_RAW_EVENTS} "
        f"WHERE RAW_INGEST_TIMESTAMP >= '{run_start_iso}'"
    ).collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move ingested files: incoming/ -> processed/
# MAGIC A move failure here is logged as a WARNING, not a pipeline failure — the data is
# MAGIC already durably committed above (Auto Loader's checkpoint means it will never be
# MAGIC re-ingested even if it's still sitting in incoming/), so this is housekeeping,
# MAGIC not correctness. `06_archive_processed_files.py` sweeps processed/ into a
# MAGIC year/month/day archive structure on a weekly schedule.

# COMMAND ----------

moved, move_failures = 0, []
for file_path in ingested_files:
    file_name = file_path.rstrip("/").split("/")[-1]
    dest_path = f"{VOL_PROCESSED}{file_name}"
    try:
        dbutils.fs.mv(file_path, dest_path)
        moved += 1
    except Exception as exc:
        move_failures.append(file_name)
        log_event(NOTEBOOK, "WARNING", reason="COULD_NOT_MOVE_TO_PROCESSED",
                   file=file_name, error=str(exc))

log_event(
    NOTEBOOK, "SUCCESS",
    rows_ingested_last_batch=rows_ingested,
    files_ingested_this_run=len(ingested_files),
    files_moved_to_processed=moved,
    files_move_failed=len(move_failures),
    target=TBL_RAW_EVENTS,
)

dbutils.notebook.exit("SUCCESS")
