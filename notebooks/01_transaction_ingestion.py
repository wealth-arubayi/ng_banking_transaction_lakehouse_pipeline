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
files_processed = spark.sql(
    f"SELECT COUNT(DISTINCT SOURCE_FILE_NAME) AS n FROM {TBL_RAW_EVENTS} "
    f"WHERE RAW_INGEST_TIMESTAMP >= current_timestamp() - INTERVAL 1 HOURS"
).first()["n"]

log_event(
    NOTEBOOK, "SUCCESS",
    rows_ingested_last_batch=rows_ingested,
    recent_files_seen=files_processed,
    target=TBL_RAW_EVENTS,
)

dbutils.notebook.exit("SUCCESS")
