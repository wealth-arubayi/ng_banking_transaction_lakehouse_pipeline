# Databricks notebook source
# DBTITLE 1,07 · Pipeline Success Notification
# MAGIC %md
# MAGIC # 07 · Pipeline Success Notification
# MAGIC Runs as the **final task** in the daily pipeline job (after `05_build_transaction_marts`)
# MAGIC and fires only when every upstream task succeeded. Logs a structured SUCCESS event,
# MAGIC prints a human-readable summary of the latest run, and relies on the job's
# MAGIC `email_notifications.on_success` setting to deliver the email automatically.

# COMMAND ----------

# DBTITLE 1,Config
# MAGIC %run ./00_pipeline_config

# COMMAND ----------

# DBTITLE 1,Gather latest run metrics
from datetime import datetime, timezone

NOTEBOOK = "07_pipeline_success_notification"

log_event(NOTEBOOK, "START", pipeline=PIPELINE_NAME, environment=ENVIRONMENT)

# Pull the latest run's metrics from the processing log so the notification
# includes concrete numbers (rows processed, files ingested, etc.) rather
# than just "pipeline succeeded".
latest_run = spark.sql(
    f"""
    SELECT *
    FROM {TBL_PROCESSING_LOG}
    WHERE PIPELINE_NAME = '{PIPELINE_NAME}'
      AND STATUS = 'success'
    ORDER BY RUN_END_TIMESTAMP DESC
    LIMIT 1
    """
)

run_info = latest_run.collect()
has_run_info = len(run_info) > 0

if has_run_info:
    row = run_info[0].asDict()
    summary_lines = [
        f"Pipeline:            {row.get('PIPELINE_NAME', 'N/A')}",
        f"Run ID:              {row.get('PIPELINE_RUN_ID', 'N/A')}",
        f"Status:              SUCCESS",
        f"Run start:           {row.get('RUN_START_TIMESTAMP', 'N/A')}",
        f"Run end:             {row.get('RUN_END_TIMESTAMP', 'N/A')}",
        f"Records received:    {row.get('RECORDS_RECEIVED', 0):,}",
        f"Valid records:       {row.get('VALID_RECORDS', 0):,}",
        f"Invalid records:     {row.get('INVALID_RECORDS', 0):,}",
        f"Transaction range:  {row.get('MIN_TRANSACTION_DATE', 'N/A')} \u2013 {row.get('MAX_TRANSACTION_DATE', 'N/A')}",
    ]
else:
    summary_lines = [
        f"Pipeline:            {PIPELINE_NAME}",
        f"Environment:         {ENVIRONMENT}",
        f"Status:              All upstream tasks completed successfully.",
        f"Note:                No structured run record found in {TBL_PROCESSING_LOG},",
        f"                     but this notification only fires when all prior tasks passed.",
    ]

# COMMAND ----------

# DBTITLE 1,Print summary and exit
# Print the human-readable summary — this appears in the job run output
# and is included in the on_success email notification body.
print("=" * 60)
print("  PIPELINE RUN SUCCESSFUL")
print("=" * 60)
for line in summary_lines:
    print(f"  {line}")
print("=" * 60)

log_event(
    NOTEBOOK, "SUCCESS",
    pipeline=PIPELINE_NAME,
    environment=ENVIRONMENT,
    has_run_metrics=has_run_info,
)

dbutils.notebook.exit("SUCCESS")