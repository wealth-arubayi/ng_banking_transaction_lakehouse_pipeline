# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Pipeline Configuration
# MAGIC Shared constants, environment widgets, and a small structured-logging helper.
# MAGIC Every other notebook in this pipeline starts with:
# MAGIC ```python
# MAGIC %run ./00_pipeline_config
# MAGIC ```
# MAGIC so table names, paths and the pipeline name are defined in exactly one place.

# COMMAND ----------

dbutils.widgets.text("catalog", "ng_banking_lakehouse", "Unity Catalog name")
dbutils.widgets.text("pipeline_name", "ng_banking_transaction_lakehouse_pipeline", "Pipeline name")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

CATALOG = dbutils.widgets.get("catalog")
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
ENVIRONMENT = dbutils.widgets.get("environment")

# COMMAND ----------

# Schemas
RAW_SCHEMA = f"{CATALOG}.transaction_raw"
CURATED_SCHEMA = f"{CATALOG}.transaction_curated"
MART_SCHEMA = f"{CATALOG}.transaction_mart"

# Tables
TBL_RAW_EVENTS = f"{RAW_SCHEMA}.transaction_events"
TBL_MAPPING = f"{CURATED_SCHEMA}.transaction_mapping"
TBL_WATERMARK = f"{CURATED_SCHEMA}.etl_watermark"
TBL_PROCESSING_LOG = f"{CURATED_SCHEMA}.pipeline_processing_log"
TBL_AFFECTED_DATES = f"{CURATED_SCHEMA}.pipeline_affected_dates"
TBL_REJECTED = f"{CURATED_SCHEMA}.transaction_rejected"
TBL_ENTRIES = f"{CURATED_SCHEMA}.transaction_entries"
TBL_DQ_RESULTS = f"{CURATED_SCHEMA}.pipeline_dq_results"

MART_TABLES = {
    "daily_transaction_activity": f"{MART_SCHEMA}.daily_transaction_activity",
    "account_transaction_activity": f"{MART_SCHEMA}.account_transaction_activity",
    "branch_transaction_activity": f"{MART_SCHEMA}.branch_transaction_activity",
    "transaction_code_activity": f"{MART_SCHEMA}.transaction_code_activity",
    "transaction_category_activity": f"{MART_SCHEMA}.transaction_category_activity",
    "aml_exception_activity": f"{MART_SCHEMA}.aml_exception_activity",
}

# Volume paths (Auto Loader source / schema / checkpoint locations)
VOL_ROOT = f"/Volumes/{CATALOG}/transaction_raw/transaction_files"
VOL_SOURCE = f"{VOL_ROOT}/incoming/"
VOL_SCHEMA_LOC = f"{VOL_ROOT}/schema/transaction_events/"
VOL_CHECKPOINT = f"{VOL_ROOT}/checkpoints/transaction_events/"
VOL_REFERENCE = f"{VOL_ROOT}/reference/transaction_mapping_seed.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Structured logging
# MAGIC Every notebook logs a single JSON line per event via `log_event(...)`, tagged with
# MAGIC `PIPELINE_NAME` and `ENVIRONMENT`, so run history can be grepped/queried consistently
# MAGIC whether it's read from driver logs or piped to an external sink (e.g. a Databricks
# MAGIC SQL alert on `pipeline_processing_log`, or a workspace webhook).
# MAGIC
# MAGIC No secrets are ever printed here. Any credential a notebook needs (e.g. a webhook URL
# MAGIC for alerting) is retrieved with `dbutils.secrets.get(scope=..., key=...)` at the point
# MAGIC of use and never assigned to a variable that gets logged or displayed.

# COMMAND ----------

import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(PIPELINE_NAME)


def log_event(notebook: str, event: str, **fields) -> None:
    """Emit one structured JSON log line. Keep values JSON-serialisable (str/int/float/bool)."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline": PIPELINE_NAME,
        "environment": ENVIRONMENT,
        "notebook": notebook,
        "event": event,
        **fields,
    }
    _logger.info(json.dumps(payload, default=str))


def fail_notebook(notebook: str, reason: str, **fields) -> None:
    """Log a structured failure record, then raise so the job/task is marked FAILED
    (rather than silently succeeding with bad data — this is what lets Databricks
    Workflows retries/alerts/downstream-task-skip actually engage)."""
    log_event(notebook, "FAILURE", reason=reason, **fields)
    raise RuntimeError(f"[{notebook}] {reason}: {fields}")
