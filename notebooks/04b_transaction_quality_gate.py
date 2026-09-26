# Databricks notebook source
# MAGIC %md
# MAGIC # 04b · Quality Gate (enforced)
# MAGIC The SQL profiling in `04_transaction_quality_checks.sql` is read-only and never
# MAGIC stops the pipeline on its own — a human has to notice a bad number. This notebook
# MAGIC runs the same critical checks, writes every result to
# MAGIC `transaction_curated.pipeline_dq_results` for trend/alerting, and **raises** (failing
# MAGIC the Databricks Workflow task) if any CRITICAL check is breached — which is what
# MAGIC actually stops notebook 05 from building marts on top of bad data.

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

from datetime import datetime
from uuid import uuid4

from pyspark.sql import functions as F

NOTEBOOK = "04b_transaction_quality_gate"
run_id = str(uuid4())
log_event(NOTEBOOK, "START", run_id=run_id)

entries = spark.table(TBL_ENTRIES)

# COMMAND ----------

# MAGIC %md ### Define checks
# MAGIC Each check is `(name, severity, sql)`. `sql` must return a single row with a single
# MAGIC `RESULT` column — the count/measure being thresholded against zero for CRITICAL
# MAGIC checks, or just observed/logged for WARN checks.

# COMMAND ----------

CHECKS = [
    (
        "DUPLICATE_TRANSACTION_KEY", "CRITICAL",
        f"""SELECT COUNT(*) AS RESULT FROM (
              SELECT TRANSACTION_KEY FROM {TBL_ENTRIES} GROUP BY TRANSACTION_KEY HAVING COUNT(*) > 1)""",
    ),
    (
        "SIGN_MISMATCH", "CRITICAL",
        f"""SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES}
              WHERE (DRCR_IND='C' AND TRANSACTION_SIGN<>'+') OR (DRCR_IND='D' AND TRANSACTION_SIGN<>'-')""",
    ),
    (
        "NEGATIVE_FLAG_MISMATCH", "CRITICAL",
        f"""SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES}
              WHERE IS_NEGATIVE_AMOUNT <> CASE WHEN SOURCE_LCY_AMOUNT < 0 THEN TRUE ELSE FALSE END""",
    ),
    (
        "ABS_AMOUNT_MISMATCH", "CRITICAL",
        f"SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES} WHERE ABS_LCY_AMOUNT <> ABS(SOURCE_LCY_AMOUNT)",
    ),
    (
        "NET_AMOUNT_MISMATCH", "CRITICAL",
        f"""SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES}
              WHERE NET_SIGNED_AMOUNT <> CASE WHEN DRCR_IND='C' THEN ABS(SOURCE_LCY_AMOUNT)
                                               WHEN DRCR_IND='D' THEN -ABS(SOURCE_LCY_AMOUNT) ELSE 0 END""",
    ),
    (
        "REVERSAL_CANDIDATE_WITHOUT_VERIFIED_CODE", "CRITICAL",
        f"""SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES}
              WHERE IS_REVERSAL_CANDIDATE AND NOT IS_NEGATIVE_AMOUNT""",
    ),
    # WARN checks surface things worth a human look without blocking the run.
    (
        "UNMAPPED_VOLUME_SHARE_OVER_5_PERCENT", "WARN",
        f"""SELECT ROUND(100.0 * SUM(CASE WHEN CLASSIFICATION_STATUS='UNMAPPED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS RESULT
              FROM {TBL_ENTRIES}""",
    ),
    (
        "DEBIT_ROWS_CLASSIFIED_AS_DEPOSIT", "WARN",
        f"SELECT COUNT(*) AS RESULT FROM {TBL_ENTRIES} WHERE DRCR_IND='D' AND MODULE_CATEGORY='DEPOSIT'",
    ),
]

WARN_THRESHOLDS = {"UNMAPPED_VOLUME_SHARE_OVER_5_PERCENT": 5.0}

# COMMAND ----------

results = []
critical_failures = []
warnings = []

for name, severity, query in CHECKS:
    value = spark.sql(query).first()["RESULT"]
    value = float(value) if value is not None else 0.0
    passed = True
    if severity == "CRITICAL":
        passed = value == 0
        if not passed:
            critical_failures.append((name, value))
    else:
        threshold = WARN_THRESHOLDS.get(name)
        passed = threshold is None or value <= threshold
        if not passed:
            warnings.append((name, value))
    results.append((run_id, PIPELINE_NAME, name, severity, value, passed, datetime.utcnow()))
    log_event(NOTEBOOK, "CHECK_RESULT", run_id=run_id, check=name, severity=severity, value=value, passed=passed)

# COMMAND ----------

# MAGIC %md ### Persist results (trend line for a Lakeview dashboard / SQL alert)

# COMMAND ----------

results_df = spark.createDataFrame(
    results,
    "PIPELINE_RUN_ID string, PIPELINE_NAME string, CHECK_NAME string, SEVERITY string, "
    "RESULT_VALUE double, PASSED boolean, CHECKED_TIMESTAMP timestamp",
)
results_df.write.mode("append").saveAsTable(TBL_DQ_RESULTS)

# COMMAND ----------

if warnings:
    log_event(NOTEBOOK, "WARNINGS", run_id=run_id, warnings=warnings)

if critical_failures:
    fail_notebook(
        NOTEBOOK, "CRITICAL_DQ_CHECK_FAILED", run_id=run_id,
        failed_checks=critical_failures,
    )

log_event(NOTEBOOK, "SUCCESS", run_id=run_id, checks_run=len(CHECKS), warnings=len(warnings))
dbutils.notebook.exit("SUCCESS")
