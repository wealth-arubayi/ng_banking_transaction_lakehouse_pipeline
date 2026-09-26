# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Incremental Transform (Bronze → Silver)
# MAGIC Applies the fixed business rules below to every row ingested since the last
# MAGIC watermark, classifies it against `transaction_mapping`, and merges valid rows
# MAGIC into `transaction_curated.transaction_entries` (rejects go to `transaction_rejected`).
# MAGIC
# MAGIC **Business rules (do not change without a data-owner sign-off):**
# MAGIC - `TRANSACTION_SIGN` comes only from `DRCR_IND`: C=+, D=-.
# MAGIC - `SOURCE_LCY_AMOUNT` preserves the source amount as-is (before any sign convention).
# MAGIC - `IS_NEGATIVE_AMOUNT` checks only `SOURCE_LCY_AMOUNT < 0`.
# MAGIC - `ABS_LCY_AMOUNT` is the amount magnitude; `NET_SIGNED_AMOUNT` is credit-positive /
# MAGIC   debit-negative, derived from `DRCR_IND` and the absolute amount (not from the sign
# MAGIC   of the source amount — those two are validated as consistent by notebook 04).
# MAGIC - `IS_REVERSAL_CANDIDATE` requires a **negative source amount AND** a verified
# MAGIC   reversal code/module mapping (`IS_REVERSAL_CODE = TRUE` in `transaction_mapping`).
# MAGIC   A negative amount alone is never treated as a reversal.
# MAGIC - Classification precedence: `TRN_CODE+MODULE+PRODUCT` > `TRN_CODE+MODULE` >
# MAGIC   `TRN_CODE` > `MODULE+PRODUCT` > `MODULE`. The highest-scoring active mapping wins;
# MAGIC   ties are broken by `PRIORITY DESC`.
# MAGIC - Actual source TRN_CODE/MODULE/PRODUCT combinations should be profiled (notebook 02)
# MAGIC   before extending `transaction_mapping` — don't pre-guess mappings.
# MAGIC
# MAGIC **Idempotency:** re-running this notebook for an already-processed watermark range
# MAGIC is a no-op (`dbutils.notebook.exit('SUCCESS_NO_NEW_DATA')`); re-running after a
# MAGIC partial failure is safe because the curated MERGE is keyed on `TRANSACTION_KEY` with
# MAGIC a "latest STREAM_UPDATE_TIMESTAMP wins" update rule.

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

from datetime import datetime
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK = "03_transaction_incremental_transform"

# A run is aborted (rather than silently merging garbage) if more than this share of
# the batch fails validation — a sudden spike almost always means an upstream extract
# problem, not a handful of bad rows.
REJECTION_RATE_CIRCUIT_BREAKER = 0.10

run_id = str(uuid4())
run_start = datetime.utcnow()
log_event(NOTEBOOK, "START", run_id=run_id)

# COMMAND ----------

def s(c):
    x = F.trim(F.col(c).cast("string"))
    return F.when(x == "", None).otherwise(x)


def d(c):
    x = s(c)
    return F.coalesce(
        F.to_date(x, "yyyy-MM-dd"), F.to_date(x, "dd-MMM-yy"),
        F.to_date(x, "dd-MMM-yyyy"), F.to_date(x, "yyyyMMdd"), F.to_date(x),
    )


def ts(c):
    x = s(c)
    return F.coalesce(
        F.to_timestamp(x, "dd-MMM-yy hh.mm.ss.SSSSSS a"),
        F.to_timestamp(x, "dd-MMM-yy hh.mm.ss.SSS a"),
        F.to_timestamp(x, "dd-MMM-yy hh.mm.ss a"),
        F.to_timestamp(x, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(x),
    )

# COMMAND ----------

# MAGIC %md ### Pull the incremental slice (everything since the last successful watermark)

# COMMAND ----------

r = spark.table(TBL_RAW_EVENTS)
last_row = (
    spark.table(TBL_WATERMARK)
    .filter(F.col("PIPELINE_NAME") == PIPELINE_NAME)
    .select("LAST_RAW_INGEST_TIMESTAMP")
    .first()
)
last_watermark = last_row[0] if last_row else datetime(1900, 1, 1)

r = r.filter(F.col("RAW_INGEST_TIMESTAMP") > F.lit(last_watermark))
n = r.count()

if n == 0:
    log_event(NOTEBOOK, "SUCCESS_NO_NEW_DATA", run_id=run_id, watermark=str(last_watermark))
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

log_event(NOTEBOOK, "INCREMENTAL_SLICE_LOADED", run_id=run_id, records_received=n, watermark=str(last_watermark))

# COMMAND ----------

# MAGIC %md ### Clean, type-cast, and derive the fixed business columns

# COMMAND ----------

cols = ['STREAM_COMPOSIT_ID','STREAM_TABLE_ID','year_month_id','AFFILIATE_ID','TRN_REF_NO','EVENT','AC_BRANCH','AC_NO','AC_CCY','DRCR_IND','TRN_CODE','AMOUNT_TAG','RELATED_CUSTOMER','RELATED_ACCOUNT','RELATED_REFERENCE','MIS_FLAG','MIS_HEAD','TRN_DATE_KEY','TXN_INIT_DATE','FINANCIAL_CYCLE','PERIOD_CODE','INSTRUMENT_CODE','BANK_CODE','TYPE','CATEGORY','CUST_GL','MODULE','IB','FLG_POSITION_STATUS','GLMIS_UPDATE_FLAG','USER_ID','BATCH_NO','PRINT_STAT','PRODUCT_ACCRUAL','AUTH_ID','PRODUCT','GLMIS_VAL_UPD_FLAG','EXTERNAL_REF_NO','DONT_SHOWIN_STMT','IC_BAL_INCLUSION','AML_EXCEPTION','ORIG_PNL_GL','STMT_DT','VIRTUAL_AC_NO','GRP_REF_NO','PRODUCT_PROCESSOR','RELATED_AC_ENTRY_SR_NO','LOADDATE']
for c in cols:
    if c in r.columns:
        r = r.withColumn(c, s(c))
for c in ['AFFILIATE_ID', 'AC_CCY', 'DRCR_IND', 'TRN_CODE', 'MODULE', 'PRODUCT']:
    r = r.withColumn(c, F.upper(F.col(c)))

r = (
    r.withColumn('YEAR_MONTH_ID', F.col('year_month_id').cast('int'))
     .withColumn('EVENT_SR_NO', F.col('EVENT_SR_NO').cast('int'))
     .withColumn('AC_ENTRY_SR_NO', F.col('AC_ENTRY_SR_NO').cast('int'))
     .withColumn('CURR_NO', F.col('CURR_NO').cast('int'))
     .withColumn('NTRY_SEQ_NO', F.col('NTRY_SEQ_NO').cast('int'))
     .withColumn('FCY_AMOUNT', F.regexp_replace('FCY_AMOUNT', ',', '').cast('decimal(20,3)'))
     .withColumn('EXCH_RATE', F.regexp_replace('EXCH_RATE', ',', '').cast('decimal(18,6)'))
     .withColumn('LCY_AMOUNT', F.regexp_replace('LCY_AMOUNT', ',', '').cast('decimal(20,3)'))
     .withColumn('CLAIM_AMOUNT', F.regexp_replace('CLAIM_AMOUNT', ',', '').cast('decimal(20,3)'))
     .withColumn('TRANSACTION_DATE', d('TRN_DT'))
     .withColumn('VALUE_DATE', d('VALUE_DT'))
     .withColumn('STREAM_UPDATE_TIMESTAMP', ts('STREAM_UPDATE_TIME'))
     .withColumn('SAVE_TIMESTAMP_PARSED', ts('SAVE_TIMESTAMP'))
     .withColumn('AUTH_TIMESTAMP_PARSED', ts('AUTH_TIMESTAMP'))
)
r = r.withColumn('SOURCE_LCY_AMOUNT', F.col('LCY_AMOUNT'))
r = r.withColumn(
    'TRANSACTION_KEY',
    F.sha2(F.concat_ws('||', F.coalesce('TRN_REF_NO', F.lit('')),
                        F.coalesce(F.col('EVENT_SR_NO').cast('string'), F.lit('')),
                        F.coalesce(F.col('AC_ENTRY_SR_NO').cast('string'), F.lit(''))), 256),
)
r = (
    r.withColumn('TRANSACTION_DIRECTION', F.when(F.col('DRCR_IND') == 'C', 'CREDIT').when(F.col('DRCR_IND') == 'D', 'DEBIT').otherwise('UNKNOWN'))
     .withColumn('TRANSACTION_SIGN', F.when(F.col('DRCR_IND') == 'C', '+').when(F.col('DRCR_IND') == 'D', '-').otherwise('?'))
     .withColumn('IS_NEGATIVE_AMOUNT', F.coalesce(F.col('SOURCE_LCY_AMOUNT') < 0, F.lit(False)))
     .withColumn('ABS_LCY_AMOUNT', F.abs('SOURCE_LCY_AMOUNT'))
     .withColumn('NET_SIGNED_AMOUNT', F.when(F.col('DRCR_IND') == 'C', F.abs('SOURCE_LCY_AMOUNT')).when(F.col('DRCR_IND') == 'D', -F.abs('SOURCE_LCY_AMOUNT')).otherwise(F.lit(0).cast('decimal(20,3)')))
)

# COMMAND ----------

# MAGIC %md ### Classify against the reference mapping (highest-precedence active match wins)

# COMMAND ----------

m = (
    spark.table(TBL_MAPPING)
    .filter(F.col('ACTIVE_FLAG') == 'Y')
    .select(
        F.upper('TRN_CODE').alias('M_TRN_CODE'), F.upper('MODULE').alias('M_MODULE'),
        F.upper('PRODUCT').alias('M_PRODUCT'), 'PRIORITY', 'MAP_MODULE', 'MODULE_CATEGORY',
        'MODULE_DESCRIPTION', 'TRANSACTION_CATEGORY', 'TRANSACTION_DESCRIPTION', 'IS_REVERSAL_CODE',
    )
)
cond = (
    (F.col('m.M_TRN_CODE').isNull() | (F.col('m.M_TRN_CODE') == F.col('d.TRN_CODE')))
    & (F.col('m.M_MODULE').isNull() | (F.col('m.M_MODULE') == F.col('d.MODULE')))
    & (F.col('m.M_PRODUCT').isNull() | (F.col('m.M_PRODUCT') == F.col('d.PRODUCT')))
)
c = (
    r.alias('d').join(m.alias('m'), cond, 'left')
     .withColumn(
        'MATCH_SCORE',
        F.when(F.col('m.M_TRN_CODE').isNotNull() & F.col('m.M_MODULE').isNotNull() & F.col('m.M_PRODUCT').isNotNull(), 400)
         .when(F.col('m.M_TRN_CODE').isNotNull() & F.col('m.M_MODULE').isNotNull(), 300)
         .when(F.col('m.M_TRN_CODE').isNotNull(), 200)
         .when(F.col('m.M_MODULE').isNotNull() & F.col('m.M_PRODUCT').isNotNull(), 150)
         .when(F.col('m.M_MODULE').isNotNull(), 100)
         .otherwise(0),
    )
)
w = Window.partitionBy(F.col('d.TRANSACTION_KEY')).orderBy(F.col('MATCH_SCORE').desc(), F.col('m.PRIORITY').desc_nulls_last())
c = (
    c.withColumn('_rn', F.row_number().over(w)).filter('_rn=1')
     .select(
        'd.*',
        F.coalesce('m.MAP_MODULE', F.lit('UNMAPPED')).alias('MAP_MODULE'),
        F.coalesce('m.MODULE_CATEGORY', F.lit('OTHER')).alias('MODULE_CATEGORY'),
        F.coalesce('m.MODULE_DESCRIPTION', F.lit('Other / Unmapped')).alias('MODULE_DESCRIPTION'),
        F.coalesce('m.TRANSACTION_CATEGORY', F.lit('OTHER')).alias('TRANSACTION_CATEGORY'),
        F.coalesce('m.TRANSACTION_DESCRIPTION', F.lit('Other / Unmapped')).alias('TRANSACTION_DESCRIPTION'),
        F.coalesce('m.IS_REVERSAL_CODE', F.lit(False)).alias('IS_REVERSAL_CODE'),
        'MATCH_SCORE',
    )
)
c = c.withColumn(
    'CLASSIFICATION_STATUS',
    F.when(F.col('MATCH_SCORE') >= 400, 'MAPPED_TRN_MODULE_PRODUCT')
     .when(F.col('MATCH_SCORE') >= 300, 'MAPPED_TRN_MODULE')
     .when(F.col('MATCH_SCORE') >= 200, 'MAPPED_TRN_CODE')
     .when(F.col('MATCH_SCORE') >= 150, 'MAPPED_MODULE_PRODUCT')
     .when(F.col('MATCH_SCORE') >= 100, 'MAPPED_MODULE')
     .otherwise('UNMAPPED'),
)
c = c.withColumn('IS_REVERSAL_CANDIDATE', F.col('IS_NEGATIVE_AMOUNT') & (F.col('IS_REVERSAL_CODE') | (F.col('MAP_MODULE') == 'REVERSAL')))

# COMMAND ----------

# MAGIC %md ### Split valid vs. rejected, keep only the latest version of each transaction key

# COMMAND ----------

c = c.withColumn(
    'REJECTION_REASON',
    F.when(F.col('TRN_REF_NO').isNull(), 'MISSING_TRANSACTION_REFERENCE')
     .when(F.col('AC_NO').isNull(), 'MISSING_ACCOUNT')
     .when(F.col('TRANSACTION_DATE').isNull(), 'INVALID_TRANSACTION_DATE')
     .when(F.col('LCY_AMOUNT').isNull(), 'INVALID_TRANSACTION_AMOUNT')
     .when(~F.col('DRCR_IND').isin('C', 'D'), 'INVALID_DEBIT_CREDIT_INDICATOR')
     .when(F.col('AC_CCY').isNull(), 'MISSING_CURRENCY'),
)
c = c.cache()
v = c.filter(F.col('REJECTION_REASON').isNull())
bad = c.filter(F.col('REJECTION_REASON').isNotNull())

rejection_rate = bad.count() / n if n else 0.0
if rejection_rate > REJECTION_RATE_CIRCUIT_BREAKER:
    c.unpersist()
    fail_notebook(
        NOTEBOOK, "REJECTION_RATE_CIRCUIT_BREAKER_TRIPPED",
        run_id=run_id, rejection_rate=round(rejection_rate, 4),
        threshold=REJECTION_RATE_CIRCUIT_BREAKER, records_received=n,
    )

wd = Window.partitionBy('TRANSACTION_KEY').orderBy(F.col('STREAM_UPDATE_TIMESTAMP').desc_nulls_last(), F.col('RAW_INGEST_TIMESTAMP').desc_nulls_last())
v = v.withColumn('_rn', F.row_number().over(wd)).filter('_rn=1').drop('_rn')

# COMMAND ----------

# MAGIC %md ### Merge valid rows into curated, append rejects, record affected dates + watermark

# COMMAND ----------

outcols = ['TRANSACTION_KEY','STREAM_COMPOSIT_ID','STREAM_TABLE_ID','STREAM_UPDATE_TIMESTAMP','YEAR_MONTH_ID','AFFILIATE_ID','TRN_REF_NO','EVENT_SR_NO','EVENT','AC_BRANCH','AC_NO','AC_CCY','DRCR_IND','TRANSACTION_DIRECTION','TRANSACTION_SIGN','TRN_CODE','AMOUNT_TAG','FCY_AMOUNT','EXCH_RATE','SOURCE_LCY_AMOUNT','LCY_AMOUNT','ABS_LCY_AMOUNT','NET_SIGNED_AMOUNT','IS_NEGATIVE_AMOUNT','IS_REVERSAL_CANDIDATE','RELATED_CUSTOMER','RELATED_ACCOUNT','RELATED_REFERENCE','MIS_FLAG','MIS_HEAD','TRN_DATE_KEY','TRN_DT','TRANSACTION_DATE','VALUE_DT','VALUE_DATE','TXN_INIT_DATE','FINANCIAL_CYCLE','PERIOD_CODE','INSTRUMENT_CODE','BANK_CODE','TYPE','CATEGORY','CUST_GL','MODULE','MAP_MODULE','MODULE_CATEGORY','MODULE_DESCRIPTION','CLASSIFICATION_STATUS','AC_ENTRY_SR_NO','IB','FLG_POSITION_STATUS','GLMIS_UPDATE_FLAG','USER_ID','CURR_NO','BATCH_NO','PRINT_STAT','PRODUCT_ACCRUAL','AUTH_ID','PRODUCT','GLMIS_VAL_UPD_FLAG','EXTERNAL_REF_NO','DONT_SHOWIN_STMT','IC_BAL_INCLUSION','AML_EXCEPTION','ORIG_PNL_GL','STMT_DT','NTRY_SEQ_NO','VIRTUAL_AC_NO','CLAIM_AMOUNT','SAVE_TIMESTAMP_PARSED','AUTH_TIMESTAMP_PARSED','GRP_REF_NO','PRODUCT_PROCESSOR','RELATED_AC_ENTRY_SR_NO','LOADDATE','SOURCE_FILE_NAME','SOURCE_FILE_PATH','SOURCE_SYSTEM','RAW_INGEST_TIMESTAMP']
out = v.select(*outcols).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('CREATED_TIMESTAMP', F.current_timestamp())
out.createOrReplaceTempView('valid_batch')

spark.sql(f"""
    MERGE INTO {TBL_ENTRIES} t USING valid_batch s ON t.TRANSACTION_KEY = s.TRANSACTION_KEY
    WHEN MATCHED AND (t.STREAM_UPDATE_TIMESTAMP IS NULL OR s.STREAM_UPDATE_TIMESTAMP > t.STREAM_UPDATE_TIMESTAMP) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

badout = bad.select(
    F.lit(run_id).alias('PIPELINE_RUN_ID'), 'TRANSACTION_KEY', 'TRN_REF_NO', 'AC_NO', 'TRN_CODE', 'MODULE',
    'DRCR_IND', 'LCY_AMOUNT', 'TRANSACTION_DATE', 'REJECTION_REASON', 'SOURCE_FILE_NAME', 'RAW_INGEST_TIMESTAMP',
).withColumn('REJECTED_TIMESTAMP', F.current_timestamp())
if badout.limit(1).count() > 0:
    badout.write.mode('append').saveAsTable(TBL_REJECTED)

a = (
    out.select('TRANSACTION_DATE').where('TRANSACTION_DATE IS NOT NULL').distinct()
       .withColumn('TRANSACTION_MONTH_ID', F.date_format('TRANSACTION_DATE', 'yyyyMM').cast('int'))
       .withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_NAME', F.lit(PIPELINE_NAME))
       .withColumn('CREATED_TIMESTAMP', F.current_timestamp()).withColumn('PROCESSED_FLAG', F.lit(False))
       .withColumn('PROCESSED_TIMESTAMP', F.lit(None).cast('timestamp'))
)
if a.limit(1).count() > 0:
    a.write.mode('append').saveAsTable(TBL_AFFECTED_DATES)

mx = r.agg(F.max('RAW_INGEST_TIMESTAMP')).first()[0]
mn = r.agg(F.min('RAW_INGEST_TIMESTAMP')).first()[0]
spark.sql(
    f"MERGE INTO {TBL_WATERMARK} t USING (SELECT '{PIPELINE_NAME}' PIPELINE_NAME, "
    f"TIMESTAMP('{mx.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}') LAST_RAW_INGEST_TIMESTAMP, "
    f"current_timestamp() LAST_RUN_TIMESTAMP) s ON t.PIPELINE_NAME=s.PIPELINE_NAME "
    f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)

vc = out.count()
bc = badout.count() if badout.limit(1).count() > 0 else 0
b = out.agg(F.min('TRANSACTION_DATE'), F.max('TRANSACTION_DATE')).first()
spark.createDataFrame(
    [(run_id, PIPELINE_NAME, run_start, datetime.utcnow(), mn, mx, n, vc, bc, b[0], b[1], 'SUCCESS')],
    'PIPELINE_RUN_ID string,PIPELINE_NAME string,RUN_START_TIMESTAMP timestamp,RUN_END_TIMESTAMP timestamp,'
    'MIN_RAW_INGEST_TIMESTAMP timestamp,MAX_RAW_INGEST_TIMESTAMP timestamp,RECORDS_RECEIVED long,'
    'VALID_RECORDS long,INVALID_RECORDS long,MIN_TRANSACTION_DATE date,MAX_TRANSACTION_DATE date,STATUS string',
).write.mode('append').saveAsTable(TBL_PROCESSING_LOG)

c.unpersist()

log_event(
    NOTEBOOK, "SUCCESS", run_id=run_id, records_received=n, valid_records=vc,
    invalid_records=bc, rejection_rate=round(rejection_rate, 4),
)
dbutils.notebook.exit("SUCCESS")
