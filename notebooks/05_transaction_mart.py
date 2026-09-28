# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 · Transaction Marts (Gold)
# MAGIC Rebuilds each mart's slice for exactly the dates/months touched by this run
# MAGIC (`pipeline_affected_dates`), never a full-table recompute — so the marts stay cheap
# MAGIC to refresh no matter how much history has accumulated.
# MAGIC
# MAGIC **Idempotency:** `write_slice()` deletes the affected slice before appending, so
# MAGIC re-running this notebook for the same affected dates is always safe.
# MAGIC
# MAGIC | # | Mart | Grain | Answers |
# MAGIC |---|------|-------|---------|
# MAGIC | 1 | `daily_transaction_activity` | date × branch × currency | "How is today tracking vs. usual volume/value?" |
# MAGIC | 2 | `account_transaction_activity` | account × month | "Which accounts are most/least active this month?" |
# MAGIC | 3 | `branch_transaction_activity` | date × branch | "Which branches are driving volume?" |
# MAGIC | 4 | `transaction_code_activity` | date × TRN_CODE | "Which products/channels are moving?" |
# MAGIC | 5 | `transaction_category_activity` | date × business category | "Retail vs. treasury vs. trade finance mix?" |
# MAGIC | 6 | `aml_exception_activity` | date × branch × currency | "Where are AML-flagged / high-value transactions concentrated?" |

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F

NOTEBOOK = "05_transaction_mart"

# A transaction above this LCY value is treated as "high value" for monitoring purposes
# even when it carries no AML_EXCEPTION flag from source — separate from, and in addition
# to, whatever threshold the AML system itself applies upstream.
HIGH_VALUE_THRESHOLD_LCY = 5_000_000

log_event(NOTEBOOK, "START")

affected = spark.table(TBL_AFFECTED_DATES).filter(
    (F.col('PIPELINE_NAME') == PIPELINE_NAME) & (~F.col('PROCESSED_FLAG'))
)
dates = [row[0] for row in affected.select('TRANSACTION_DATE').distinct().collect() if row[0]]
months = [row[0] for row in affected.select('TRANSACTION_MONTH_ID').distinct().collect() if row[0]]
if not dates:
    log_event(NOTEBOOK, "SUCCESS_NO_AFFECTED_DATES")
    dbutils.notebook.exit('SUCCESS_NO_AFFECTED_DATES')

date_sql = ','.join("DATE('" + x.isoformat() + "')" for x in dates)
month_sql = ','.join(str(int(x)) for x in months)
base = spark.table(TBL_ENTRIES)
df = base.filter(F.col('TRANSACTION_DATE').isin(dates))
month_df = base.filter(F.col('YEAR_MONTH_ID').isin(months))

credit_amount = F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', F.col('ABS_LCY_AMOUNT')).otherwise(F.lit(0))
debit_amount = F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', F.col('ABS_LCY_AMOUNT')).otherwise(F.lit(0))
net_amount = (
    F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', F.col('ABS_LCY_AMOUNT'))
     .when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', -F.col('ABS_LCY_AMOUNT'))
     .otherwise(F.lit(0))
)
negative_value = F.when(F.col('IS_NEGATIVE_AMOUNT'), F.abs(F.col('SOURCE_LCY_AMOUNT'))).otherwise(F.lit(0))
reversal_value = F.when(F.col('IS_REVERSAL_CANDIDATE'), F.abs(F.col('SOURCE_LCY_AMOUNT'))).otherwise(F.lit(0))


def write_slice(frame, table, predicate):
    spark.sql(f'DELETE FROM {table} WHERE {predicate}')
    frame.write.mode('append').saveAsTable(table)


# COMMAND ----------

# 1. Daily activity
daily = (
    df.groupBy('TRANSACTION_DATE', 'AFFILIATE_ID', 'AC_BRANCH', 'AC_CCY')
      .agg(
          F.count('*').alias('TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_KEY').alias('UNIQUE_TRANSACTION_COUNT'),
          F.countDistinct('AC_NO').alias('UNIQUE_ACCOUNT_COUNT'),
          F.countDistinct('RELATED_CUSTOMER').alias('UNIQUE_CUSTOMER_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', 1).otherwise(0)).alias('CREDIT_COUNT'),
          F.sum(credit_amount).alias('TOTAL_CREDIT_AMOUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', 1).otherwise(0)).alias('DEBIT_COUNT'),
          F.sum(debit_amount).alias('TOTAL_DEBIT_AMOUNT'),
          F.sum(net_amount).alias('NET_TRANSACTION_AMOUNT'),
          F.avg('ABS_LCY_AMOUNT').alias('AVERAGE_TRANSACTION_AMOUNT'),
          F.min('ABS_LCY_AMOUNT').alias('MIN_TRANSACTION_AMOUNT'),
          F.max('ABS_LCY_AMOUNT').alias('MAX_TRANSACTION_AMOUNT'),
          F.countDistinct('TRN_CODE').alias('UNIQUE_TRANSACTION_CODES'),
          F.countDistinct('PRODUCT').alias('UNIQUE_PRODUCTS'),
          F.countDistinct('TRANSACTION_CATEGORY').alias('UNIQUE_TRANSACTION_CATEGORIES'),
          F.sum(F.when(F.col('IS_NEGATIVE_AMOUNT'), 1).otherwise(0)).alias('NEGATIVE_AMOUNT_COUNT'),
          F.sum(negative_value).alias('NEGATIVE_AMOUNT_VALUE'),
          F.sum(F.when(F.col('IS_REVERSAL_CANDIDATE'), 1).otherwise(0)).alias('REVERSAL_CANDIDATE_COUNT'),
          F.sum(reversal_value).alias('REVERSAL_CANDIDATE_AMOUNT'),
          F.min('STREAM_UPDATE_TIMESTAMP').alias('FIRST_TRANSACTION_TIMESTAMP'),
          F.max('STREAM_UPDATE_TIMESTAMP').alias('LAST_TRANSACTION_TIMESTAMP'),
      ).withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(daily, MART_TABLES['daily_transaction_activity'], f'TRANSACTION_DATE IN ({date_sql})')

# COMMAND ----------

# 2. Account monthly activity
account = (
    month_df.groupBy('AC_NO', 'RELATED_CUSTOMER', 'AFFILIATE_ID', 'AC_CCY', 'YEAR_MONTH_ID')
      .agg(
          F.count('*').alias('TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_KEY').alias('UNIQUE_TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_DATE').alias('ACTIVE_TRANSACTION_DAYS'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', 1).otherwise(0)).alias('CREDIT_COUNT'),
          F.sum(credit_amount).alias('TOTAL_CREDIT_AMOUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', 1).otherwise(0)).alias('DEBIT_COUNT'),
          F.sum(debit_amount).alias('TOTAL_DEBIT_AMOUNT'),
          F.sum(net_amount).alias('NET_TRANSACTION_AMOUNT'),
          F.avg('ABS_LCY_AMOUNT').alias('AVERAGE_TRANSACTION_AMOUNT'),
          F.min('ABS_LCY_AMOUNT').alias('MIN_TRANSACTION_AMOUNT'),
          F.max('ABS_LCY_AMOUNT').alias('MAX_TRANSACTION_AMOUNT'),
          F.min('TRANSACTION_DATE').alias('FIRST_TRANSACTION_DATE'),
          F.max('TRANSACTION_DATE').alias('LAST_TRANSACTION_DATE'),
          F.countDistinct('AC_BRANCH').alias('UNIQUE_BRANCHES_USED'),
          F.countDistinct('TRN_CODE').alias('UNIQUE_TRANSACTION_CODES'),
          F.countDistinct('PRODUCT').alias('UNIQUE_PRODUCTS'),
          F.countDistinct('TRANSACTION_CATEGORY').alias('UNIQUE_TRANSACTION_CATEGORIES'),
          F.sum(F.when(F.col('IS_REVERSAL_CANDIDATE'), 1).otherwise(0)).alias('REVERSAL_CANDIDATE_COUNT'),
          F.sum(reversal_value).alias('REVERSAL_CANDIDATE_AMOUNT'),
      ).withColumnRenamed('YEAR_MONTH_ID', 'TRANSACTION_MONTH_ID')
       .withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(account, MART_TABLES['account_transaction_activity'], f'TRANSACTION_MONTH_ID IN ({month_sql})')

# COMMAND ----------

# 3. Branch daily activity
branch = (
    df.groupBy('TRANSACTION_DATE', 'AFFILIATE_ID', 'AC_BRANCH')
      .agg(
          F.count('*').alias('TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_KEY').alias('UNIQUE_TRANSACTION_COUNT'),
          F.countDistinct('AC_NO').alias('UNIQUE_ACCOUNT_COUNT'),
          F.countDistinct('RELATED_CUSTOMER').alias('UNIQUE_CUSTOMER_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', 1).otherwise(0)).alias('CREDIT_COUNT'),
          F.sum(credit_amount).alias('TOTAL_CREDIT_AMOUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', 1).otherwise(0)).alias('DEBIT_COUNT'),
          F.sum(debit_amount).alias('TOTAL_DEBIT_AMOUNT'),
          F.sum(net_amount).alias('NET_TRANSACTION_AMOUNT'),
          F.avg('ABS_LCY_AMOUNT').alias('AVERAGE_TRANSACTION_AMOUNT'),
          F.max('ABS_LCY_AMOUNT').alias('MAX_TRANSACTION_AMOUNT'),
          F.countDistinct('PRODUCT').alias('UNIQUE_PRODUCTS'),
          F.countDistinct('TRN_CODE').alias('UNIQUE_TRANSACTION_CODES'),
          F.countDistinct('TRANSACTION_CATEGORY').alias('UNIQUE_TRANSACTION_CATEGORIES'),
          F.sum(F.when(F.col('IS_REVERSAL_CANDIDATE'), 1).otherwise(0)).alias('REVERSAL_CANDIDATE_COUNT'),
          F.sum(reversal_value).alias('REVERSAL_CANDIDATE_AMOUNT'),
      ).withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(branch, MART_TABLES['branch_transaction_activity'], f'TRANSACTION_DATE IN ({date_sql})')

# COMMAND ----------

# 4. Transaction-code activity
code = (
    df.groupBy('TRANSACTION_DATE', 'TRN_CODE', 'TRANSACTION_CATEGORY', 'MODULE', 'MAP_MODULE', 'AC_CCY')
      .agg(
          F.count('*').alias('TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_KEY').alias('UNIQUE_TRANSACTION_COUNT'),
          F.countDistinct('AC_NO').alias('UNIQUE_ACCOUNT_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', 1).otherwise(0)).alias('CREDIT_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', 1).otherwise(0)).alias('DEBIT_COUNT'),
          F.sum(credit_amount).alias('TOTAL_CREDIT_AMOUNT'),
          F.sum(debit_amount).alias('TOTAL_DEBIT_AMOUNT'),
          F.sum(net_amount).alias('NET_TRANSACTION_AMOUNT'),
          F.avg('ABS_LCY_AMOUNT').alias('AVERAGE_TRANSACTION_AMOUNT'),
          F.min('ABS_LCY_AMOUNT').alias('MIN_TRANSACTION_AMOUNT'),
          F.max('ABS_LCY_AMOUNT').alias('MAX_TRANSACTION_AMOUNT'),
          F.sum(F.when(F.col('IS_NEGATIVE_AMOUNT'), 1).otherwise(0)).alias('NEGATIVE_AMOUNT_COUNT'),
          F.sum(negative_value).alias('NEGATIVE_AMOUNT_VALUE'),
          F.sum(F.when(F.col('IS_REVERSAL_CANDIDATE'), 1).otherwise(0)).alias('REVERSAL_CANDIDATE_COUNT'),
          F.sum(reversal_value).alias('REVERSAL_CANDIDATE_AMOUNT'),
      ).withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(code, MART_TABLES['transaction_code_activity'], f'TRANSACTION_DATE IN ({date_sql})')

# COMMAND ----------

# 5. Transaction-category activity
category = (
    df.groupBy('TRANSACTION_DATE', 'TRANSACTION_CATEGORY', 'AFFILIATE_ID', 'AC_CCY')
      .agg(
          F.count('*').alias('TRANSACTION_COUNT'),
          F.countDistinct('TRANSACTION_KEY').alias('UNIQUE_TRANSACTION_COUNT'),
          F.countDistinct('AC_NO').alias('UNIQUE_ACCOUNT_COUNT'),
          F.countDistinct('AC_BRANCH').alias('UNIQUE_BRANCH_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'CREDIT', 1).otherwise(0)).alias('CREDIT_COUNT'),
          F.sum(F.when(F.col('TRANSACTION_DIRECTION') == 'DEBIT', 1).otherwise(0)).alias('DEBIT_COUNT'),
          F.sum(credit_amount).alias('TOTAL_CREDIT_AMOUNT'),
          F.sum(debit_amount).alias('TOTAL_DEBIT_AMOUNT'),
          F.sum(net_amount).alias('NET_TRANSACTION_AMOUNT'),
          F.avg('ABS_LCY_AMOUNT').alias('AVERAGE_TRANSACTION_AMOUNT'),
          F.sum(F.when(F.col('IS_NEGATIVE_AMOUNT'), 1).otherwise(0)).alias('NEGATIVE_AMOUNT_COUNT'),
          F.sum(negative_value).alias('NEGATIVE_AMOUNT_VALUE'),
          F.sum(F.when(F.col('IS_REVERSAL_CANDIDATE'), 1).otherwise(0)).alias('REVERSAL_CANDIDATE_COUNT'),
          F.sum(reversal_value).alias('REVERSAL_CANDIDATE_AMOUNT'),
      ).withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(category, MART_TABLES['transaction_category_activity'], f'TRANSACTION_DATE IN ({date_sql})')

# COMMAND ----------

# 6. AML / high-value exception monitoring — NEW.
# Flags both source-provided AML_EXCEPTION='Y' rows and any transaction over
# HIGH_VALUE_THRESHOLD_LCY, so compliance/ops has one place to see concentration
# by date, branch and currency without querying the full curated table.
is_aml_flagged = F.col('AML_EXCEPTION') == 'Y'
is_high_value = F.col('ABS_LCY_AMOUNT') > F.lit(HIGH_VALUE_THRESHOLD_LCY)
exception_rows = df.filter(is_aml_flagged | is_high_value)

aml = (
    exception_rows.groupBy('TRANSACTION_DATE', 'AFFILIATE_ID', 'AC_BRANCH', 'AC_CCY')
      .agg(
          F.count('*').alias('EXCEPTION_TRANSACTION_COUNT'),
          F.sum(F.when(is_aml_flagged, 1).otherwise(0)).alias('AML_FLAGGED_COUNT'),
          F.sum(F.when(is_high_value, 1).otherwise(0)).alias('HIGH_VALUE_COUNT'),
          F.sum('ABS_LCY_AMOUNT').alias('TOTAL_EXCEPTION_AMOUNT'),
          F.max('ABS_LCY_AMOUNT').alias('MAX_EXCEPTION_AMOUNT'),
          F.countDistinct('AC_NO').alias('UNIQUE_ACCOUNTS_FLAGGED'),
          F.countDistinct('TRN_CODE').alias('UNIQUE_TRANSACTION_CODES'),
      ).withColumn('HIGH_VALUE_THRESHOLD_LCY', F.lit(HIGH_VALUE_THRESHOLD_LCY))
       .withColumn('MART_LOAD_TIMESTAMP', F.current_timestamp())
)
write_slice(aml, MART_TABLES['aml_exception_activity'], f'TRANSACTION_DATE IN ({date_sql})')

# COMMAND ----------

spark.sql(
    f"UPDATE {TBL_AFFECTED_DATES} SET PROCESSED_FLAG=TRUE, PROCESSED_TIMESTAMP=current_timestamp() "
    f"WHERE PIPELINE_NAME='{PIPELINE_NAME}' AND PROCESSED_FLAG=FALSE AND TRANSACTION_DATE IN ({date_sql})"
)

log_event(NOTEBOOK, "SUCCESS", affected_dates=len(dates), affected_months=len(months))
print('Incremental transaction marts completed successfully.')
dbutils.notebook.exit("SUCCESS")