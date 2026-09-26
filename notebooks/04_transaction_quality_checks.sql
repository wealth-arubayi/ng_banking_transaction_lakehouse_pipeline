-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Quality Checks — Profiling (human-readable)
-- MAGIC Ad hoc / dashboard-friendly versions of the same checks that
-- MAGIC `04b_transaction_quality_gate.py` enforces programmatically. Run this one when you
-- MAGIC want to **look at** the numbers (e.g. from a SQL editor or a Lakeview dashboard);
-- MAGIC run 04b when you want the **pipeline to stop** if something's wrong.

-- COMMAND ----------

-- Should always be zero: TRANSACTION_KEY is the MERGE key in notebook 03.
SELECT COUNT(*) AS DUPLICATE_KEY_COUNT FROM (
 SELECT TRANSACTION_KEY FROM ng_banking_lakehouse.transaction_curated.transaction_entries
 GROUP BY TRANSACTION_KEY HAVING COUNT(*)>1
);

-- COMMAND ----------

-- Internal consistency of the three amount-derived fields (see notebook 03's business rules).
SELECT COUNT(*) AS SIGN_MISMATCH_COUNT FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE (DRCR_IND='C' AND TRANSACTION_SIGN<>'+') OR (DRCR_IND='D' AND TRANSACTION_SIGN<>'-');

SELECT COUNT(*) AS NEGATIVE_FLAG_MISMATCH_COUNT FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE IS_NEGATIVE_AMOUNT <> CASE WHEN SOURCE_LCY_AMOUNT<0 THEN TRUE ELSE FALSE END;

SELECT COUNT(*) AS ABS_AMOUNT_MISMATCH_COUNT FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE ABS_LCY_AMOUNT<>ABS(SOURCE_LCY_AMOUNT);

SELECT COUNT(*) AS NET_AMOUNT_MISMATCH_COUNT FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE NET_SIGNED_AMOUNT<>CASE WHEN DRCR_IND='C' THEN ABS(SOURCE_LCY_AMOUNT) WHEN DRCR_IND='D' THEN -ABS(SOURCE_LCY_AMOUNT) ELSE 0 END;

-- COMMAND ----------

-- Classification profile — how much volume falls into each mapped/unmapped bucket.
SELECT TRN_CODE,MODULE,MAP_MODULE,MODULE_CATEGORY,MODULE_DESCRIPTION,TRANSACTION_CATEGORY,CLASSIFICATION_STATUS,COUNT(*) RECORD_COUNT
FROM ng_banking_lakehouse.transaction_curated.transaction_entries
GROUP BY TRN_CODE,MODULE,MAP_MODULE,MODULE_CATEGORY,MODULE_DESCRIPTION,TRANSACTION_CATEGORY,CLASSIFICATION_STATUS
ORDER BY RECORD_COUNT DESC;

-- COMMAND ----------

-- Business-sense check: debit records still classified under the DEPOSIT category are
-- almost certainly a mapping gap worth a manual look (deposits are credit-side by nature).
SELECT TRN_CODE,MODULE,DRCR_IND,MAP_MODULE,MODULE_CATEGORY,MODULE_DESCRIPTION,TRANSACTION_CATEGORY,COUNT(*) RECORD_COUNT
FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE DRCR_IND='D' AND MODULE_CATEGORY='DEPOSIT'
GROUP BY TRN_CODE,MODULE,DRCR_IND,MAP_MODULE,MODULE_CATEGORY,MODULE_DESCRIPTION,TRANSACTION_CATEGORY
ORDER BY RECORD_COUNT DESC;

-- COMMAND ----------

-- Actual source combinations still unmapped — feed straight back into notebook 02's
-- mapping-coverage query and reference/transaction_mapping_seed.csv.
SELECT TRN_CODE,MODULE,PRODUCT,DRCR_IND,COUNT(*) RECORD_COUNT
FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE CLASSIFICATION_STATUS='UNMAPPED'
GROUP BY TRN_CODE,MODULE,PRODUCT,DRCR_IND ORDER BY RECORD_COUNT DESC;

-- COMMAND ----------

-- Reversal candidates found — should only ever be codes explicitly marked
-- IS_REVERSAL_CODE=TRUE in transaction_mapping (e.g. 'REV').
SELECT TRN_CODE,MODULE,MAP_MODULE,DRCR_IND,COUNT(*) RECORD_COUNT,SUM(ABS_LCY_AMOUNT) TOTAL_ABS_AMOUNT
FROM ng_banking_lakehouse.transaction_curated.transaction_entries
WHERE IS_REVERSAL_CANDIDATE
GROUP BY TRN_CODE,MODULE,MAP_MODULE,DRCR_IND ORDER BY RECORD_COUNT DESC;
