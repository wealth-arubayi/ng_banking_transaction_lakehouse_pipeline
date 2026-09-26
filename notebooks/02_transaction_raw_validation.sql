-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Raw Validation (Bronze profiling)
-- MAGIC Read-only profiling of `transaction_raw.transaction_events`. Nothing here mutates
-- MAGIC data or blocks the pipeline — it exists to answer two questions before every run
-- MAGIC of the transform notebook: *"is the raw feed structurally sound?"* and
-- MAGIC *"does the reference mapping actually cover what showed up today?"*

-- COMMAND ----------

-- Field-completeness check on the fields the transform notebook treats as mandatory.
SELECT COUNT(*) AS TOTAL_RAW_RECORDS,
 SUM(CASE WHEN TRN_REF_NO IS NULL OR TRIM(TRN_REF_NO)='' THEN 1 ELSE 0 END) AS MISSING_TRN_REF_NO,
 SUM(CASE WHEN AC_NO IS NULL OR TRIM(AC_NO)='' THEN 1 ELSE 0 END) AS MISSING_AC_NO,
 SUM(CASE WHEN TRN_DT IS NULL OR TRIM(TRN_DT)='' THEN 1 ELSE 0 END) AS MISSING_TRN_DT,
 SUM(CASE WHEN LCY_AMOUNT IS NULL OR TRIM(LCY_AMOUNT)='' THEN 1 ELSE 0 END) AS MISSING_LCY_AMOUNT,
 SUM(CASE WHEN UPPER(TRIM(DRCR_IND)) NOT IN ('C','D') OR DRCR_IND IS NULL THEN 1 ELSE 0 END) AS INVALID_DRCR_IND,
 SUM(CASE WHEN AC_CCY IS NULL OR TRIM(AC_CCY)='' THEN 1 ELSE 0 END) AS MISSING_CURRENCY,
 SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS RESCUED_COLUMN_HITS
FROM ng_banking_lakehouse.transaction_raw.transaction_events;

-- COMMAND ----------

-- MAGIC %md Use this result to extend the reference mapping with actual source values —
-- MAGIC never guess a mapping ahead of what the source has actually produced.

-- COMMAND ----------

SELECT UPPER(NULLIF(TRIM(TRN_CODE),'')) TRN_CODE,
       UPPER(NULLIF(TRIM(MODULE),'')) MODULE,
       UPPER(NULLIF(TRIM(PRODUCT),'')) PRODUCT,
       UPPER(NULLIF(TRIM(DRCR_IND),'')) DRCR_IND,
       COUNT(*) RECORD_COUNT
FROM ng_banking_lakehouse.transaction_raw.transaction_events
GROUP BY UPPER(NULLIF(TRIM(TRN_CODE),'')),UPPER(NULLIF(TRIM(MODULE),'')),UPPER(NULLIF(TRIM(PRODUCT),'')),UPPER(NULLIF(TRIM(DRCR_IND),''))
ORDER BY RECORD_COUNT DESC;

-- COMMAND ----------

-- MAGIC %md ### Mapping coverage
-- MAGIC For every distinct (TRN_CODE, MODULE) pair seen in the raw feed today, does an
-- MAGIC *active* row in `transaction_mapping` actually match it (by TRN_CODE, by MODULE,
-- MAGIC or by both)? Anything flagged `NO_ACTIVE_MAPPING` below will land as
-- MAGIC `CLASSIFICATION_STATUS = 'UNMAPPED'` in the curated layer — this is the earliest
-- MAGIC point to catch a new product code before it silently falls into "Other/Unmapped".

-- COMMAND ----------

WITH source_combos AS (
  SELECT UPPER(NULLIF(TRIM(TRN_CODE),'')) TRN_CODE,
         UPPER(NULLIF(TRIM(MODULE),'')) MODULE,
         UPPER(NULLIF(TRIM(PRODUCT),'')) PRODUCT,
         COUNT(*) RECORD_COUNT
  FROM ng_banking_lakehouse.transaction_raw.transaction_events
  GROUP BY 1,2,3
),
active_map AS (
  SELECT UPPER(TRN_CODE) TRN_CODE, UPPER(MODULE) MODULE, UPPER(PRODUCT) PRODUCT
  FROM ng_banking_lakehouse.transaction_curated.transaction_mapping
  WHERE ACTIVE_FLAG='Y'
)
SELECT s.TRN_CODE, s.MODULE, s.PRODUCT, s.RECORD_COUNT,
       CASE WHEN EXISTS (
         SELECT 1 FROM active_map m
         WHERE (m.TRN_CODE IS NULL OR m.TRN_CODE = s.TRN_CODE)
           AND (m.MODULE IS NULL OR m.MODULE = s.MODULE)
           AND (m.PRODUCT IS NULL OR m.PRODUCT = s.PRODUCT)
       ) THEN 'MAPPED' ELSE 'NO_ACTIVE_MAPPING' END AS MAPPING_STATUS
FROM source_combos s
ORDER BY MAPPING_STATUS DESC, RECORD_COUNT DESC;
