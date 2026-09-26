# Data Dictionary

## `transaction_curated.transaction_entries` — key derived columns

Everything not listed here is a cleaned/typed pass-through of the source field of the
same name (see `sql/01_create_tables.sql` for the full column list).

| Column | Type | Derivation |
|---|---|---|
| `TRANSACTION_KEY` | STRING | `sha2(TRN_REF_NO \|\| EVENT_SR_NO \|\| AC_ENTRY_SR_NO, 256)` — the MERGE/dedup key |
| `TRANSACTION_DIRECTION` | STRING | `CREDIT` / `DEBIT` / `UNKNOWN`, from `DRCR_IND` |
| `TRANSACTION_SIGN` | STRING | `+` for credit, `-` for debit, `?` otherwise |
| `SOURCE_LCY_AMOUNT` | DECIMAL | The source `LCY_AMOUNT`, preserved before any sign convention is applied |
| `ABS_LCY_AMOUNT` | DECIMAL | `ABS(SOURCE_LCY_AMOUNT)` |
| `NET_SIGNED_AMOUNT` | DECIMAL | Credit-positive / debit-negative, from `DRCR_IND` + `ABS_LCY_AMOUNT` (independent of the source amount's own sign) |
| `IS_NEGATIVE_AMOUNT` | BOOLEAN | `SOURCE_LCY_AMOUNT < 0` — nothing else |
| `MAP_MODULE` / `MODULE_CATEGORY` / `MODULE_DESCRIPTION` / `TRANSACTION_CATEGORY` / `TRANSACTION_DESCRIPTION` | STRING | Looked up from `transaction_mapping` by best-matching active rule |
| `CLASSIFICATION_STATUS` | STRING | Which precedence tier matched: `MAPPED_TRN_MODULE_PRODUCT` > `MAPPED_TRN_MODULE` > `MAPPED_TRN_CODE` > `MAPPED_MODULE_PRODUCT` > `MAPPED_MODULE` > `UNMAPPED` |
| `IS_REVERSAL_CANDIDATE` | BOOLEAN | `IS_NEGATIVE_AMOUNT AND (verified reversal code OR MAP_MODULE = 'REVERSAL')` — never negative-amount alone |
| `PIPELINE_RUN_ID` | STRING | UUID of the `03_transaction_incremental_transform` run that wrote/last-updated this row |

## `transaction_mapping` (reference)

| Column | Meaning |
|---|---|
| `TRN_CODE` / `MODULE` / `PRODUCT` | Any of the three may be `NULL` — a rule matches on whichever columns are populated. `NULL` = wildcard. |
| `PRIORITY` | Tiebreaker when two active rules score equally (higher wins) |
| `IS_REVERSAL_CODE` | Must be `TRUE` for a row to ever produce `IS_REVERSAL_CANDIDATE = TRUE` downstream |
| `ACTIVE_FLAG` | Inactive (`'N'`) rows are ignored by the classifier entirely |

## Gold marts

| Mart | Grain | Rebuilt for |
|---|---|---|
| `daily_transaction_activity` | date × branch × currency | dates touched this run |
| `account_transaction_activity` | account × month | months touched this run |
| `branch_transaction_activity` | date × branch | dates touched this run |
| `transaction_code_activity` | date × TRN_CODE × category | dates touched this run |
| `transaction_category_activity` | date × business category | dates touched this run |
| `aml_exception_activity` | date × branch × currency | dates touched this run — rows where `AML_EXCEPTION='Y'` OR `ABS_LCY_AMOUNT` exceeds `HIGH_VALUE_THRESHOLD_LCY` |

## `pipeline_dq_results`

One row per check per run, written by `04b_transaction_quality_gate.py`. `SEVERITY='CRITICAL'`
rows with `PASSED=FALSE` are what fail the Databricks task; `SEVERITY='WARN'` rows are
logged for trend/alerting but never block the run.
