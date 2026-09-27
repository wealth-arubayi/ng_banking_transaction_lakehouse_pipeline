# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Volume Structure Setup
# MAGIC Run this **once** after `sql/01_create_tables.sql` (which creates the `transaction_files`
# MAGIC Volume itself) and before the first pipeline run. Safe to re-run any time — every
# MAGIC `dbutils.fs.mkdirs` call is a no-op if the directory already exists.
# MAGIC
# MAGIC Creates the file lifecycle structure under `/Volumes/<catalog>/transaction_raw/transaction_files/`:
# MAGIC
# MAGIC | Folder | Purpose |
# MAGIC |---|---|
# MAGIC | `incoming/` | Drop new transaction extracts here — Auto Loader (notebook 01) watches this path |
# MAGIC | `processed/` | Files land here automatically right after Auto Loader durably commits them |
# MAGIC | `archive/YYYY/MM/DD/` | Populated weekly by `06_archive_processed_files.py`; **not** pre-created here — each dated folder is created on demand the first time a file needs it |
# MAGIC | `schema/transaction_events/` | Auto Loader's schema-evolution tracking location |
# MAGIC | `checkpoints/transaction_events/` | Auto Loader's streaming checkpoint |
# MAGIC | `reference/` | Upload `transaction_mapping_seed.csv` here if loading it via `COPY INTO` |
# MAGIC
# MAGIC You do **not** need to pre-create `archive/2026/09/27/`-style dated folders — Unity
# MAGIC Catalog Volumes create intermediate directories automatically the first time a file
# MAGIC is written under a new path, and `06_archive_processed_files.py` handles that itself.

# COMMAND ----------

# MAGIC %run ./00_pipeline_config

# COMMAND ----------

NOTEBOOK = "00b_setup_volume_structure"

REQUIRED_DIRS = [
    VOL_SOURCE,
    VOL_PROCESSED,
    VOL_ARCHIVE_ROOT,
    VOL_SCHEMA_LOC,
    VOL_CHECKPOINT,
    f"{VOL_ROOT}/reference/",
]

log_event(NOTEBOOK, "START", volume_root=VOL_ROOT)

created = []
for path in REQUIRED_DIRS:
    try:
        dbutils.fs.mkdirs(path)
        created.append(path)
    except Exception as exc:
        fail_notebook(NOTEBOOK, "COULD_NOT_CREATE_DIRECTORY", path=path, error=str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy `reference/transaction_mapping_seed.csv` into the Volume
# MAGIC The file lives in the repo at `reference/transaction_mapping_seed.csv` and is synced into
# MAGIC the workspace file tree alongside this notebook on every `databricks bundle deploy`. This
# MAGIC step copies that deployed copy into the Volume so `sql/02_seed_reference.sql`'s `COPY INTO`
# MAGIC always has an up-to-date source — re-running this notebook after editing the CSV and
# MAGIC redeploying keeps the two in sync automatically.
# MAGIC
# MAGIC **Best-effort, not fatal:** this only works when the notebook is actually running from a
# MAGIC synced bundle/Git-folder location (so a sibling `reference/` folder exists next to
# MAGIC `notebooks/`). If it can't be found — e.g. you're running this notebook standalone, outside
# MAGIC its repo structure — this logs a WARNING with manual upload instructions instead of failing
# MAGIC the whole setup, since every other directory above was created successfully regardless.

# COMMAND ----------

seed_copied = False
try:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    # e.g. ".../files/notebooks/00b_setup_volume_structure" -> ".../files/reference/transaction_mapping_seed.csv"
    repo_root = notebook_path.rsplit("/notebooks/", 1)[0]
    source_path = f"file:/Workspace{repo_root}/reference/transaction_mapping_seed.csv"

    dbutils.fs.cp(source_path, VOL_REFERENCE)
    seed_copied = True
    log_event(NOTEBOOK, "SEED_CSV_COPIED", source=source_path, destination=VOL_REFERENCE)
except Exception as exc:
    log_event(
        NOTEBOOK, "WARNING",
        reason="COULD_NOT_COPY_SEED_CSV",
        error=str(exc),
        manual_fallback=(
            f"Upload reference/transaction_mapping_seed.csv to {VOL_REFERENCE} manually via "
            "Catalog Explorer, or run the INSERT VALUES block in sql/02_seed_reference.sql instead "
            "of its COPY INTO statement."
        ),
    )

# COMMAND ----------

# Sanity check: list what's actually there now.
listing = dbutils.fs.ls(VOL_ROOT)
for entry in listing:
    print(f"{'DIR ' if entry.isDir() else 'FILE'}  {entry.path}")

log_event(NOTEBOOK, "SUCCESS", directories_ensured=created, seed_csv_copied=seed_copied)
dbutils.notebook.exit("SUCCESS")
