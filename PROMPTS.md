# PROMPTS.md — Vibe Coding Prompt Pack

Use these prompts **in order**. Each prompt corresponds to one Databricks notebook cell you’ll generate with an AI assistant. Keep your parameters secret; no hardcoding.

---

## Global Guardrails (paste once before generating cells)
You are my senior Databricks engineer. Generate production-ready code for Databricks (Unity Catalog). Prefer `spark.sql()` for catalog/schema/table ops; use PySpark where SQL cannot (filesystem, hashing, streaming). Do not remove features unless I say so. Keep code self-contained and runnable on a UC-enabled cluster. Log clearly. No secrets or hardcoded paths; everything must come from widgets/params.

---

## Notebook: Vibe Coding Ingest (Bronze)

Use these prompts **in order**. Each prompt corresponds to one Databricks notebook cell you’ll generate with an AI assistant. Keep your parameters secret; no hardcoding.

### Cell 1 — Title & Description (Markdown)
Create a Markdown cell titled **“Bronze Ingest from UC Volume (CSV/JSON, Auto Loader + Batch, Unity Catalog)”**. Describe that this notebook ingests mixed CSV/JSON from a UC Volume into Bronze Delta tables, supports Auto Loader and batch fallback, includes verification and a dry-run mode, and ends with a runbook.

---

### Cell 2 — Parameter Widgets & Assertions
Create a Python cell that:
- Defines dbutils widgets (text): `catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `volume_schema`, `volume_name`, `root_path`.
- Defines dropdown widgets:  
  `mode` in `["dry-run","load"]`,  
  `use_auto_loader` in `["true","false"]`,  
  `table_naming` in `["folder","filename"]`,  
  `overwrite_existing` in `["true","false"]`.
- Reads all widget values into variables.
- Asserts presence of: `catalog`, `bronze_schema`, `volume_schema`, `volume_name`, `root_path`.
- Prints a one-line params summary including `mode` and the boolean `use_auto_loader`.

---

### Cell 3 — Imports & Structured Logging
Create a Python cell that imports: `os`, `re`, `hashlib`, `sys`, `traceback`, `datetime`, typing (`Dict`, `Any`, `List`), `pyspark.sql.functions as F`, `pyspark.sql.types.*`, `pyspark.sql import DataFrame`, and `delta.tables import DeltaTable`.  
Define `log(msg, table=None, level="INFO", **kwargs)` that prints timestamped, leveled messages and pretty-prints extra kwargs.

---

### Cell 4 — Helper: Sanitize Table Names
Create `sanitize_table_name(name: str) -> str` that:
- lowercases,
- converts "-" to "_",
- replaces any non-word char with "_",
- collapses multiple "_" to one,
- strips leading/trailing "_",
and returns the sanitized name.

---

### Cell 5 — Helper: Recursive File Listing
Create `list_files_recursive(path: str) -> list[dict]` that:
- Recursively lists everything under `dbutils.fs.ls(path)`.
- For each **file** (not dir) returns `{ "path", "name", "ext" (lowercase without dot), "relpath" }`, where `relpath` is path relative to the input base. Do **not** reconstruct or lowercase actual file paths.

---

### Cell 6 — Helper: Group Files into Tables
Create `group_files(files: List[Dict], root_path: str, table_naming: str) -> Dict[str, List[Dict]]` that:
- If `table_naming=="folder"`: group by the first subfolder under `ROOT_PATH`; if a file is directly under `ROOT_PATH`, group by its base filename (no extension).
- If `table_naming=="filename"`: group by each file’s base filename.
- Sanitize the derived group name with `sanitize_table_name`.
- Return a dict mapping `table_name -> list of file dicts`.

---

### Cell 7 — Per-Table Overrides & Defaults
Create a cell that:
- Defines `table_overrides = collections.defaultdict(dict)`; include commented examples for `readOptions`, `partitionBy`, `inferSchema=False`, and explicit `schema` (`StructType`).
- Defines:
  - `default_csv_options = dict(header="true", inferSchema="true", multiline="false", delimiter=",")`
  - `default_json_options = dict(multiline="true")`
- Defines `partition_overrides = collections.defaultdict(lambda: ["p_ingest_date"])` with a comment showing how to disable by setting `[]`.

---

### Cell 8 — Privilege Validation (Fast-Fail) and Execute
Create `validate_privileges()` that:
- Runs `USE CATALOG <catalog>` and `USE SCHEMA <bronze_schema>` via `spark.sql`.
- Executes `SHOW TABLES IN \`<catalog>\`.\`<bronze_schema>\`` and collects to verify access.
- Logs success; on exception, logs `ERROR` and re-raises.  
Call `validate_privileges()` at the end of the cell.

---

### Cell 9 — Ingestion Metadata Columns & Hash
Create `add_ingest_metadata_cols(df: DataFrame, input_format: str, input_path_col: str = "_ingest_file_path") -> DataFrame` that:
- Adds `_ingest_ts = current_timestamp()` if missing.
- Sets `_ingest_file_path` using `_metadata.file_path` when available; else a nullable string column.
- Sets `_ingest_file_name` from the tail of `_ingest_file_path`.
- Sets `_source_format` to `input_format`.
- Adds `_input_record_hash` as `sha1(to_json(struct of all columns except _input_record_hash))`.
Return the new DataFrame.

---

### Cell 10 — Auto Loader Ingest (Per Group)
Implement `ingest_group_autoloader(group_files: List[Dict[str,Any]], table_name: str, table_opts: Dict[str,Any], dry_run: bool = True) -> Dict[str,Any]` that:
- Prepares an `output` dict with fields: `table_name`, `file_count`, `rows_written=0`, `status=("SKIPPED" if dry_run else "OK")`, `format=None`, `exception=None`, `path_pattern=None`, `partitionBy=[]`, `options={}`, `table_full_name=f"{catalog}.{bronze_schema}.{table_name}"`.
- Validates extensions: only `csv`/`json`; otherwise log `WARN` and return `output`.
- Picks `fmt`: `csv` if any csv else `json`.
- Computes `path_pattern` using the common prefix of discovered file paths; ensure it ends with `"/*.<fmt>"`. Do **not** lowercase or reconstruct actual file paths.
- Merges read options: start from defaults (`default_csv_options`/`default_json_options`), apply `table_opts["readOptions"]`, and if `"inferSchema"` is in `table_opts`, set options accordingly (`"true"`/`"false"`).
- Determine `partitionBy` from `table_opts["partitionBy"]` or `partition_overrides` or default `["p_ingest_date"]`.
- If `dry_run`: log `[DRY-RUN] Would ingest …` with options and return `output`.
- Else:
  - Build a `readStream` with format `"cloudFiles"`, set `cloudFiles.format=<fmt>`, `cloudFiles.schemaLocation=f"{root_path}/_schemas/{table_name}"`, apply options; if `schema` provided in `table_opts`, apply `.schema(schema)`. `load(path_pattern)`.
  - For CSV add `_rescued_data` (string) column (JSON has rescued data in newer runtimes).
  - Add ingest metadata via `add_ingest_metadata_cols` and add `p_ingest_date = to_date(_ingest_ts)`.
  - Optionally `dropDuplicates(["_input_record_hash"])` if `table_opts.get("dedupe", True)`.
  - WriteStream to Delta with `.outputMode("append")`, `checkpointLocation=f"{root_path}/_checkpoints/{table_name}"`, `.trigger(availableNow=True)`. Partition by `partitionBy` when present. Enable `mergeSchema`.
  - Use `toTable(table_full_name, tableProperties=props, overwrite=overwrite_existing)` where `props` includes:
    - `delta.enableChangeDataFeed=true`
    - `delta.autoOptimize.optimizeWrite=true`
    - `delta.autoOptimize.autoCompact=true`
    - `delta.columnMapping.mode=name`
    - `delta.minReaderVersion=2`
    - `delta.minWriterVersion=5`
    - `comment="Bronze from UC Volume: {root_path}/{table_name}"`
  - Await termination; count rows via `spark.table(table_full_name).count()`, set `rows_written`, `status="OK"`.
- On exception, set `status="ERROR"`, attach traceback text to `output["exception"]`, log `ERROR`, return.

---

### Cell 11 — Batch Ingest Fallback (Per Group)
Implement `ingest_group_batch(group_files: List[Dict[str,Any]], table_name: str, table_opts: Dict[str,Any], dry_run: bool = True) -> Dict[str,Any]` that:
- Mirrors output fields of the Auto Loader version.
- Validates extensions (csv/json only). Sets `fmt` and builds a comma-joined list of **actual file paths** (no lowercasing).
- Merges options as above; picks `partitionBy` and optional `schema`.
- If `dry_run`: log `[DRY-RUN] Would ingest …` and return.
- Else:
  - `reader = spark.read.format(fmt).option("includeMetadata","true")`, apply options, apply schema if provided, `load(paths)`.
  - For CSV add `_rescued_data` string column.
  - Add ingest metadata and `p_ingest_date`; optional dedupe by `_input_record_hash`.
  - Determine `table_full_name` and `table_exists = spark.catalog.tableExists(table_full_name)`.
  - Write with `df.write.format("delta")`
      - `.mode("overwrite" if overwrite_existing or not table_exists else "append")`
      - `.partitionBy(*partitionBy)`
      - `.option("mergeSchema","true")`
      - `.options(**table_props)` where `table_props` matches Auto Loader cell.
      - `.saveAsTable(table_full_name)`
    Log whether **Created/Overwritten/Appended**.
  - Count rows via `spark.table(...).count()`, set `rows_written`, `status="OK"`.
- On exception, set `status="ERROR"`, attach traceback, log `ERROR`, return.

---

### Cell 12 — Main Driver: Discover → Group → Ingest
Create the main driver cell that:
- Calls `list_files_recursive(root_path)`, logs total discovered files.
- Filters to `csv/json` only and logs that count.
- Groups with `group_files(..., table_naming)`.
- Iterates groups; `opts = table_overrides.get(table_name,{})`.
  - If `use_auto_loader` is `True`: call `ingest_group_autoloader(files, table_name, opts, dry_run=(mode=="dry-run"))`.
  - Else call `ingest_group_batch(...)`.
- Wrap each table in `try/except`; on error capture traceback + `status="ERROR"`.
- Append each result dict to a `summary` list.

---

### Cell 13 — Summary DataFrame
Build a pandas DataFrame from `summary`, convert to Spark DataFrame, and `display()` it.  
Log: “Ingestion complete. See above for per-table status and row counts.”

---

### Cell 14 — Mini Unit-Style Test (Dry-Run Only)
Create a self-test that runs **only when** `mode=="dry-run"`:
- Remove and recreate `/tmp/bronze_uc_test`.
- Write tiny `test1.csv` (`a,b\n1,2\n3,4`) and `test2.json` (`{"a":1,"b":2}\n{"a":3,"b":4}`).
- Use `list_files_recursive + group_files(..., "filename")` to form groups.
- Call `ingest_group_batch(..., dry_run=True)` and `ingest_group_autoloader(..., dry_run=True)` for each group.
- Assert `file_count==1` and `format in {"csv","json"}` for both paths.
- Log: “Mini ingest path test: PASS”.

---

### Cell 15 — RUNBOOK (Markdown)
Create a Markdown runbook titled **“RUNBOOK: Bronze Ingest from UC Volumes”** with:
- How to set widgets/params and run `MODE=dry-run` then `MODE=load`.
- How to switch Auto Loader to continuous (`trigger=processingTime`).
- How to set per-table overrides in `table_overrides` and `partition_overrides`.
- How to read CDF downstream (sample queries, `DESCRIBE HISTORY`).
- `VACUUM` and `GRANT` examples; how to reprocess a single table by filtering groups.
- Notes on checkpoint and schema locations under the volume path.

---

### Optional Footer — Guardrails Used
- Prefer `spark.sql()` for UC ops; all config via widgets; no hardcoded secrets.
- Never mutate or lowercase file paths returned by `dbutils.fs`.
- Add `_ingest_*` metadata and `_input_record_hash` for idempotency.
- Set CDF, `autoOptimize`, `columnMapping=name`, min reader/writer versions.
- Dry-run must do **no writes**; load must verify table existence and row counts.
- Per-table errors do **not** stop the whole run; preflight failures should.

---

## Notebook: Vibe Coding - Load (Silver)

Use these prompts **in order** to generate a simple, generic **Silver loader** that reads every Bronze table in a catalog, applies cleaning/deduplication, and writes to the Silver schema. Keep parameters explicit; minimal assumptions.

### Cell 1 — Title & Description (Markdown)
Create a Markdown cell titled **“Silver Table Loader: Clean and Deduplicate from Bronze (vibecoding)”** that explains:
- Reads all tables in `<catalog>.<bronze_schema>`, writes to `<catalog>.<silver_schema>`.
- Steps per table: drop exact duplicates & all-null rows; add `_silver_loaded_at`; attempt business-key dedupe (columns ending with `id` or `key`); drop Bronze/ingest metadata columns; overwrite Silver table.
- Note that per-table customization may be needed for complex domains.

---

### Cell 2 — Parameters & Schema Creation
Create a Python cell that:
- Declares string variables: `catalog = "<your_catalog>"`, `bronze_schema = "bronze"`, `silver_schema = "silver"` (leave placeholders for the user to edit).
- Runs `CREATE SCHEMA IF NOT EXISTS <catalog>.<silver_schema>` via `spark.sql` to ensure the Silver schema exists.

---

### Cell 3 — Imports & Helper for Candidate Key
Create a Python cell that:
- Imports `pyspark.sql.functions as F` and `pyspark.sql.utils.AnalysisException`.
- Defines `get_candidate_key(df)` that scans `df.columns` and returns the first column whose lowercase name ends with `"key"` or `"id"`, else `None`.

---

### Cell 4 — Enumerate Bronze Tables
Create a Python cell that:
- Queries `SHOW TABLES IN <catalog>.<bronze_schema>` and collects the list of `tableName` values into `bronze_tables`.
- Prints the count and the list, e.g., `Found N bronze tables: [...]`.

---

### Cell 5 — Process Loop: Bronze → Silver
Create a Python cell that loops over `bronze_tables` and for each:
- Build `full_bronze = f"{catalog}.{bronze_schema}.{table}"` and `full_silver = f"{catalog}.{silver_schema}.{table}"`.
- Try:
  - `df = spark.table(full_bronze)`
  - `df = df.dropDuplicates().na.drop(how="all")`
  - `df = df.withColumn("_silver_loaded_at", F.current_timestamp())`
  - `key_col = get_candidate_key(df)`; if present:
    - `df = df.dropDuplicates([key_col])`
    - `df = df.filter(F.col(key_col).isNotNull())`
  - Drop any metadata columns that start with `"_bronze"` or `"_ingest"` (derive list and drop if non-empty).
  - Write to Silver with overwrite and schema evolution:
    ```
    (df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(full_silver))
    ```
  - Print `Wrote silver table: <full_silver> (<row_count> rows)`
- Except `AnalysisException`: print `Skipping <full_bronze>: <error>` and continue.

---

### Cell 6 — Next Steps (Markdown)
Create a Markdown cell titled **“Next Steps”** that shows:
- SQL snippet to list Silver tables:  
  ```sql
  SHOW TABLES IN <catalog>.silver;

## Notebook: Vibe Coding - Gold (serve)
### Prompts — Gold: `gold_sales_monthly` (Catalog = `vibecoding`)

#### Option A — One-liner
Create a Gold table called `gold_sales_monthly` in catalog **vibecoding** from my Silver data (`silver` schema) and write it to the `gold` schema. Use Internet + Reseller sales, group by month (yyyymm), channel, product category & subcategory, and calculate revenue, units, orders, gross margin, and average order value. Make it idempotent, create the gold schema if missing, partition by (channel, yyyymm), enable CDF and auto-optimize, verify row count > 0, and show a small preview. Return **one** Databricks **Python** cell using `spark.sql` (code only, no prose).

---

#### Option B — Fill-in with fixed catalog
Build a Gold table for me.

Catalog: **vibecoding**  
Silver schema: **silver**  
Gold schema: **gold**  
Gold table name: **gold_sales_monthly**

What I want:
- Combine Internet + Reseller sales
- Month = yyyymm from order date
- Group by month, channel, product category, product subcategory
- Metrics: revenue, units, orders, gross margin, average order value
- If a `SALES_BASE` table exists in Silver, use it; otherwise build it inline
- Safe to re-run (idempotent); create schema if needed
- Partition by (channel, yyyymm); enable CDF and auto-optimize
- End with a quick row-count check and top-10 preview

Return only one runnable **Databricks Python** cell (no explanations).

---

#### Option C — Conversational
You are my Databricks engineer. All my data is in a catalog called **vibecoding**.

Task: Create the Gold table `gold_sales_monthly` from the `silver` schema and write it to the `gold` schema. Use Internet + Reseller sales with DimDate and product dims, group by yyyymm, channel, category, subcategory, and compute revenue, units, orders, gross margin, and average order value. If `SALES_BASE` exists in Silver, use it; otherwise build it inline. Make it idempotent, create the gold schema if missing, partition by (channel, yyyymm), enable CDF and auto-optimize, then verify the table exists, row count > 0, and show a small preview. Output only a single runnable **Python** cell using `spark.sql`.



  ### Prompt — Gold ad-hoc query: Internet customer summary

You are my analytics SQL assistant. Produce **ONLY** a single Databricks SQL `SELECT` statement (no prose).

Task: Summarize **internet sales per customer** from `vibecoding.silver.FactInternetSales`.

Requirements:
- Group by `CustomerKey`.
- Select exactly:
  - `CustomerKey`
  - `SUM(SalesAmount) AS total_sales_amount`
  - `COUNT(DISTINCT SalesOrderNumber) AS orders`
  - `MIN(OrderDateKey) AS first_order_date`
  - `MAX(OrderDateKey) AS last_order_date`
- (Optional) Also include `SUM(OrderQuantity) AS total_units`.
- No filters, no DDL, no comments—just the `SELECT`. Optional `ORDER BY CustomerKey` at the end.


  
