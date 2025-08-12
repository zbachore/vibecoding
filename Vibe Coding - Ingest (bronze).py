# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest from UC Volume (CSV/JSON, Auto Loader + Batch, Unity Catalog)
# MAGIC
# MAGIC **PRODUCTION-READY** notebook for flexible ingestion of mixed CSV/JSON data from a Unity Catalog Volume into Delta Bronze tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Parameters

# COMMAND ----------

# DBTITLE 1,Parameter Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("volume_schema", "")
dbutils.widgets.text("volume_name", "")
dbutils.widgets.text("root_path", "")
dbutils.widgets.dropdown("mode", "dry-run", ["dry-run", "load"])
dbutils.widgets.dropdown("use_auto_loader", "true", ["true", "false"])
dbutils.widgets.dropdown("table_naming", "folder", ["folder", "filename"])
dbutils.widgets.dropdown("overwrite_existing", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
volume_schema = dbutils.widgets.get("volume_schema")
volume_name = dbutils.widgets.get("volume_name")
root_path = dbutils.widgets.get("root_path")
mode = dbutils.widgets.get("mode")
use_auto_loader = dbutils.widgets.get("use_auto_loader") == "true"
table_naming = dbutils.widgets.get("table_naming")
overwrite_existing = dbutils.widgets.get("overwrite_existing") == "true"

assert catalog, "catalog is required"
assert bronze_schema, "bronze_schema is required"
assert volume_schema, "volume_schema is required"
assert volume_name, "volume_name is required"
assert root_path, "root_path is required"

print(f"Params: catalog={catalog}, bronze_schema={bronze_schema}, root_path={root_path}, mode={mode}, use_auto_loader={use_auto_loader}")

# COMMAND ----------

# DBTITLE 1,Imports and Logging Setup
import os, re, hashlib, sys, traceback
from datetime import datetime
from typing import Dict, Any, List
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame

from delta.tables import DeltaTable

# Simple structured logging
def log(msg, table=None, level="INFO", **kwargs):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{level}]"
    if table: prefix += f"[{table}]"
    print(f"{prefix} {ts} | {msg}")
    if kwargs: print("   ", kwargs)

# COMMAND ----------

# DBTITLE 1,Helper: Sanitize Table Names
def sanitize_table_name(name: str) -> str:
    """
    Lower, underscores, only alphanum/_.
    """
    name = name.lower().replace("-", "_")
    name = re.sub(r"[^\w]", "_", name)
    name = re.sub(r"__+", "_", name)
    return name.strip("_")

# COMMAND ----------

# DBTITLE 1,Helper: List All Files Recursively in ROOT_PATH
def list_files_recursive(path: str):
    """
    Recursively list all files under path.
    Returns list of dicts: {path, name, ext, relpath, group_key}
    """
    files = []
    base_len = len(path.rstrip("/")) + 1
    for row in dbutils.fs.ls(path):
        if row.isDir():
            files += list_files_recursive(row.path)
        else:
            ext = os.path.splitext(row.name)[1].lower().lstrip(".")
            relpath = row.path[base_len:]
            files.append({
                "path": row.path,
                "name": row.name,
                "ext": ext,
                "relpath": relpath
            })
    return files

# COMMAND ----------

# DBTITLE 1,Helper: Group Files to Tables
def group_files(files: List[Dict], root_path: str, table_naming: str):
    """
    Groups files by table according to immediate subfolder or filename.
    Returns table_name -> list of file dicts.
    """
    groups = {}
    for file in files:
        rel = file["relpath"]
        parts = rel.split("/")
        if table_naming == "folder":
            if len(parts) > 1:
                group = sanitize_table_name(parts[0])
            else:
                group = sanitize_table_name(os.path.splitext(parts[0])[0])
        elif table_naming == "filename":
            group = sanitize_table_name(os.path.splitext(parts[-1])[0])
        else:
            raise ValueError(f"Unknown table_naming: {table_naming}")
        if group not in groups: groups[group] = []
        groups[group].append(file)
    return groups

# COMMAND ----------

# DBTITLE 1,Per-Table Overrides (readOptions, partitionBy, schema)
from collections import defaultdict

table_overrides = defaultdict(dict)
# Example:
# table_overrides['mytable'] = {
#     'readOptions': {'delimiter': '|', 'quote': '"'},
#     'partitionBy': ['my_col'],
#     'inferSchema': False,
#     'schema': StructType([...])
# }

default_csv_options = dict(header="true", inferSchema="true", multiline="false", delimiter=",")
default_json_options = dict(multiline="true")

partition_overrides = defaultdict(lambda: ["p_ingest_date"])  # table_name -> partition columns (list or empty)

# You can override per-table as:
# partition_overrides['mytable'] = []  # disables partitioning

# COMMAND ----------

# DBTITLE 1,Privilege Validation Fast-Fail
def validate_privileges():
    try:
        spark.sql(f"USE CATALOG `{catalog}`")
        spark.sql(f"USE SCHEMA `{bronze_schema}`")
        _ = spark.sql(f"SHOW TABLES IN `{catalog}`.`{bronze_schema}`").collect()
        log("Privilege validation succeeded.")
    except Exception as e:
        log("Privilege validation failed. Check your permissions on the catalog/schema.", level="ERROR")
        raise

validate_privileges()

# COMMAND ----------

# DBTITLE 1,Ingestion Metadata Columns & Hash
def add_ingest_metadata_cols(df: DataFrame, input_format: str, input_path_col: str = "_ingest_file_path"):
    """
    Adds: _ingest_ts, _ingest_file_path, _ingest_file_name, _source_format, _input_record_hash
    """
    if "_ingest_ts" not in df.columns:
        df = df.withColumn("_ingest_ts", F.current_timestamp())
    # Correct way to extract file path from metadata struct
    if "_ingest_file_path" not in df.columns:
        if "_metadata" in df.columns:
            df = df.withColumn("_ingest_file_path", F.col("_metadata")["file_path"])
        else:
            df = df.withColumn("_ingest_file_path", F.lit(None).cast("string"))
    if "_ingest_file_name" not in df.columns:
        df = df.withColumn("_ingest_file_name", F.element_at(F.split(F.col("_ingest_file_path"), "/"), -1))
    if "_source_format" not in df.columns:
        df = df.withColumn("_source_format", F.lit(input_format))
    df = df.withColumn("_input_record_hash", F.sha1(F.to_json(F.struct([F.col(c) for c in df.columns if c != "_input_record_hash"]))))
    return df

# COMMAND ----------

# DBTITLE 1,Auto Loader Ingest Function
def ingest_group_autoloader(
    group_files: List[Dict[str,Any]],
    table_name: str,
    table_opts: Dict[str,Any],
    dry_run: bool = True
) -> Dict[str,Any]:
    """
    Ingests files for one table using Auto Loader.
    Returns dict with table info and stats.
    """
    output = {
        "table_name": table_name,
        "file_count": len(group_files),
        "rows_written": 0,
        "status": "SKIPPED" if dry_run else "OK",
        "format": None,
        "exception": None,
        "path_pattern": None,
        "partitionBy": [],
        "options": {},
        "table_full_name": f"{catalog}.{bronze_schema}.{table_name}"
    }
    try:
        exts = set(f["ext"] for f in group_files)
        if exts - {"csv", "json"}:
            log(f"Skipping unsupported file types: {exts - {'csv', 'json'}}", table=table_name, level="WARN")
            return output

        fmt = "csv" if "csv" in exts else "json"
        output["format"] = fmt
        # Use actual file paths as discovered - do not lowercase or reconstruct
        path_pattern = os.path.commonprefix([f["path"] for f in group_files])
        if not path_pattern.endswith("/"): path_pattern = os.path.dirname(path_pattern)
        path_pattern += "/*." + fmt
        output["path_pattern"] = path_pattern

        # Merge default, per-table, and user overrides
        user_opts = table_opts.get("readOptions", {})
        options = dict(default_csv_options if fmt=="csv" else default_json_options)
        options.update(user_opts)
        if "inferSchema" in table_opts:
            options["inferSchema"] = str(table_opts["inferSchema"]).lower()
        output["options"] = options

        # Partitioning
        partitionBy = table_opts.get("partitionBy", partition_overrides.get(table_name, ["p_ingest_date"]))
        output["partitionBy"] = partitionBy

        # Schema
        schema = table_opts.get("schema", None)

        # Dry-run mode: just print what would do
        if dry_run:
            log(f"[DRY-RUN] Would ingest {len(group_files)} files as {fmt} to {output['table_full_name']}", table=table_name, level="INFO", options=options)
            return output

        # Main load
        log(f"Starting Auto Loader ingest: {output['table_full_name']}, {len(group_files)} files, format={fmt}", table=table_name, options=options)
        schema_location = f"{root_path}/_schemas/{table_name}"
        autoloader_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_location)
        )
        for k,v in options.items():
            autoloader_df = autoloader_df.option(k, v)
        if schema is not None:
            autoloader_df = autoloader_df.schema(schema)
        autoloader_df = autoloader_df.load(path_pattern)

        # Add rescued data
        if fmt == "csv":
            autoloader_df = autoloader_df.withColumn("_rescued_data", F.lit(None).cast("string"))
        else:
            # JSON has _rescued_data by default in new versions
            pass

        # Add ingestion metadata
        autoloader_df = add_ingest_metadata_cols(autoloader_df, fmt)

        # Partition col
        autoloader_df = autoloader_df.withColumn("p_ingest_date", F.to_date(F.col("_ingest_ts")))

        # Dedupe by hash if desired
        dedupe = table_opts.get("dedupe", True)
        if dedupe:
            autoloader_df = autoloader_df.dropDuplicates(["_input_record_hash"])

        # Table creation/writing
        table_full_name = output['table_full_name']
        table_exists = spark.catalog.tableExists(table_full_name)
        writer = (
            autoloader_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{root_path}/_checkpoints/{table_name}")
            .trigger(availableNow=True)
        )
        if partitionBy:
            writer = writer.partitionBy(*partitionBy)
        # Table properties for CDF, optimize, etc.
        table_props = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "comment": f"Bronze from UC Volume: {root_path}/{table_name}",
        }
        writer = writer.option("mergeSchema", "true")
        # In UC, managed tables default
        writer = writer.toTable(table_full_name, tableProperties=table_props, overwrite=overwrite_existing)

        # Wait for batch (availableNow)
        query = writer.awaitTermination()
        # Row count
        rows = spark.table(table_full_name).count()
        output["rows_written"] = rows
        output["status"] = "OK"
        log(f"Ingested {rows} rows to {table_full_name}", table=table_name)
    except Exception as e:
        output["status"] = "ERROR"
        output["exception"] = traceback.format_exc()
        log(f"Error in Auto Loader ingest: {e}", table=table_name, level="ERROR")
    return output

# COMMAND ----------

# DBTITLE 1,Batch Fallback Ingest Function (Non-Auto Loader)
def ingest_group_batch(
    group_files: List[Dict[str,Any]],
    table_name: str,
    table_opts: Dict[str,Any],
    dry_run: bool = True
) -> Dict[str,Any]:
    """
    Ingests files for one table using batch read.
    Returns dict with table info and stats.
    """
    output = {
        "table_name": table_name,
        "file_count": len(group_files),
        "rows_written": 0,
        "status": "SKIPPED" if dry_run else "OK",
        "format": None,
        "exception": None,
        "path_pattern": None,
        "partitionBy": [],
        "options": {},
        "table_full_name": f"{catalog}.{bronze_schema}.{table_name}"
    }
    try:
        exts = set(f["ext"] for f in group_files)
        if exts - {"csv", "json"}:
            log(f"Skipping unsupported file types: {exts - {'csv', 'json'}}", table=table_name, level="WARN")
            return output

        fmt = "csv" if "csv" in exts else "json"
        output["format"] = fmt
        # Use the actual file paths as returned by file listing!
        paths = [f["path"] for f in group_files if f["ext"] == fmt]
        output["path_pattern"] = ",".join(paths)

        # Merge default, per-table, and user overrides
        user_opts = table_opts.get("readOptions", {})
        options = dict(default_csv_options if fmt=="csv" else default_json_options)
        options.update(user_opts)
        if "inferSchema" in table_opts:
            options["inferSchema"] = str(table_opts["inferSchema"]).lower()
        output["options"] = options

        # Partitioning
        partitionBy = table_opts.get("partitionBy", partition_overrides.get(table_name, ["p_ingest_date"]))
        output["partitionBy"] = partitionBy

        # Schema
        schema = table_opts.get("schema", None)

        # Dry-run mode: just print what would do
        if dry_run:
            log(f"[DRY-RUN] Would ingest {len(paths)} files as {fmt} to {output['table_full_name']}", table=table_name, level="INFO", options=options)
            return output

        # Main load
        log(f"Starting batch ingest: {output['table_full_name']}, {len(paths)} files, format={fmt}", table=table_name, options=options)
        reader = spark.read.format(fmt).option("includeMetadata", "true")
        for k,v in options.items():
            reader = reader.option(k, v)
        if schema is not None:
            reader = reader.schema(schema)
        df = reader.load(paths)

        # Add rescued data
        if fmt == "csv":
            df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
        else:
            pass

        # Add ingestion metadata
        df = add_ingest_metadata_cols(df, fmt)

        # Partition col
        df = df.withColumn("p_ingest_date", F.to_date(F.col("_ingest_ts")))

        # Dedupe by hash if desired
        dedupe = table_opts.get("dedupe", True)
        if dedupe:
            df = df.dropDuplicates(["_input_record_hash"])

        table_full_name = output['table_full_name']
        table_exists = spark.catalog.tableExists(table_full_name)
        print(f'table_exists: {table_exists}')

        # Write/merge
        table_props = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "comment": f"Bronze from UC Volume: {root_path}/{table_name}",
        }
        if not table_exists or overwrite_existing:
            (df.write
                .format("delta")
                .mode("overwrite" if overwrite_existing else "append")
                .partitionBy(*partitionBy)
                .option("mergeSchema", "true")
                .options(**table_props)
                .saveAsTable(table_full_name)
            )
            op = "Created" if not table_exists else "Overwritten"
        else:
            # Merge/Append
            (df.write
                .format("delta")
                .mode("append")
                .partitionBy(*partitionBy)
                .option("mergeSchema", "true")
                .options(**table_props)
                .saveAsTable(table_full_name)
            )
            op = "Appended"
        rows = spark.table(table_full_name).count()
        output["rows_written"] = rows
        output["status"] = "OK"
        log(f"{op} {rows} rows to {table_full_name}", table=table_name)
    except Exception as e:
        output["status"] = "ERROR"
        output["exception"] = traceback.format_exc()
        log(f"Error in batch ingest: {e}", table=table_name, level="ERROR")
    return output

# COMMAND ----------

# DBTITLE 1,Main Driver: Enumerate, Group, Ingest
all_files = list_files_recursive(root_path)
log(f"Discovered {len(all_files)} files under {root_path}.")
# Filter for supported file types
all_files = [f for f in all_files if f["ext"] in ("csv", "json")]
log(f"{len(all_files)} CSV/JSON files to process.")
groups = group_files(all_files, root_path, table_naming)

summary = []
for table_name, files in groups.items():
    opts = table_overrides.get(table_name, {})
    try:
        if use_auto_loader:
            result = ingest_group_autoloader(files, table_name, opts, dry_run=(mode=="dry-run"))
        else:
            result = ingest_group_batch(files, table_name, opts, dry_run=(mode=="dry-run"))
    except Exception as e:
        result = {
            "table_name": table_name,
            "exception": traceback.format_exc(),
            "status": "ERROR"
        }
    summary.append(result)

# COMMAND ----------

# DBTITLE 1,Summary DataFrame
import pandas as pd

summary_df = pd.DataFrame(summary)
display(spark.createDataFrame(summary_df))

log("Ingestion complete. See above for per-table status and row counts.")

# COMMAND ----------

# DBTITLE 1,Unit-style Mini Test: CSV/JSON Ingest Path (Dry-Run, /tmp)
if mode == "dry-run":
    import tempfile
    import shutil

    tmp_dir = "/tmp/bronze_uc_test"
    dbutils.fs.rm(tmp_dir, True)
    dbutils.fs.mkdirs(tmp_dir)
    # Write a simple CSV and JSON
    csv_path = f"{tmp_dir}/test1.csv"
    json_path = f"{tmp_dir}/test2.json"
    dbutils.fs.put(csv_path, "a,b\n1,2\n3,4", overwrite=True)
    dbutils.fs.put(json_path, '{"a":1,"b":2}\n{"a":3,"b":4}', overwrite=True)
    test_files = list_files_recursive(tmp_dir)
    test_groups = group_files(test_files, tmp_dir, "filename")
    # Try batch and autoloader in dry-run
    for table_name, files in test_groups.items():
        res_batch = ingest_group_batch(files, table_name, {}, dry_run=True)
        res_loader = ingest_group_autoloader(files, table_name, {}, dry_run=True)
        assert res_batch["file_count"] == 1
        assert res_loader["file_count"] == 1
        assert res_batch["format"] in ("csv", "json")
        assert res_loader["format"] in ("csv", "json")
    log("Mini ingest path test: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC # RUNBOOK: Bronze Ingest from UC Volumes
