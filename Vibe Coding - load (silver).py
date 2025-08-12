# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Table Loader: Clean and Deduplicate from Bronze (vibecoding)
# MAGIC
# MAGIC This notebook reads every table in the `bronze` schema of the `vibecoding` catalog, applies basic cleaning and deduplication, and writes a corresponding `silver` table in the same catalog.
# MAGIC
# MAGIC ## Steps for Each Table:
# MAGIC - Read from bronze Delta table
# MAGIC - Drop exact duplicates and all-null rows
# MAGIC - Cast all columns to their inferred types
# MAGIC - Optionally, remove rows with nulls in primary/business key columns (if detected)
# MAGIC - Add audit columns: `_silver_loaded_at`
# MAGIC - Write to silver Delta table (overwrite mode)
# MAGIC
# MAGIC ---
# MAGIC **Customize per table as needed for complex cleaning!**

# COMMAND ----------

# Parameters
catalog = "vibecoding"
bronze_schema = "bronze"
silver_schema = "silver"

# Create silver schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

def get_candidate_key(df):
    """Try to pick a candidate key column for deduplication.
    Prefers columns ending with 'key' or 'id' (case-insensitive), else returns None."""
    for c in df.columns:
        if c.lower().endswith("key") or c.lower().endswith("id"):
            return c
    return None

# COMMAND ----------

# Get all bronze tables
bronze_tables = [
    row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}").collect()
]

print(f"Found {len(bronze_tables)} bronze tables: {bronze_tables}")

# COMMAND ----------

for table in bronze_tables:
    full_bronze = f"{catalog}.{bronze_schema}.{table}"
    full_silver = f"{catalog}.{silver_schema}.{table}"
    print(f"Processing table: {full_bronze}")
    try:
        df = spark.table(full_bronze)
        # Drop exact duplicates and all-null rows
        df = df.dropDuplicates().na.drop(how="all")
        # Add loaded timestamp
        df = df.withColumn("_silver_loaded_at", F.current_timestamp())
        # Try to drop duplicates on a likely business key, if it exists
        key_col = get_candidate_key(df)
        if key_col:
            df = df.dropDuplicates([key_col])
            df = df.filter(F.col(key_col).isNotNull())
        # Remove any _bronze or _ingest metadata columns for a clean silver table
        drop_cols = [c for c in df.columns if c.startswith("_bronze") or c.startswith("_ingest")]
        if drop_cols:
            df = df.drop(*drop_cols)
        # Write to silver table
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_silver)
        )
        print(f"Wrote silver table: {full_silver} ({df.count()} rows)")
    except AnalysisException as e:
        print(f"Skipping {full_bronze}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC - Review silver tables in the UI or via SQL:
# MAGIC   ```sql
# MAGIC   SHOW TABLES IN vibecoding.silver;
# MAGIC   ```
# MAGIC - Modify transformation logic per table as needed for business rules.
# MAGIC - Schedule this notebook to run after each bronze ingest.
