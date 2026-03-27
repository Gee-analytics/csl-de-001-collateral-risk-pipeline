# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7266b952-80ed-467c-93c3-06bb66189b5e",
# META       "default_lakehouse_name": "CSL_Collateral_Risk_LH",
# META       "default_lakehouse_workspace_id": "afee936f-76b3-4600-bbec-695c27501baf",
# META       "known_lakehouses": [
# META         {
# META           "id": "7266b952-80ed-467c-93c3-06bb66189b5e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Stream B: Bronze S3 Bank Balance Ingestion
# 
# **Project:** Collateral Risk Monitoring & Margin Call Automation System <br>
# **Organisation:** Collection Solutions Limited (CSL) <br>
# **Notebook:** nb_bronze_s3_bank_balance_ingest  
# **Layer:** Bronze  
# **Source:** Amazon S3 via Fabric Shortcut  
# **Target Table:** bronze_bank_balance_update  
# **Version:** 1.0  
# **Last Updated:** March 2026  
# 
# ## Purpose
# This notebook ingests daily balance update files dropped by client banks 
# into the S3 bucket. It implements incremental loading logic using the 
# ReportingDate column as a watermark to ensure only new records are appended 
# on each run, avoiding duplicates and unnecessary reprocessing.
# 
# ## Client Folders
# CLT-001, CLT-002, CLT-003, CLT-004
# 
# ## Metadata Columns Added
# - ingestion_timestamp: exact datetime this notebook ran
# - source_system: s3_clientbank
# - pipeline_run_id: identifies the specific pipeline execution batch
# - SourceFileName: traces each record back to the specific client file it came from


# MARKDOWN ********************

# ## Step 1: Imports and Configuration
# All dependencies and pipeline configuration variables are defined here. 
# Centralising configuration at the top means any changes to folder paths, 
# table names, or pipeline identifiers only need to be made in one place.

# CELL ********************

# Import specific Spark SQL functions we need for adding metadata columns:
# - lit: creates a literal/constant column value, used for source_system and pipeline_run_id
# - current_timestamp: captures the exact moment the notebook runs, used for ingestion_timestamp
# - input_file_name: returns the full file path of the source file being read, used for SourceFileName
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


# Configuration
SOURCE_ROOT_PATH = "Files/client-drops"
CLIENT_FOLDERS = ["CLT-001", "CLT-002", "CLT-003", "CLT-004"]
TARGET_TABLE = "bronze_bank_balance_update"
SOURCE_SYSTEM = "s3_clientbank"
PIPELINE_RUN_ID = "manual_run_001"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Watermark Check
# Check the maximum ReportingDate already loaded in bronze_bank_balance_update.
# If the table does not exist or is empty, default to 1900-01-01 to ensure all 
# records pass the filter on the first run. This prevents duplicate loading on 
# subsequent runs by only processing records newer than the last loaded date.

# CELL ********************

# Check the maximum ReportingDate already loaded in the Bronze table.
# If the table does not exist or is empty, default to 1900-01-01 to load everything.
try:
    last_loaded = spark.table(TARGET_TABLE).select(F.max("ReportingDate")).collect()[0][0]
    if last_loaded is None:
        raise ValueError("Table is empty")
    print(f"Table found. Last loaded date: {last_loaded}")

except (AnalysisException, ValueError) as e:
    print(f"Notice: {e}. Setting default start date.")
    # Seed with a date so far in the past that every record will be newer than it
    last_loaded = datetime(1900, 1, 1)
    print(f"Starting full load from: {last_loaded}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Read Parquet Files from S3 Client Folders
# Each client folder is read individually so we can capture the source filename 
# per record before combining. Reading folder-by-folder also makes it easy to 
# add per-client logic in future without restructuring the notebook.

# CELL ********************

# This empty list will hold the parquet files from each client folder
all_dfs = []

# Loop through each client folder
for client in CLIENT_FOLDERS:
    folder_path = f"{SOURCE_ROOT_PATH}/{client}"

    # Read all parquet files in this client folder
    df = spark.read.parquet(folder_path)

    # Add SourceFileName column capturing the full file path
    df = df.withColumn("SourceFileName", F.input_file_name())

    # Append to our collection
    all_dfs.append(df)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Union All Client DataFrames
# All four client DataFrames are combined into a single DataFrame using unionAll. 
# ClientID is already present on every record so no client-level granularity is lost. 
# Row and column counts are printed as an immediate validation checkpoint.

# CELL ********************

# Union all client DataFrames into one combined DataFrame
combined_df = reduce(DataFrame.unionAll, all_dfs)

print(f"Total rows read: {combined_df.count()}")
print(f"Total columns: {len(combined_df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Cast Timestamp Columns to Standard Timestamp Type
# ReportingDate and LastPaymentDate are written by PyArrow as timestamp_ntz 
# (without timezone), which is not supported by the Fabric SQL Analytics Endpoint. 
# Casting to standard timestamp ensures full visibility and queryability across 
# all Fabric interfaces.

# CELL ********************


# ReportingDate and LastPaymentDate are written by PyArrow as timestamp_ntz (without timezone),
# which is not supported by the Fabric SQL Analytics Endpoint. Casting to standard timestamp
# ensures full visibility and queryability across all Fabric interfaces.
combined_df = (
    combined_df
    .withColumn("ReportingDate", F.col("ReportingDate").cast("timestamp"))
    .withColumn("LastPaymentDate", F.col("LastPaymentDate").cast("timestamp"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Add Metadata Columns
# Three audit columns are stamped onto every record at ingestion time. 
# ingestion_timestamp records exactly when this notebook ran. source_system 
# identifies the origin of this data for lineage tracking. pipeline_run_id 
# allows any batch of records to be traced back to a specific pipeline execution.

# CELL ********************

# Stamp audit metadata onto every record at ingestion time
# ingestion_timestamp: when this notebook ran
# source_system: identifies S3 client bank as the data origin
# pipeline_run_id: traces this batch back to a specific pipeline execution
combined_df = (
    combined_df
    .withColumn('ingestion_timestamp', F.current_timestamp())
    .withColumn('source_system', F.lit(SOURCE_SYSTEM))
    .withColumn('pipeline_run_id', F.lit(PIPELINE_RUN_ID))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Extract Filename from Full ABFSS Path
# input_file_name() returns the full internal OneLake path. We extract only the 
# client-drops/CLT-XXX/filename.parquet portion to keep the SourceFileName column 
# clean, readable, and traceable back to both the client and the reporting date.

# CELL ********************

# Extract only the client-drops/CLT-XXX/filename.parquet portion from the full ABFSS path
# This keeps SourceFileName clean, readable, and traceable to both client and reporting date
combined_df = combined_df.withColumn(
    "SourceFileName",
    F.regexp_extract(F.col("SourceFileName"), r"(client-drops/.+\.parquet)$", 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Filter to New Records Only
# Filter the combined DataFrame to only include records with a ReportingDate 
# newer than the watermark date established in Step 2. If no new records exist, 
# the notebook exits cleanly without writing anything to the Delta table.

# CELL ********************

# Filter to only records with a ReportingDate newer than the last loaded date
# This ensures we never re-load data already present in the Bronze table
filtered_combined_df = combined_df.filter(F.col("ReportingDate") > last_loaded)

# Count new records
row_count = filtered_combined_df.count()
print(f"New records found: {row_count}")

# Exit early if nothing new to load
if row_count == 0:
    print("Client folders have not been updated. No new data to load.")
    # raise SystemExit("No new data to load.")
    mssparkutils.notebook.exit("No new data to load.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9: Schema Verification
# Print the full schema of the filtered DataFrame to confirm all columns are 
# present with the correct data types before writing to the Delta table.

# CELL ********************

# Confirm all columns are present with correct data types before writing
filtered_combined_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 10: Sample Data Preview
# Display the first 5 rows of the filtered DataFrame for a visual sanity check. 
# Confirms SourceFileName is clean, timestamps are properly formatted, and 
# metadata columns are populated correctly.

# CELL ********************

# Sample a few rows
display(filtered_combined_df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 11: Final Dimension Check
# Confirm the final row and column count of the filtered DataFrame before writing. 
# This is the last validation checkpoint before the write operation.

# CELL ********************

# Confirm final row and column count of the filtered DataFrame
print(f"Rows to write: {filtered_combined_df.count()}")
print(f"Total columns: {len(filtered_combined_df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 12: Write to Bronze Delta Table
# Write filtered records to the bronze_bank_balance_update Delta table.
# On first run, the table is created with a full initial load.
# On subsequent runs, only new records beyond the watermark are appended.
# This pattern ensures no duplicates and no unnecessary reprocessing.

# CELL ********************

# Write filtered records to Bronze Delta table
# If table does not exist, create it with full initial load
# If table exists, append only new records
try:
    spark.table(TARGET_TABLE)
    filtered_combined_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)
    print(f"{TARGET_TABLE} exists. Appended {row_count} new records.")

except AnalysisException:
    filtered_combined_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(TARGET_TABLE)
    print(f"{TARGET_TABLE} created with initial load of {row_count} records.")

print(f"Rows written this run: {row_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
