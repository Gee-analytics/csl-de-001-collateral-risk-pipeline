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

# 
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
import datetime 
from functools import reduce
from pyspark.sql import DataFrame

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

# ## Step 2: Read Parquet Files from S3 Client Folders
# Each client folder is read individually so we can capture the source filename 
# per record before combining. Reading folder-by-folder also makes it easy to 
# add per-client logic in future without restructuring the notebook.

# CELL ********************

# This empty list will hold the parquet file from each client folder
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

# ## Step 3: Union All Client DataFrames
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

# ## Step 4: Cast Timestamp Columns to Standard Timestamp Type
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

# ## Step 5: Add Metadata Columns
# Three audit columns are stamped onto every record at ingestion time. 
# ingestion_timestamp records exactly when this pipeline ran. source_system 
# identifies the origin of this data for lineage tracking. pipeline_run_id 
# allows any batch of records to be traced back to a specific pipeline execution.

# CELL ********************


# Add metadata columns 
combined_df = (
combined_df.withColumn('ingestion_timestamp', F.current_timestamp())
.withColumn('source_system', F.lit(SOURCE_SYSTEM))
.withColumn('pipeline_run_id', F.lit(PIPELINE_RUN_ID))

)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Extract Filename from Full ABFSS Path
# input_file_name() returns the full internal OneLake path. We extract only the 
# client-drops/CLT-XXX/filename.parquet portion to keep the SourceFileName column 
# clean, readable, and traceable back to both the client and the reporting date.

# CELL ********************

# Extract just the filename from the full ABFSS path
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

# ## Step 7: Schema Verification
# Print the full schema to confirm all columns are present with the correct 
# data types before writing to the Delta table. This is a pre-write validation 
# checkpoint to catch any type issues early.

# CELL ********************

combined_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Sample Data Preview
# Display the first 5 rows for a visual sanity check. Confirms that 
# SourceFileName is clean, timestamps are properly formatted, and metadata 
# columns are populated correctly.

# CELL ********************

display(combined_df.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9: Final Dimension Check
# Confirm the final row and column count matches the expected values before 
# writing. Expected: 247 rows, 13 columns. This is the last validation 
# checkpoint before the write operation.

# CELL ********************

print(f"Total rows read: {combined_df.count()}")
print(f"Total columns: {len(combined_df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 10: Write to Bronze Delta Table
# Write the final DataFrame to the bronze_bank_balance_update Delta table 
# using Overwrite mode. Overwrite is appropriate here because this is a full 
# daily snapshot from the client banks. Every run reflects the complete current 
# state of all client balance files present in S3.

# CELL ********************

#Write the result to bronze_bank_balance_update Delta table using Overwrite
combined_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_bank_balance_update")


#Print a confirmation message showing how many rows were written
print(f"bronze_bank_balance_update loaded successfully. Rows written: {combined_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
