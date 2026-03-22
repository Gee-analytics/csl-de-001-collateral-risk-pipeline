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

# # Setup: Pipeline Metadata Table
# **Notebook:** nb_setup_pipeline_metadata  
# **Layer:** Gold  
# **Target Table:** gold_pipeline_metadata  
# 
# ## Purpose
# One-time setup notebook that creates and seeds the gold_pipeline_metadata 
# table with initial watermark values for all 5 incrementally loaded SQL Server 
# tables. This table serves as the watermark store for the Stream A incremental 
# ingestion pipeline. After seeding, this notebook does not need to run again 
# unless the metadata table is dropped or corrupted.
# 
# ## Watermark Columns Per Table
# - collections_officer: DateJoined
# - officer_client_mapping: AssignmentStartDate
# - debtor: DateOnboarded
# - loan: LoanStartDate
# - collateral: CollateralPledgeDate

# CELL ********************

# Imports and setup
# pandas: used to construct the seed data as a DataFrame before converting to Spark
# pyspark functions and types: used for Spark DataFrame operations
# datetime with timezone: generates an accurate UTC timestamp for LastRunTimestamp
import pandas as pd 
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DecimalType, DateType, BooleanType, TimestampType
)
from datetime import datetime, timezone


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Capture the current UTC time to stamp all seed records with a real timestamp
current_time = datetime.now(timezone.utc).isoformat()

# Seed data: one record per incrementally loaded SQL Server table
# LastWatermarkValue is set to the minimum date in each source table
# ensuring the first incremental run pulls all historical records
Metadata_Table = [
    {
        'TableName': 'collections_officer',
        'WatermarkColumn': 'DateJoined',
        'LastWatermarkValue': '2018-11-19',
        'SourceSystem': 'onprem_sqlserver',
        'LastRunTimestamp': current_time,
        'PipelineRunID': 'manual_run_001'
    },
    {
        'TableName': 'officer_client_mapping',
        'WatermarkColumn': 'AssignmentStartDate',
        'LastWatermarkValue': '2019-01-04',
        'SourceSystem': 'onprem_sqlserver',
        'LastRunTimestamp': current_time,
        'PipelineRunID': 'manual_run_001'
    },
    {
        'TableName': 'debtor',
        'WatermarkColumn': 'DateOnboarded',
        'LastWatermarkValue': '2019-01-11',
        'SourceSystem': 'onprem_sqlserver',
        'LastRunTimestamp': current_time,
        'PipelineRunID': 'manual_run_001'
    },
    {
        'TableName': 'loan',
        'WatermarkColumn': 'LoanStartDate',
        'LastWatermarkValue': '2019-02-03',
        'SourceSystem': 'onprem_sqlserver',
        'LastRunTimestamp': current_time,
        'PipelineRunID': 'manual_run_001'
    },
    {
        'TableName': 'collateral',
        'WatermarkColumn': 'CollateralPledgeDate',
        'LastWatermarkValue': '2019-04-01',
        'SourceSystem': 'onprem_sqlserver',
        'LastRunTimestamp': current_time,
        'PipelineRunID': 'manual_run_001'
    }
]

# Convert to pandas DataFrame as an intermediate step before Spark conversion
df = pd.DataFrame(Metadata_Table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert pandas DataFrame to Spark DataFrame for Delta table compatibility
# Spark DataFrames are required for writing to Delta tables in Fabric Lakehouse
spark_df = spark.createDataFrame(df)

# Write to gold_pipeline_metadata Delta table using Overwrite
# Overwrite is correct here because this is a one-time setup operation
# On subsequent pipeline runs, individual rows are updated not overwritten
spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_pipeline_metadata")

# Confirm successful creation
print(f"gold_pipeline_metadata created successfully. Rows written: {spark_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
