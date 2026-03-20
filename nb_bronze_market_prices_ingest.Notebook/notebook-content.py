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

# # Stream C: Bronze Market Price Ingestion
# **Notebook:** nb_bronze_market_prices_ingest  
# **Layer:** Bronze  
# **Source:** Yahoo Finance via yfinance Python library  
# **Target Table:** bronze_market_prices  
# 
# ## Purpose
# This notebook ingests daily closing prices for CSL collateral ticker symbols 
# from Yahoo Finance. 
# It implements incremental loading logic to ensure only 
# dates not yet present in the Bronze table are fetched on each run, avoiding 
# duplicate records and unnecessary API calls.
# 
# ## Tickers
# AAPL, TSLA, GOOGL, MSFT, AMZN, NVDA, BTC-USD
# 
# ## Metadata Columns Added
# - ingestion_timestamp: exact datetime this notebook ran
# - source_system: yfinance_api
# - pipeline_run_id: identifies the specific pipeline execution batch

# MARKDOWN ********************

# ## Step 1: Imports, Downloads and configurations

# CELL ********************

# yfinance is not pre-installed in Fabric Spark environments.
# The code below is used to install it.
%pip install yfinance

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stream C: Yahoo Finance Market Price Ingestion
# This notebook fetches daily closing prices for CSL collateral ticker symbols
# from Yahoo Finance using the yfinance library.
# Results are written to the bronze_market_prices Delta table.
# Incremental logic ensures only dates not yet loaded are fetched on each run.

from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
import yfinance as yf
from pyspark.sql.utils import AnalysisException

# Configuration
TICKER_SYMBOLS = ["AAPL", "TSLA", "GOOGL", "MSFT", "AMZN", "NVDA", "BTC-USD"]
TARGET_TABLE = "bronze_market_prices"
SOURCE_SYSTEM = "yfinance_api"
PIPELINE_RUN_ID = "manual_run_001"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Watermark Check
# Check the maximum PriceDate already loaded in bronze_market_prices.
# If the table does not exist or is empty, default to 1 year ago from today.
# This ensures only new dates are fetched on each run, avoiding duplicate 
# records and unnecessary API calls.

# CELL ********************

# Determine the watermark start date for the API call
end_date = datetime.now().strftime('%Y-%m-%d')



try:
    # Find the last date we successfully loaded
    # collect()[0][0]: reaches into the 1x1 Spark result and extracts the actual value
    max_date = spark.table(TARGET_TABLE).select(F.max("PriceDate")).collect()[0][0]

    #if the max_date is empty or none, then output the message table is empty
    if max_date is None:
        raise ValueError("Table is empty")

    # Advance by one day to avoid re-fetching the last loaded date
    start_date = (max_date + timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Table found. Last loaded date: {max_date}. Fetching from: {start_date}")

except (AnalysisException, ValueError) as e:
    print(f"Notice: {e}. Calculating dynamic start date (12 months ago).")
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    print(f"Starting first load from: {start_date}")

    

# Check if there is anything new to fetch
if start_date >= end_date:
   # print("Bronze table is already up to date. No new data to fetch.")
    raise SystemExit("Bronze table is already up to date. No new data to fetch.")

print(f"Watermark: fetching data from {start_date} to {end_date}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Fetch and Flatten Market Price Data
# Using the yfinance library, download daily OHLCV prices for all CSL collateral 
# tickers from the watermark date to today. yfinance returns a MultiIndex DataFrame 
# with one column per metric per ticker. The stack() operation flattens this into 
# a clean tabular structure with one row per ticker per date, which is the format 
# required for the Bronze Delta table.

# CELL ********************

# fetch thestock price data 
stock_data = yf.download(
    tickers=TICKER_SYMBOLS,
    start=start_date, # refer to the step 2 for the logic for the start_date
    end=end_date,  # refer to step 2
    interval="1d"
)

# Flatten the MultiIndex column structure into one row per ticker per date
flat_df = stock_data.stack(level=1).reset_index()

# Rename columns to match Bronze schema
flat_df = flat_df.rename(columns={
    'Date': 'PriceDate',
    'Ticker': 'TickerSymbol'
})



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Confirm shape and sample
print(f"Data fetched and flattened. Shape: {flat_df.shape}")
print(f"Columns: {flat_df.columns.tolist()}")
print(f"Date range: {flat_df['PriceDate'].min()} to {flat_df['PriceDate'].max()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Convert to Spark DataFrame and Add Metadata Columns
# The pandas DataFrame is converted to a Spark DataFrame to enable distributed 
# processing and compatibility with the Fabric Delta table write operation. 
# Metadata columns are then stamped onto every record at ingestion time to 
# support audit trail and data lineage tracking.

# CELL ********************

# Convert the flattened pandas DataFrame to a Spark DataFrame
# for distributed processing and Delta table compatibility
spark_stock_data_df = spark.createDataFrame(flat_df)

# Validate the conversion with a sample and schema check
spark_stock_data_df.show(5)
spark_stock_data_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stamp audit metadata onto every record at ingestion time
# ingestion_timestamp: when this notebook ran
# source_system: identifies yfinance as the data origin
# pipeline_run_id: traces this batch back to a specific pipeline execution
spark_stock_data_df = (
    spark_stock_data_df.withColumn('ingestion_timestamp', F.current_timestamp())
                       .withColumn('source_system', F.lit(SOURCE_SYSTEM))
                       .withColumn('pipeline_run_id', F.lit(PIPELINE_RUN_ID))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Confirm all columns are present including the three metadata columns
spark_stock_data_df.printSchema()

# Visual sample to verify data quality before writing to Delta table
display(spark_stock_data_df.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_stock_data_df.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Write to Bronze Delta Table
# Write the final DataFrame to the bronze_market_prices Delta table.
# On the first run, the table does not exist so it is created with a full 
# initial load covering one year of historical prices. On subsequent runs, 
# only records with a PriceDate newer than the watermark are appended.
# This pattern ensures no duplicate price records and no unnecessary API calls.
# If the table is already up to date, the notebook exits cleanly before reaching 
# this step.

# CELL ********************


# Check if the target table exists and write accordingly
# If the table does not exist, create it. 
try:
    spark.table(TARGET_TABLE)
    spark_stock_data_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)
    print(f"{TARGET_TABLE} exists. Appended new records.")
# If it does exist, append new records only.
except AnalysisException:
    spark_stock_data_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(TARGET_TABLE)
    print(f"{TARGET_TABLE} created with initial load.")

print(f"Rows written this run: {spark_stock_data_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

