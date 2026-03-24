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

# # CSL-DE-001 | Silver Layer Transformation Notebook
# 
# **Notebook:** `nb_silver_transformation`  
# **Project:** Collateral Risk Monitoring & Margin Call Automation System  
# **Organisation:** Collection Solutions Limited (CSL)  
# **Layer:** Silver  
# **Version:** 1.0  
# **Last Updated:** March 2026  
# 
# ---
# 
# ## Purpose
# 
# This notebook transforms raw Bronze layer data into clean, typed, joined, and quality-flagged Silver layer tables.
# 
# It is the most complex transformation notebook in the pipeline. It takes data exactly as it arrived from three source systems and produces five Silver tables that the Gold layer can trust and aggregate from.
# 
# ---
# 
# ## Inputs (Bronze Tables)
# 
# | Bronze Table | Source System |
# |---|---|
# | `bronze_collections_officer` | On-Premises SQL Server |
# | `bronze_officer_client_mapping` | On-Premises SQL Server |
# | `bronze_debtor` | On-Premises SQL Server |
# | `bronze_loan` | On-Premises SQL Server |
# | `bronze_collateral` | On-Premises SQL Server |
# | `bronze_client_bank` | On-Premises SQL Server |
# | `bronze_bank_balance_update` | Amazon S3 (Client Bank Drops) |
# | `bronze_market_prices` | yfinance API |
# 
# ---
# 
# ## Outputs (Silver Tables)
# 
# | Silver Table | Description |
# |---|---|
# | `silver_collections_officer` | Cleaned officer records with phone standardisation and status validation |
# | `silver_officer_client_mapping` | Deduplicated assignment records with SCD Type 2 history |
# | `silver_debtor_loan_collateral` | Pre-joined debtor, loan, and collateral records with quality flags and SCD Type 2 for loan status |
# | `silver_bank_balance_update` | Cleaned balance update files from client banks |
# | `silver_market_prices` | Cleaned and typed market price records per ticker per date |
# 
# ---
# 
# ## Key Design Decisions
# 
# - **Quarantine pattern:** No record is ever silently dropped. Every data quality issue is flagged with a specific flag column. Records remain visible in Silver for investigation and remediation.
# - **`is_eligible_for_ltv` flag:** A single boolean column that the Gold layer uses to filter records before computing LTV ratios. Set to `FALSE` for any record with a critical quality issue.
# - **PII handling:** `NationalID` is SHA-256 hashed at this layer. `PhoneNumber`, `EmailAddress`, and `ResidentialAddress` are passed through in readable form and protected at the Power BI semantic model level via Row Level Security and Object Level Security.
# - **SCD Type 2:** Applied to `silver_debtor_loan_collateral` (loan status changes) and `silver_officer_client_mapping` (assignment changes) to preserve historical state for audit and trend analysis.
# 
# ---
# 
# ## Notebook Structure
# 
# | Section | Description |
# |---|---|
# | Section 1 | Imports and configuration |
# | Section 2 | Read all Bronze tables |
# | Section 3 | Clean and transform `silver_collections_officer` |
# | Section 4 | Clean and transform `silver_officer_client_mapping` |
# | Section 5 | Clean and transform `silver_debtor_loan_collateral` |
# | Section 6 | Clean and transform `silver_bank_balance_update` |
# | Section 7 | Clean and transform `silver_market_prices` |
# | Section 8 | Write all Silver tables to Delta |
# | Section 9 | Log results to `gold_pipeline_metadata` |


# MARKDOWN ********************

# ---
# 
# ## Section 1: Imports and Configuration
# 
# This section imports all libraries needed across the entire notebook and defines the shared configuration variables used in every subsequent section.

# MARKDOWN ********************

# - ## Step 1.1 - Import all required libraries
# Imports all PySpark, Delta Lake, and standard library modules required across the entire notebook.  
# All imports are declared here once rather than scattered across sections.

# CELL ********************

# --- Core PySpark imports ---
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DecimalType, DateType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

# --- Delta Lake import for SCD Type 2 merge operations ---
from delta.tables import DeltaTable

# --- Standard library ---
from datetime import datetime, timezone
# (Universally Unique Identifier) a 128-bit label 
# used to uniquely identify information in computer systems without a central registration authority.)
import uuid  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# - ## Step 1.2 - Define configuration variables 
# (paths, run ID, timestamp)
# Defines the shared constants used throughout the notebook:  
# - `PIPELINE_RUN_ID` - a unique UUID generated fresh each run, used to trace every Silver row back to the execution that produced it  
# - `SILVER_INGESTION_TIMESTAMP` - the UTC timestamp of this run  
# - `BRONZE_PATH` and `SILVER_PATH` - Lakehouse table path prefixes


# CELL ********************

# Unique identifier for this notebook run.
# Every Silver table written in this session will carry this ID.
# This is what lets you trace a specific row back to a specific pipeline execution.
PIPELINE_RUN_ID = str(uuid.uuid4())

# Timestamp for this run. Applied as silver_ingestion_timestamp across all tables.
SILVER_INGESTION_TIMESTAMP = datetime.now(timezone.utc)

# Lakehouse table path prefix.
# Fabric resolves this relative to the attached Lakehouse (CSL_Collateral_Risk_LH).
BRONZE_PATH = "Tables/dbo"    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - ## Step 1.3 - Define the pipeline metadata logging helper function
# Defines `log_pipeline_metadata()`, a reusable function called at the end of each section to record row counts, status, and notes into `gold_pipeline_metadata`.  
# This is the audit trail for the Silver layer.


# CELL ********************

# ============================================================
# LOGGING HELPER
# ============================================================
# This function is called at the end of each section to record
# what was processed. It writes one row to gold_pipeline_metadata
# per Silver table produced.

def log_pipeline_metadata(table_name, rows_in, rows_out, status, notes=""):
    """
    This logs a single pipeline run record for one Silver table.

    Args:
        table_name  : Name of the Silver table being logged
        rows_in     : Row count read from Bronze
        rows_out    : Row count written to Silver
        status      : 'SUCCESS' or 'FAILED'
        notes       : Optional notes e.g. quarantine counts
    """
    log_row = spark.createDataFrame([{
        "pipeline_run_id"  : PIPELINE_RUN_ID,
        "notebook_name"    : "nb_silver_transformation",
        "table_name"       : table_name,
        "layer"            : "silver",
        "rows_in"          : rows_in,
        "rows_out"         : rows_out,
        "status"           : status,
        "notes"            : notes,
        "run_timestamp"    : SILVER_INGESTION_TIMESTAMP
    }])

    log_row.write.format("delta").mode("append").save("Tables/gold/gold_pipeline_metadata")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - ## Step 1.4 - Print confirmation that configuration is loaded
# Prints all config values to confirm the section loaded without errors before proceeding.

# CELL ********************

# ============================================================
# CONFIRMATION
# ============================================================

print(f"Pipeline Run ID  : {PIPELINE_RUN_ID}")
print(f"Ingestion TS     : {SILVER_INGESTION_TIMESTAMP}")
print(f"Bronze path      : {BRONZE_PATH}")
print("Section 1 complete. Ready to read Bronze tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2: Read All Bronze Tables
# 
# Reads all 8 Bronze Delta tables into Spark DataFrames.
# No transformations are applied here. This section is purely data loading.
# Row counts are printed for each table to establish a baseline for reconciliation against Silver output counts.
# 
# ### Steps
# - **Step 2.1** - Read SQL Server sourced Bronze tables (6 tables)
# - **Step 2.2** - Read S3 sourced Bronze table (1 table)
# - **Step 2.3** - Read API sourced Bronze table (1 table)
# - **Step 2.4** - Print row counts for all 8 tables

# CELL ********************

# ============================================================
# SECTION 2: READ ALL BRONZE TABLES
# ============================================================

# --- Step 2.1: SQL Server sourced Bronze tables ---

df_bronze_collections_officer = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_collections_officer")
df_bronze_officer_client_mapping = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_officer_client_mapping")
df_bronze_debtor = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_debtor")
df_bronze_loan = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_loan")
df_bronze_collateral = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_collateral")
df_bronze_client_bank = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_client_bank")

# --- Step 2.2: S3 sourced Bronze table ---

df_bronze_bank_balance_update = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_bank_balance_update")

# --- Step 2.3: API sourced Bronze table ---

df_bronze_market_prices = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_market_prices")

# --- Step 2.4: Print row counts for baseline reconciliation ---

bronze_counts = {
    "bronze_collections_officer"  : df_bronze_collections_officer.count(),
    "bronze_officer_client_mapping": df_bronze_officer_client_mapping.count(),
    "bronze_debtor"               : df_bronze_debtor.count(),
    "bronze_loan"                 : df_bronze_loan.count(),
    "bronze_collateral"           : df_bronze_collateral.count(),
    "bronze_client_bank"          : df_bronze_client_bank.count(),
    "bronze_bank_balance_update"  : df_bronze_bank_balance_update.count(),
    "bronze_market_prices"        : df_bronze_market_prices.count(),
}

print("=" * 50)
print("BRONZE TABLE ROW COUNTS")
print("=" * 50)
for table, count in bronze_counts.items():
    print(f"  {table:<40} {count:>6} rows")
print("=" * 50)
print("Section 2 complete. All Bronze tables loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --
# ## Section 3: Clean and Transform `silver_collections_officer`
# 
# Produces `silver_collections_officer` from `bronze_collections_officer`.
# Kept separate because it serves Row Level Security independently at the Power BI semantic model level.
# 
# All data quality issues are captured in a single composite `data_quality_flag` column.
# Records with issues are retained and flagged. Nothing is silently dropped.
# 
# ### Steps
# - **Step 3.1** - Standardise PhoneNumber to +234XXXXXXXXXX format
# - **Step 3.2** - Clean Email and flag suspect email addresses
# - **Step 3.3** - Validate Status against allowed values
# - **Step 3.4** - Handle NULL DateJoined with sentinel value and cast to DATE
# - **Step 3.5** - Build composite data_quality_flag column
# - **Step 3.6** - Add audit columns
# - **Step 3.7** - Select and order final columns
# - **Step 3.8** - Print summary counts

# CELL ********************

# ============================================================
# SECTION 3: CLEAN AND TRANSFORM silver_collections_officer
# ============================================================

# Start from the raw Bronze DataFrame
df_officer = df_bronze_collections_officer

# --- Step 3.1: Standardise PhoneNumber to +234XXXXXXXXX format ---
# Profiling finding: OFF-0005 has 08012345678 (local format)
#                   OFF-0006 has 234-801-234-5678 (malformed with hyphens)
# Diagnostic confirmed: clean records have +234 followed by 9 digits.
# Nigerian mobile numbers are 10 digits including leading zero.
# Stripping the leading zero leaves 9 digits after the country code.
# IMPORTANT: Invalid phone numbers are retained as-is and flagged.
# Setting them to NULL would destroy the original value and violate
# the quarantine pattern. The flag is sufficient to prevent misuse.

df_officer = df_officer.withColumn(
    "PhoneNumber_clean",
    F.when(
        # Already correct: +234 followed by exactly 9 digits
        F.col("PhoneNumber").rlike("^\\+234[0-9]{9}$"),
        F.col("PhoneNumber")
    ).when(
        # Local format: 0 followed by exactly 10 digits
        # Strip leading 0, prepend +234
        F.col("PhoneNumber").rlike("^0[0-9]{10}$"),
        F.concat(F.lit("+234"), F.substring(F.col("PhoneNumber"), 2, 10))
    ).when(
        # Missing + prefix: 234 followed by exactly 9 digits
        F.col("PhoneNumber").rlike("^234[0-9]{9}$"),
        F.concat(F.lit("+"), F.col("PhoneNumber"))
    ).otherwise(
        # Cannot be standardised - retain original value, flag below
        F.col("PhoneNumber")
    )
).withColumn(
    "PhoneNumber_flag",
    F.when(
        F.col("PhoneNumber").rlike("^\\+234[0-9]{9}$"),
        F.lit("CLEAN")
    ).when(
        F.col("PhoneNumber").rlike("^0[0-9]{10}$"),
        F.lit("PHONE_STANDARDISED")
    ).when(
        F.col("PhoneNumber").rlike("^234[0-9]{9}$"),
        F.lit("PHONE_STANDARDISED")
    ).otherwise(
        F.lit("PHONE_INVALID")
    )
).drop("PhoneNumber").withColumnRenamed("PhoneNumber_clean", "PhoneNumber")

# Quick check - print phone flag distribution
print("Step 3.1 complete - PhoneNumber flag distribution:")
df_officer.groupBy("PhoneNumber_flag").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 3.2: Clean Email and flag suspect email addresses ---
# Profiling finding: OFF-0007 has trailing space in email
#                   OFF-0008 has email that appears to belong to a different person
# All emails are trimmed and lowercased unconditionally.
# Suspect email heuristic: if neither the officer's first name nor last name
# appears anywhere in their email address, flag as SUSPECT_EMAIL.
# We cannot correct a suspect email. We flag and retain for HR escalation.

df_officer = df_officer.withColumn(
    "Email",
    F.lower(F.trim(F.col("Email")))
)

# Split FullName into first and last name for the suspect email check.
# F.split returns an array. Index 0 is first name, index -1 is last name.
df_officer = df_officer.withColumn(
    "first_name_lower",
    F.lower(F.split(F.col("FullName"), " ")[0])
).withColumn(
    "last_name_lower",
    F.lower(F.split(F.col("FullName"), " ")[F.size(F.split(F.col("FullName"), " ")) - 1])
)

# Flag as SUSPECT_EMAIL if neither first nor last name appears in the email.
df_officer = df_officer.withColumn(
    "Email_flag",
    F.when(
        F.col("Email").isNull(),
        F.lit("MISSING_EMAIL")
    ).when(
        ~(
            F.col("Email").contains(F.col("first_name_lower")) |
            F.col("Email").contains(F.col("last_name_lower"))
        ),
        F.lit("SUSPECT_EMAIL")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Drop the helper columns - they were only needed for the check
df_officer = df_officer.drop("first_name_lower", "last_name_lower")

# Quick check - print email flag distribution and the suspect record
print("Step 3.2 complete - Email flag distribution:")
df_officer.groupBy("Email_flag").count().show()

print("Suspect email records:")
df_officer.filter(F.col("Email_flag") == "SUSPECT_EMAIL").select(
    "OfficerID", "FullName", "Email", "Email_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 3.3: Validate Status against allowed values ---
# Profiling finding: Suspended was confirmed as a valid status during profiling.
# Valid domain: Active, Inactive, Suspended.
# Anything outside this set is flagged INVALID_STATUS and retained.
# We never overwrite an unrecognised status. That is a business decision,
# not a pipeline decision.

VALID_STATUSES = ["Active", "Inactive", "Suspended"]

df_officer = df_officer.withColumn(
    "Status_flag",
    F.when(
        F.col("Status").isin(VALID_STATUSES),
        F.lit("CLEAN")
    ).otherwise(
        F.lit("INVALID_STATUS")
    )
)

# Quick check - print status distribution alongside flag
print("Step 3.3 complete - Status and flag distribution:")
df_officer.groupBy("Status", "Status_flag").count().orderBy("Status").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# --- Step 3.4: Handle NULL DateJoined with sentinel value and cast to DATE ---
# Profiling finding: 2 NULLs on OFF-0009 and OFF-0010, both Active status.
# Order of operations is critical here:
#   1. Flag first - check for NULL before replacing it
#   2. Replace NULL with sentinel 1900-01-01
#   3. Cast to DATE
# Sentinel value 1900-01-01 signals a known missing date without breaking
# downstream date calculations the way a NULL would.

df_officer = df_officer.withColumn(
    "DateJoined_flag",
    F.when(
        F.col("DateJoined").isNull(),
        F.lit("MISSING_JOIN_DATE")
    ).otherwise(
        F.lit("CLEAN")
    )
).withColumn(
    "DateJoined",
    F.when(
        F.col("DateJoined").isNull(),
        F.lit("1900-01-01")
    ).otherwise(
        F.col("DateJoined")
    )
).withColumn(
    "DateJoined",
    F.to_date(F.col("DateJoined"), "yyyy-MM-dd")
)

# Quick check - flag distribution and confirm sentinel records
print("Step 3.4 complete - DateJoined flag distribution:")
df_officer.groupBy("DateJoined_flag").count().show()

print("Sentinel date records:")
df_officer.filter(F.col("DateJoined_flag") == "MISSING_JOIN_DATE").select(
    "OfficerID", "FullName", "DateJoined", "DateJoined_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 3.5: Build composite data_quality_flag column ---
# Combines all individual flag columns into a single column.
# Records with no issues carry CLEAN.
# Records with one or more issues carry pipe-separated issue codes.
# Example: SUSPECT_EMAIL|MISSING_JOIN_DATE
# Individual flag columns are dropped after the composite is built.
# This keeps the Silver table schema clean and queryable from one column.

df_officer = df_officer.withColumn(
    "data_quality_flag",
    F.when(
        # Build array of all non-CLEAN flags and check if any exist
        F.size(
            F.array_remove(
                F.array(
                    F.col("PhoneNumber_flag"),
                    F.col("Email_flag"),
                    F.col("Status_flag"),
                    F.col("DateJoined_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        # All flags are CLEAN
        F.lit("CLEAN")
    ).otherwise(
        # Join non-CLEAN flags with pipe separator
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("PhoneNumber_flag"),
                    F.col("Email_flag"),
                    F.col("Status_flag"),
                    F.col("DateJoined_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop("PhoneNumber_flag", "Email_flag", "Status_flag", "DateJoined_flag")

# Quick check - show composite flag distribution
print("Step 3.5 complete - Composite data_quality_flag distribution:")
df_officer.groupBy("data_quality_flag").count().orderBy("count", ascending=False).show(truncate=False)

# Show the flagged records in full
print("Flagged records:")
df_officer.filter(F.col("data_quality_flag") != "CLEAN").select(
    "OfficerID", "FullName", "data_quality_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 3.6: Add audit columns ---
# silver_ingestion_timestamp and pipeline_run_id are defined in Section 1.
# Applied consistently to every Silver table in this notebook.

df_officer = df_officer.withColumn(
    "silver_ingestion_timestamp", F.lit(SILVER_INGESTION_TIMESTAMP).cast(TimestampType())
).withColumn(
    "pipeline_run_id", F.lit(PIPELINE_RUN_ID)
)

# --- Step 3.7: Select and order final columns ---
# Explicit column selection ensures no intermediate helper columns
# leak into the Silver table. Column order follows the convention:
# primary key first, business columns, flag column, audit columns last.

df_silver_collections_officer = df_officer.select(
    "OfficerID",
    "FullName",
    "Email",
    "PhoneNumber",
    "Status",
    "DateJoined",
    "TeamLeadOfficerID",
    "data_quality_flag",
    "ingestion_timestamp",
    "source_system",
    "pipeline_run_id",
    "silver_ingestion_timestamp"
)

# --- Step 3.8: Print summary counts ---

total            = df_silver_collections_officer.count()
clean            = df_silver_collections_officer.filter(F.col("data_quality_flag") == "CLEAN").count()
flagged          = df_silver_collections_officer.filter(F.col("data_quality_flag") != "CLEAN").count()
phone_std        = df_silver_collections_officer.filter(F.col("data_quality_flag").contains("PHONE_STANDARDISED")).count()
phone_invalid    = df_silver_collections_officer.filter(F.col("data_quality_flag").contains("PHONE_INVALID")).count()
suspect_email    = df_silver_collections_officer.filter(F.col("data_quality_flag").contains("SUSPECT_EMAIL")).count()
missing_date     = df_silver_collections_officer.filter(F.col("data_quality_flag").contains("MISSING_JOIN_DATE")).count()

print("=" * 55)
print("silver_collections_officer SUMMARY")
print("=" * 55)
print(f"  Total records              : {total}")
print(f"  Clean records              : {clean}")
print(f"  Flagged records            : {flagged}")
print(f"  Phone standardised         : {phone_std}")
print(f"  Phone invalid              : {phone_invalid}")
print(f"  Suspect email              : {suspect_email}")
print(f"  Missing join date          : {missing_date}")
print("=" * 55)
print("Section 3 complete. df_silver_collections_officer ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_officer.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_officer1 = df_bronze_collections_officer
df_officer1.show(22)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_officer.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

b_collections_officer = df_bronze_collections_officer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

b_collections_officer.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

"""
b_collections_officer = b_collections_officer.withColumn(
    # strip the leading and the trailing spaces
    # convert every value in the col to lowercase 
    "Email", F.lower(F.trim(F.col("Email")))
)
"""
# convert every value in the col to lowercase 
b_collections_officer = b_collections_officer.withColumn("Email", F.lower("Email"))

# strip the leading and the trailing spaces
b_collections_officer = b_collections_officer.withColumn("Email", F.trim("Email"))


# Apply BOTH transformations to the column in a single pass
b_collections_officer = b_collections_officer.withColumn("Email", F.trim(F.lower("Email")))



    # strip the leading and the trailing spaces
    # convert every value in the col to lowercase





# Split FullName into first and last name for the suspect email check.
# F.split returns an array. Index 0 is first name, index -1 is last name.
df_officer = df_officer.withColumn(
    "first_name_lower",
    F.lower(F.split(F.col("FullName"), " ")[0])
).withColumn(
    "last_name_lower",
    F.lower(F.split(F.col("FullName"), " ")[F.size(F.split(F.col("FullName"), " ")) - 1])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Diagnostic: show all raw PhoneNumber values from Bronze#
# df_bronze_collections_officer.select("OfficerID", "PhoneNumber").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
