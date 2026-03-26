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

# ### Step 1.1 - Import all required libraries
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

# MARKDOWN ********************

# ---
# ## Section 4: Clean and Transform `silver_officer_client_mapping`
# 
# Produces `silver_officer_client_mapping` from `bronze_officer_client_mapping`.
# Kept separate because it drives Row Level Security mapping logic independently.
# SCD Type 2 is applied here because officer assignment history is a business fact
# that must be preserved at Silver, the system of record for this Lakehouse.
# 
# ### Steps
# - **Step 4.1** - Deduplicate on OfficerID + ClientID keeping latest AssignmentStartDate
# - **Step 4.2** - Correct IsActive where AssignmentEndDate is in the past
# - **Step 4.3** - Join officer status and flag inactive officer mappings
# - **Step 4.4** - Cast data types
# - **Step 4.5** - Rename AssignmentStartDate and AssignmentEndDate to EffectiveStartDate and EffectiveEndDate
# - **Step 4.6** - Add IsCurrent boolean
# - **Step 4.7** - Generate surrogate key mapping_sk
# - **Step 4.8** - Build composite data_quality_flag
# - **Step 4.9** - Add audit columns
# - **Step 4.10** - Select and order final columns
# - **Step 4.11** - Print summary counts


# CELL ********************

# ============================================================
# SECTION 4: CLEAN AND TRANSFORM silver_officer_client_mapping
# ============================================================

# Start from the raw Bronze DataFrame
df_mapping = df_bronze_officer_client_mapping

# --- Step 4.1: Deduplicate on OfficerID + ClientID ---
# Profiling finding: 3 duplicate composite key combinations.
# OFF-0001 mapped twice each to CLT-001, CLT-002, and CLT-003.
# Deduplication rule: keep the record with the latest AssignmentStartDate.
# If AssignmentStartDate is equal, keep the first record encountered.
# Removed duplicates are flagged not silently dropped.
# This must run before SCD Type 2 logic to prevent fabricated history rows.

from pyspark.sql.window import Window

# First add a flag to identify duplicates before removing them
window_dup = Window.partitionBy("OfficerID", "ClientID").orderBy(
    F.col("AssignmentStartDate").desc()
)

df_mapping = df_mapping.withColumn(
    "row_rank", F.row_number().over(window_dup)
).withColumn(
    "IsDuplicate_flag",
    F.when(F.col("row_rank") == 1, F.lit("CLEAN"))
    .otherwise(F.lit("DUPLICATE_REMOVED"))
)

# Count duplicates before removing for logging
duplicate_count = df_mapping.filter(
    F.col("IsDuplicate_flag") == "DUPLICATE_REMOVED"
).count()

# Keep only the first ranked record per OfficerID + ClientID
df_mapping = df_mapping.filter(F.col("row_rank") == 1).drop("row_rank")

# Quick check
print(f"Step 4.1 complete - Duplicates removed: {duplicate_count}")
print(f"Rows after deduplication: {df_mapping.count()}")
print("\nIsDuplicate_flag distribution:")
df_mapping.groupBy("IsDuplicate_flag").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.2: Correct IsActive where AssignmentEndDate is in the past ---
# Profiling finding: 4 rows have AssignmentEndDate populated with a past date
# but IsActive = True. Logical contradiction.
# Schema check confirmed IsActive is BOOLEAN and AssignmentEndDate is DATE.
# Correction rule: where AssignmentEndDate is not null and before today,
# set IsActive to False and flag as CORRECTED.

df_mapping = df_mapping.withColumn(
    "IsActive_corrected",
    F.when(
        (F.col("AssignmentEndDate").isNotNull()) & (F.col("AssignmentEndDate") < F.current_date()) & (F.col("IsActive") == True),
        F.lit(False)
    ).otherwise(
        F.col("IsActive")
    )
).withColumn(
    "IsActive_flag",
    F.when(
        (F.col("AssignmentEndDate").isNotNull()) &
        (F.col("AssignmentEndDate") < F.current_date()) &
        (F.col("IsActive") == True),
        F.lit("CORRECTED")
    ).otherwise(
        F.lit("CLEAN")
    )
).drop("IsActive").withColumnRenamed("IsActive_corrected", "IsActive")

# Quick check
print("Step 4.2 complete - IsActive flag distribution:")
df_mapping.groupBy("IsActive_flag").count().show()

print("Corrected records:")
df_mapping.filter(F.col("IsActive_flag") == "CORRECTED").select(
    "MappingID", "OfficerID", "ClientID", "AssignmentEndDate", "IsActive", "IsActive_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.3: Join officer status and flag inactive officer mappings ---
# Profiling finding: 9 mapping records belong to Inactive or Suspended officers
# who still have active portfolio assignments. Operational risk.
# We join to bronze_collections_officer to bring in current officer status.
# We flag affected records but do not delete them. Portfolio reassignment
# is a business decision, not a pipeline decision.

# Bring in only OfficerID and Status from the officer table
df_officer_status = df_bronze_collections_officer.select(
    "OfficerID",
    F.col("Status").alias("officer_status_at_load")
)

# Left join to preserve all mapping records even if officer lookup fails
df_mapping = df_mapping.join(
    df_officer_status,
    on="OfficerID",
    how="left"
)

# Flag mappings where officer is not Active
df_mapping = df_mapping.withColumn(
    "OfficerStatus_flag",
    F.when(
        F.col("officer_status_at_load") == "Active",
        F.lit("CLEAN")
    ).when(
        F.col("officer_status_at_load").isNull(),
        F.lit("OFFICER_NOT_FOUND")
    ).otherwise(
        F.lit("INACTIVE_OFFICER_MAPPING")
    )
)

# Quick check
print("Step 4.3 complete - OfficerStatus flag distribution:")
df_mapping.groupBy("officer_status_at_load", "OfficerStatus_flag").count().orderBy("count", ascending=False).show()

print("Inactive officer mapping records:")
df_mapping.filter(F.col("OfficerStatus_flag") == "INACTIVE_OFFICER_MAPPING").select(
    "MappingID", "OfficerID", "ClientID", "officer_status_at_load", "OfficerStatus_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.4: Cast data types ---
# Schema check confirmed all columns are already correctly typed.
# Fabric inferred types correctly during Bronze Delta write:
#   AssignmentStartDate - DATE
#   AssignmentEndDate   - DATE
#   IsActive            - BOOLEAN
#   ingestion_timestamp - TIMESTAMP
# No casting required for this table.
# Step retained for documentation purposes and pipeline consistency.

print("Step 4.4 complete - No type casting required.")
print("All column types confirmed correct from schema check.")
df_mapping.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.5: Rename date columns to SCD Type 2 convention ---
# AssignmentStartDate and EffectiveStartDate carry identical meaning here.
# Renaming aligns this table with the SCD Type 2 pattern used across Silver.
# No data duplication needed. Rename is the correct and cleaner approach.

df_mapping = df_mapping \
    .withColumnRenamed("AssignmentStartDate", "EffectiveStartDate") \
    .withColumnRenamed("AssignmentEndDate", "EffectiveEndDate")

# Quick check - confirm rename worked
print("Step 4.5 complete - Column rename confirmed:")
df_mapping.select(
    "MappingID", "OfficerID", "ClientID",
    "EffectiveStartDate", "EffectiveEndDate"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.6: Add IsCurrent boolean ---
# IsCurrent = True where EffectiveEndDate is NULL (assignment still open).
# IsCurrent = False where EffectiveEndDate is populated (assignment ended).
# This is a convenience column for downstream filtering.
# It carries no new information beyond EffectiveEndDate but makes
# queries and RLS logic simpler and less error prone.

df_mapping = df_mapping.withColumn(
    "IsCurrent",
    F.when(F.col("EffectiveEndDate").isNull(), F.lit(True))
    .otherwise(F.lit(False))
)

# Quick check - IsCurrent distribution
print("Step 4.6 complete - IsCurrent distribution:")
df_mapping.groupBy("IsCurrent").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.7: Generate surrogate key mapping_sk ---
# The natural key OfficerID + ClientID is not unique in an SCD Type 2 table
# because multiple versions of the same assignment can exist.
# The surrogate key uniquely identifies each individual row version.
# Generated as SHA-256 hash of OfficerID + ClientID + EffectiveStartDate.
# Deterministic: the same input always produces the same hash.
# This ensures key stability across pipeline reruns.

df_mapping = df_mapping.withColumn(
    "mapping_sk",
    F.sha2(
        F.concat_ws("|",
            F.col("OfficerID"),
            F.col("ClientID"),
            F.col("EffectiveStartDate").cast("string")
        ),
        256
    )
)

# Quick check - confirm uniqueness of surrogate key
total_rows = df_mapping.count()
distinct_keys = df_mapping.select("mapping_sk").distinct().count()

print("Step 4.7 complete - Surrogate key generation:")
print(f"  Total rows       : {total_rows}")
print(f"  Distinct keys    : {distinct_keys}")
print(f"  Keys unique      : {total_rows == distinct_keys}")
print("\nSample surrogate keys:")
df_mapping.select(
    "MappingID", "OfficerID", "ClientID", "EffectiveStartDate", "mapping_sk"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.8: Build composite data_quality_flag column ---
# Combines IsDuplicate_flag, IsActive_flag, and OfficerStatus_flag
# into a single composite column following the same pattern as Section 3.
# Individual flag columns are dropped after the composite is built.

df_mapping = df_mapping.withColumn(
    "data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("IsActive_flag"),
                    F.col("OfficerStatus_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("IsActive_flag"),
                    F.col("OfficerStatus_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop("IsDuplicate_flag", "IsActive_flag", "OfficerStatus_flag")

# Quick check - composite flag distribution
print("Step 4.8 complete - Composite data_quality_flag distribution:")
df_mapping.groupBy("data_quality_flag").count().orderBy("count", ascending=False).show(truncate=False)

print("Flagged records:")
df_mapping.filter(F.col("data_quality_flag") != "CLEAN").select(
    "MappingID", "OfficerID", "ClientID", "data_quality_flag"
).orderBy("MappingID").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.9: Add audit columns ---
# silver_ingestion_timestamp and pipeline_run_id defined in Section 1.
# Applied consistently to every Silver table in this notebook.

df_mapping = df_mapping.withColumn(
    "silver_ingestion_timestamp", F.lit(SILVER_INGESTION_TIMESTAMP).cast(TimestampType())
).withColumn(
    "pipeline_run_id", F.lit(PIPELINE_RUN_ID)
)

print("Step 4.9 complete - Audit columns added.")
print(f"  pipeline_run_id            : {PIPELINE_RUN_ID}")
print(f"  silver_ingestion_timestamp : {SILVER_INGESTION_TIMESTAMP}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.10: Select and order final columns ---
# Explicit column selection ensures no intermediate helper columns
# leak into the Silver table.
# Column order convention:
#   Surrogate key first, natural keys, business columns,
#   SCD Type 2 columns, derived columns, flag column, audit columns last.

df_silver_officer_client_mapping = df_mapping.select(
    "mapping_sk",
    "MappingID",
    "OfficerID",
    "ClientID",
    "Region",
    "IsActive",
    "officer_status_at_load",
    "EffectiveStartDate",
    "EffectiveEndDate",
    "IsCurrent",
    "data_quality_flag",
    "ingestion_timestamp",
    "source_system",
    "pipeline_run_id",
    "silver_ingestion_timestamp"
)

print("Step 4.10 complete - Final column selection confirmed.")
print(f"Columns in df_silver_officer_client_mapping: {df_silver_officer_client_mapping.columns}")
print(f"Row count: {df_silver_officer_client_mapping.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4.11: Print summary counts ---

total         = df_silver_officer_client_mapping.count()
clean         = df_silver_officer_client_mapping.filter(F.col("data_quality_flag") == "CLEAN").count()
flagged       = df_silver_officer_client_mapping.filter(F.col("data_quality_flag") != "CLEAN").count()
corrected     = df_silver_officer_client_mapping.filter(F.col("data_quality_flag").contains("CORRECTED")).count()
inactive      = df_silver_officer_client_mapping.filter(F.col("data_quality_flag").contains("INACTIVE_OFFICER_MAPPING")).count()
current       = df_silver_officer_client_mapping.filter(F.col("IsCurrent") == True).count()
not_current   = df_silver_officer_client_mapping.filter(F.col("IsCurrent") == False).count()

print("=" * 55)
print("silver_officer_client_mapping SUMMARY")
print("=" * 55)
print(f"  Total records              : {total}")
print(f"  Clean records              : {clean}")
print(f"  Flagged records            : {flagged}")
print(f"  IsActive corrected         : {corrected}")
print(f"  Inactive officer mappings  : {inactive}")
print(f"  Current assignments        : {current}")
print(f"  Closed assignments         : {not_current}")
print(f"  Duplicates removed         : {duplicate_count}")
print("=" * 55)
print("Section 4 complete. df_silver_officer_client_mapping ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 5: Clean and Transform `silver_debtor_loan_collateral`
# 
# Produces `silver_debtor_loan_collateral` by cleaning bronze_debtor, bronze_loan,
# and bronze_collateral individually then joining them into a single unified table.
# 
# Grain: one row per collateral asset per loan per debtor.
# Expected row count: 300 (driven by bronze_collateral as the most granular table).
# 
# SCD Type 2 is applied for LoanStatus changes to preserve loan status history
# for audit and trend analysis in a financial services context.
# 
# ### Phase A: Clean bronze_debtor
# - **Step 5.1** - Pad 10-digit NationalIDs with leading zero
# - **Step 5.2** - Flag duplicate NationalIDs
# - **Step 5.3** - Flag inactive officer assignments
# - **Step 5.4** - Flag missing emails
# - **Step 5.5** - SHA-256 hash NationalID
# - **Step 5.6** - Build debtor data_quality_flag
# 
# ### Phase B: Clean bronze_loan
# - **Step 5.7** - Flag payment before loan start date
# - **Step 5.8** - Flag settled loans with balance anomalies
# - **Step 5.9** - Flag balance exceeding initial loan amount
# - **Step 5.10** - Flag inverted loan dates
# - **Step 5.11** - Set is_eligible_for_ltv
# - **Step 5.12** - Build loan data_quality_flag
# 
# ### Phase C: Clean bronze_collateral
# - **Step 5.13** - Apply ticker symbol corrections and uppercase
# - **Step 5.14** - Derive and impute Exchange
# - **Step 5.15** - Flag invalid quantity and pledge value
# - **Step 5.16** - Set is_eligible_for_ltv
# - **Step 5.17** - Build collateral data_quality_flag
# 
# ### Join and SCD Type 2
# - **Step 5.18** - Three way join
# - **Step 5.19** - Generate surrogate key
# - **Step 5.20** - Add SCD Type 2 columns
# - **Step 5.21** - Add audit columns
# - **Step 5.22** - Select and order final columns
# - **Step 5.23** - Print summary counts


# CELL ********************

# ============================================================
# SECTION 5: CLEAN AND TRANSFORM silver_debtor_loan_collateral
# ============================================================

# --- PHASE A: CLEAN bronze_debtor ---

# Start from the raw Bronze DataFrame
df_debtor = df_bronze_debtor

# --- Step 5.1: Pad 10-digit NationalIDs with leading zero ---
# Profiling finding: 10 NationalIDs are 10 digits instead of 11.
# Nigerian NIN is 11 digits. Leading zero was stripped during source export.
# F.lpad pads the left side of the string with zeros to reach length 11.
# Records already at 11 digits are untouched by lpad.

df_debtor = df_debtor.withColumn(
    "NationalID_flag",
    F.when(
        F.length(F.col("NationalID")) == 10,
        F.lit("NATIONALID_PADDED")
    ).when(
        F.col("NationalID").isNull(),
        F.lit("NATIONALID_MISSING")
    ).otherwise(
        F.lit("CLEAN")
    )
).withColumn(
    "NationalID",
    F.when(
        F.length(F.col("NationalID")) == 10,
        F.lpad(F.col("NationalID"), 11, "0")
    ).otherwise(
        F.col("NationalID")
    )
)

# Quick check
print("Step 5.1 complete - NationalID flag distribution:")
df_debtor.groupBy("NationalID_flag").count().show()

print("Sample padded records:")
df_debtor.filter(F.col("NationalID_flag") == "NATIONALID_PADDED").select(
    "DebtorID", "NationalID", "NationalID_flag"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.2: Flag duplicate NationalIDs ---
# Profiling finding: 4 NationalIDs each appear on 2 different DebtorIDs.
# Total affected rows: 8.
# We cannot auto-deduplicate here. Both records may represent real individuals.
# A data entry error at source means human review is required.
# All affected rows are flagged and retained for escalation.

window_nationalid = Window.partitionBy("NationalID")

df_debtor = df_debtor.withColumn(
    "nationalid_count",
    F.count("NationalID").over(window_nationalid)
).withColumn(
    "DuplicateNationalID_flag",
    F.when(
        F.col("nationalid_count") > 1,
        F.lit("DUPLICATE_NATIONAL_ID")
    ).otherwise(
        F.lit("CLEAN")
    )
).drop("nationalid_count")

# Quick check
print("Step 5.2 complete - DuplicateNationalID flag distribution:")
df_debtor.groupBy("DuplicateNationalID_flag").count().show()

print("Duplicate NationalID records:")
df_debtor.filter(F.col("DuplicateNationalID_flag") == "DUPLICATE_NATIONAL_ID").select(
    "DebtorID", "NationalID", "DuplicateNationalID_flag"
).orderBy("NationalID").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.3: Flag inactive officer assignments ---
# Profiling finding: 41 debtors assigned to Inactive or Suspended officers.
# These debtors have no active officer managing their recovery.
# Operational risk that must be visible in Silver.
# We join to bronze_collections_officer to bring in current officer status.
# Flag affected records but do not delete or reassign them.
# Reassignment is a business decision not a pipeline decision.

df_officer_status_debtor = df_bronze_collections_officer.select(
    F.col("OfficerID").alias("AssignedOfficerID"),
    F.col("Status").alias("assigned_officer_status")
)

df_debtor = df_debtor.join( df_officer_status_debtor,
    on="AssignedOfficerID",
    how="left"
)

df_debtor = df_debtor.withColumn(
    "OfficerAssignment_flag",
    F.when(
        F.col("assigned_officer_status") == "Active",
        F.lit("CLEAN")
    ).when(
        F.col("assigned_officer_status").isNull(),
        F.lit("OFFICER_NOT_FOUND")
    ).otherwise(
        F.lit("INACTIVE_OFFICER_ASSIGNMENT")
    )
)

# Quick check
print("Step 5.3 complete - OfficerAssignment flag distribution:")
df_debtor.groupBy(
    "assigned_officer_status", "OfficerAssignment_flag"
).count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.4: Flag missing email addresses ---
# Profiling finding: 3 NULL EmailAddress values.
# Email is a contact field not an identifier.
# Missing email reduces outreach options but does not invalidate the record.
# NULL is retained. Record is flagged for completeness tracking.

df_debtor = df_debtor.withColumn(
    "Email_flag",
    F.when(
        F.col("EmailAddress").isNull(),
        F.lit("MISSING_EMAIL")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.4 complete - Email flag distribution:")
df_debtor.groupBy("Email_flag").count().show()

print("Missing email records:")
df_debtor.filter(F.col("Email_flag") == "MISSING_EMAIL").select(
    "DebtorID", "FullName", "EmailAddress", "Email_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.5: SHA-256 hash NationalID ---
# NationalID is sensitive PII with no dashboard business purpose.
# No collections officer needs to see a debtor's NIN to perform recovery.
# Hashing at Silver is correct and proportionate.
# One-way SHA-256 hash. Original value is not recoverable from the hash.
# PhoneNumber, EmailAddress, ResidentialAddress are NOT hashed here.
# Those fields are operational contact fields protected at the Power BI
# semantic model level via Object Level Security.

df_debtor = df_debtor.withColumn(
    "NationalID",
    F.sha2(F.col("NationalID").cast("string"), 256)
)

# Quick check - confirm hashing worked
print("Step 5.5 complete - NationalID hashing confirmed:")
df_debtor.select("DebtorID", "NationalID").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.6: Build debtor composite data_quality_flag ---
# Combines NationalID_flag, DuplicateNationalID_flag,
# OfficerAssignment_flag, and Email_flag into a single composite column.
# Individual flag columns dropped after composite is built.

df_debtor = df_debtor.withColumn(
    "debtor_data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("NationalID_flag"),
                    F.col("DuplicateNationalID_flag"),
                    F.col("OfficerAssignment_flag"),
                    F.col("Email_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("NationalID_flag"),
                    F.col("DuplicateNationalID_flag"),
                    F.col("OfficerAssignment_flag"),
                    F.col("Email_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop(
    "NationalID_flag",
    "DuplicateNationalID_flag",
    "OfficerAssignment_flag",
    "Email_flag"
)

# Quick check
print("Step 5.6 complete - Debtor composite flag distribution:")
df_debtor.groupBy("debtor_data_quality_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_debtor.filter(
    F.col("debtor_data_quality_flag").contains("DUPLICATE_NATIONAL_ID")
).select(
    "DebtorID", "assigned_officer_status", "debtor_data_quality_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Phase B: Clean bronze_loan
# - **Step 5.7** - Flag payment before loan start date
# - **Step 5.8** - Flag settled loans with balance anomalies
# - **Step 5.9** - Flag balance exceeding initial loan amount
# - **Step 5.10** - Flag inverted loan dates
# - **Step 5.11** - Set is_eligible_for_ltv
# - **Step 5.12** - Build loan data_quality_flag

# CELL ********************

# --- PHASE B: CLEAN bronze_loan ---

# Start from the raw Bronze DataFrame
df_loan = df_bronze_loan

# --- Step 5.7: Flag payment before loan start date ---
# Profiling finding: 83 loans have LastPaymentDate before LoanStartDate.
# A payment recorded before the loan was issued is impossible.
# This is a systemic source system failure, likely a legacy migration error.
# S3 is the system of record for payment data per the formal business rule.
# SQL Server LastPaymentDate is superseded by S3 at Silver anyway.
# However these records must be flagged for audit purposes so the source system owner has evidence of the corruption.
# We do not correct the value. We flag and retain.

df_loan = df_loan.withColumn(
    "PaymentDate_flag",
    F.when(
        (F.col("LastPaymentDate").isNotNull()) &
        (F.col("LastPaymentDate") < F.col("LoanStartDate")),
        F.lit("PAYMENT_BEFORE_LOAN_START")
    ).when(
        F.col("LastPaymentDate").isNull(),
        F.lit("NO_PAYMENT_HISTORY")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.7 complete - PaymentDate flag distribution:")
df_loan.groupBy("PaymentDate_flag").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.8: Flag settled loans with balance anomalies ---
# Profiling finding 1: 7 loans with LoanStatus = Settled but
# OutstandingBalance > 0. A settled loan must have zero balance.
# Profiling finding 2: 6 loans with LoanStatus = Settled but
# DaysPastDue > 0. A settled loan cannot be past due.
# 5 of these 6 loans overlap with finding 1.
# We do not auto-correct either value. Both may be correct and the
# status may be wrong, or vice versa. Human review required.
# Composite flag pattern handles the 5 overlapping records automatically.

df_loan = df_loan.withColumn(
    "SettledBalance_flag",
    F.when(
        (F.col("LoanStatus") == "Settled") &
        (F.col("OutstandingBalance") > 0),
        F.lit("SETTLED_WITH_BALANCE")
    ).otherwise(
        F.lit("CLEAN")
    )
).withColumn(
    "SettledDPD_flag",
    F.when(
        (F.col("LoanStatus") == "Settled") &
        (F.col("DaysPastDue") > 0),
        F.lit("SETTLED_PAST_DUE")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.8 complete - Settled anomaly flag distributions:")
print("\nSettledBalance_flag:")
df_loan.groupBy("SettledBalance_flag").count().show()

print("SettledDPD_flag:")
df_loan.groupBy("SettledDPD_flag").count().show()

print("Overlapping records (both flags):")
df_loan.filter(
    (F.col("SettledBalance_flag") == "SETTLED_WITH_BALANCE") &
    (F.col("SettledDPD_flag") == "SETTLED_PAST_DUE")
).select(
    "LoanID", "LoanStatus", "OutstandingBalance",
    "DaysPastDue", "SettledBalance_flag", "SettledDPD_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.9: Flag balance exceeding initial loan amount ---
# Profiling finding: 8 loans where OutstandingBalance > InitialLoanAmount.
# A balance cannot exceed the original loan without capitalised penalties
# which are not modelled in this schema.
# Either value could be wrong. Do not auto-correct.
# Flag and exclude from LTV calculation pending business clarification.

df_loan = df_loan.withColumn(
    "BalanceExceedsInitial_flag",
    F.when(
        F.col("OutstandingBalance") > F.col("InitialLoanAmount"),
        F.lit("BALANCE_EXCEEDS_INITIAL")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.9 complete - BalanceExceedsInitial flag distribution:")
df_loan.groupBy("BalanceExceedsInitial_flag").count().show()

print("Flagged records sample:")
df_loan.filter(
    F.col("BalanceExceedsInitial_flag") == "BALANCE_EXCEEDS_INITIAL"
).select(
    "LoanID",
    "InitialLoanAmount",
    "OutstandingBalance",
    "BalanceExceedsInitial_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.10: Flag inverted loan dates ---
# Profiling finding: 3 loans where LoanMaturityDate is before LoanStartDate.
# Dates appear to have been swapped at data entry.
# We do not auto-swap. Either date could be the error.
# Escalate to source system owner for manual correction.
# These records will be excluded from LTV calculation in Step 5.11
# because a valid loan time horizon cannot be determined.

df_loan = df_loan.withColumn(
    "InvertedDates_flag",
    F.when(
        F.col("LoanMaturityDate") < F.col("LoanStartDate"),
        F.lit("INVERTED_LOAN_DATES")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.10 complete - InvertedDates flag distribution:")
df_loan.groupBy("InvertedDates_flag").count().show()

print("Inverted date records:")
df_loan.filter(F.col("InvertedDates_flag") == "INVERTED_LOAN_DATES").select(
    "LoanID", "LoanStartDate", "LoanMaturityDate", "InvertedDates_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.11: Set is_eligible_for_ltv for loan records ---
# The Gold layer uses this single boolean to filter records before
# computing LTV. Silver makes the eligibility decision. Gold respects it.
#
# FALSE conditions for loan records:
#   1. InvertedDates_flag = INVERTED_LOAN_DATES
#      Cannot determine valid loan time horizon.
#   2. SettledBalance_flag = SETTLED_WITH_BALANCE
#      Outstanding balance is untrustworthy on a settled loan.
#   3. BalanceExceedsInitial_flag = BALANCE_EXCEEDS_INITIAL
#      Balance figure is untrustworthy without capitalised penalty modelling.
#
# PAYMENT_BEFORE_LOAN_START does not trigger FALSE because S3 supersedes
# SQL Server payment data. The corrupted field does not enter LTV formula.
# SETTLED_PAST_DUE does not trigger FALSE because DaysPastDue is not
# part of the LTV formula.

df_loan = df_loan.withColumn(
    "is_eligible_for_ltv",
    F.when(
        (F.col("InvertedDates_flag") == "INVERTED_LOAN_DATES") |
        (F.col("SettledBalance_flag") == "SETTLED_WITH_BALANCE") |
        (F.col("BalanceExceedsInitial_flag") == "BALANCE_EXCEEDS_INITIAL"),
        F.lit(False)
    ).otherwise(
        F.lit(True)
    )
)

# Quick check
print("Step 5.11 complete - is_eligible_for_ltv distribution:")
df_loan.groupBy("is_eligible_for_ltv").count().show()

print("Ineligible loan records:")
df_loan.filter(F.col("is_eligible_for_ltv") == False).select(
    "LoanID",
    "LoanStatus",
    "OutstandingBalance",
    "InitialLoanAmount",
    "InvertedDates_flag",
    "SettledBalance_flag",
    "BalanceExceedsInitial_flag",
    "is_eligible_for_ltv"
).orderBy("LoanID").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.12: Build loan composite data_quality_flag ---
# Combines all five loan flag columns into a single composite column.
# is_eligible_for_ltv is retained as a separate standalone column.
# It serves a specific functional purpose at Gold and must remain
# independently queryable without parsing a flag string.

df_loan = df_loan.withColumn(
    "loan_data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("PaymentDate_flag"),
                    F.col("SettledBalance_flag"),
                    F.col("SettledDPD_flag"),
                    F.col("BalanceExceedsInitial_flag"),
                    F.col("InvertedDates_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("PaymentDate_flag"),
                    F.col("SettledBalance_flag"),
                    F.col("SettledDPD_flag"),
                    F.col("BalanceExceedsInitial_flag"),
                    F.col("InvertedDates_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop(
    "PaymentDate_flag",
    "SettledBalance_flag",
    "SettledDPD_flag",
    "BalanceExceedsInitial_flag",
    "InvertedDates_flag"
)

# Quick check
print("Step 5.12 complete - Loan composite flag distribution:")
df_loan.groupBy("loan_data_quality_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Phase C: Clean bronze_collateral
# - **Step 5.13** - Apply ticker symbol corrections and uppercase
# - **Step 5.14** - Derive and impute Exchange
# - **Step 5.15** - Flag invalid quantity and pledge value
# - **Step 5.16** - Set is_eligible_for_ltv
# - **Step 5.17** - Build collateral data_quality_flag

# CELL ********************

# --- PHASE C: CLEAN bronze_collateral ---

# Start from the raw Bronze DataFrame
df_collateral = df_bronze_collateral

# --- Baseline normalisation: trim all STRING columns ---
# Trailing and leading whitespace is a systemic data entry risk
# across any text field, not just TickerSymbol.
# Applying trim unconditionally to all STRING columns before any
# other transformation ensures no downstream logic is tripped by
# invisible whitespace characters.
# This is a defensive baseline step applied before all other cleaning.

string_columns = [
    field.name for field in df_collateral.schema.fields
    if field.dataType.typeName() == "string"
]

for col_name in string_columns:
    df_collateral = df_collateral.withColumn(
        col_name, F.trim(F.col(col_name))
    )

print(f"Baseline trim applied to {len(string_columns)} STRING columns:")
print(string_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- PHASE C: CLEAN bronze_collateral ---

# Start from the raw Bronze DataFrame
df_collateral = df_bronze_collateral

# --- Step 5.13: Apply ticker symbol corrections and uppercase ---
# Profiling finding: Four distinct ticker issues found.
# TickerSymbol is the join key to bronze_market_prices.
# A wrong ticker produces a silent null join at Gold.
# Fix: trim and uppercase first, then apply explicit corrections,
# then flag based on what changed.

# Step 1: Trim and uppercase all tickers first
# This ensures explicit corrections match regardless of original case
df_collateral = df_collateral.withColumn(
    "TickerSymbol_original",
    F.col("TickerSymbol")
).withColumn(
    "TickerSymbol",
    F.upper(F.trim(F.col("TickerSymbol")))
)

# Step 2: Apply explicit ticker corrections on uppercased values
df_collateral = df_collateral.withColumn(
    "TickerSymbol",
    F.when(
        F.col("TickerSymbol") == "APPLE INC",
        F.lit("AAPL")
    ).when(
        F.col("TickerSymbol") == "BTC/USD",
        F.lit("BTC-USD")
    ).otherwise(
        F.col("TickerSymbol")
    )
).withColumn(
    # Fix AssetType for BTC-USD and NVDA records
    "AssetType",
    F.when(
        F.col("TickerSymbol") == "BTC-USD",
        F.lit("Crypto")
    ).when(
        (F.col("TickerSymbol") == "NVDA") &
        (F.col("AssetType") == "Crypto"),
        F.lit("Stock")
    ).otherwise(
        F.col("AssetType")
    )
).withColumn(
    # Fix Exchange for BTC-USD and NVDA records
    "Exchange",
    F.when(
        F.col("TickerSymbol") == "BTC-USD",
        F.lit("CRYPTO")
    ).when(
        (F.col("TickerSymbol") == "NVDA") &
        (F.col("Exchange") == "CRYPTO"),
        F.lit("NASDAQ")
    ).otherwise(
        F.col("Exchange")
    )
)

# Step 3: Build ticker flag by comparing original to corrected value
df_collateral = df_collateral.withColumn(
    "TickerSymbol_flag",
    F.when(
        F.upper(F.trim(F.col("TickerSymbol_original"))) == "APPLE INC",
        F.lit("TICKER_CORRECTED")
    ).when(
        F.upper(F.trim(F.col("TickerSymbol_original"))) == "BTC/USD",
        F.lit("TICKER_AND_ASSET_TYPE_CORRECTED")
    ).when(
        (F.col("TickerSymbol") == "NVDA") &
        (F.upper(F.trim(F.col("TickerSymbol_original"))) == "NVDA") &
        (F.col("AssetType") == "Stock"),
        F.lit("ASSET_TYPE_CORRECTED")
    ).when(
        F.col("TickerSymbol_original") != F.upper(F.trim(F.col("TickerSymbol_original"))),
        F.lit("TICKER_UPPERCASED")
    ).otherwise(
        F.lit("CLEAN")
    )
).drop("TickerSymbol_original")

# Quick check
print("Step 5.13 complete - TickerSymbol flag distribution:")
df_collateral.groupBy("TickerSymbol_flag").count().orderBy(
    "count", ascending=False
).show()

print("Corrected ticker records:")
df_collateral.filter(
    F.col("TickerSymbol_flag") != "CLEAN"
).select(
    "CollateralID", "TickerSymbol", "AssetType",
    "Exchange", "TickerSymbol_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.14: Derive and impute Exchange from TickerSymbol ---
# Profiling finding: 6 NULL Exchange values across known tickers.
# Additional diagnostic finding: 44 NVDA records incorrectly mapped to NYSE.
# Additional diagnostic finding: BTC/USD records had CRYPTO on NYSE.
# Strategy: capture original Exchange first, derive correct value,
# compare original to correct to build flag, then replace.

ticker_exchange_map = {
    "AAPL"   : "NYSE",
    "TSLA"   : "NASDAQ",
    "GOOGL"  : "NASDAQ",
    "MSFT"   : "NASDAQ",
    "AMZN"   : "NASDAQ",
    "NVDA"   : "NASDAQ",
    "BTC-USD": "CRYPTO"
}

# Step 1: Capture original Exchange before any modification
df_collateral = df_collateral.withColumn(
    "Exchange_original", F.col("Exchange")
)

# Step 2: Derive the correct exchange into a helper column
correct_exchange = F.col("Exchange")
for ticker, exchange in ticker_exchange_map.items():
    correct_exchange = F.when(
        F.col("TickerSymbol") == ticker,
        F.lit(exchange)
    ).otherwise(correct_exchange)

df_collateral = df_collateral.withColumn(
    "Exchange_correct", correct_exchange
)

# Step 3: Replace Exchange with correct value
df_collateral = df_collateral.withColumn(
    "Exchange", F.col("Exchange_correct")
).drop("Exchange_correct")

# Step 4: Build flag by comparing Exchange_original to corrected Exchange
# Now the comparison is between the true original and the new value
df_collateral = df_collateral.withColumn(
    "Exchange_flag",
    F.when(
        F.col("Exchange_original").isNull(),
        F.lit("EXCHANGE_DERIVED")
    ).when(
        F.col("Exchange_original") != F.col("Exchange"),
        F.lit("EXCHANGE_CORRECTED")
    ).otherwise(
        F.lit("CLEAN")
    )
).drop("Exchange_original")

# Quick check
print("Step 5.14 complete - Exchange flag distribution:")
df_collateral.groupBy("Exchange_flag").count().orderBy(
    "count", ascending=False
).show()

print("Exchange distribution after correction:")
df_collateral.groupBy("Exchange").count().orderBy(
    "count", ascending=False
).show()

print("NVDA Exchange distribution after correction:")
df_collateral.filter(
    F.col("TickerSymbol") == "NVDA"
).groupBy("Exchange").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze_collateral.groupBy("TickerSymbol", "Exchange").count().orderBy(
    "TickerSymbol", "Exchange"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.15: Flag invalid quantity and pledge value ---
# Profiling finding 1: 4 rows with zero or negative QuantityHeld.
# Zero or negative quantity produces meaningless collateral value in LTV.
# Profiling finding 2: 5 rows with zero or negative CollateralValueAtPledge.
# Pledge value is not directly in LTV formula but a zero or negative value
# indicates a fundamentally broken record requiring investigation.
# Both issues retain the original value and flag for escalation.

df_collateral = df_collateral.withColumn(
    "QuantityHeld_flag",
    F.when(
        F.col("QuantityHeld") <= 0,
        F.lit("INVALID_QUANTITY")
    ).otherwise(
        F.lit("CLEAN")
    )
).withColumn(
    "PledgeValue_flag",
    F.when(
        F.col("CollateralValueAtPledge") <= 0,
        F.lit("INVALID_PLEDGE_VALUE")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 5.15 complete - QuantityHeld flag distribution:")
df_collateral.groupBy("QuantityHeld_flag").count().show()

print("PledgeValue flag distribution:")
df_collateral.groupBy("PledgeValue_flag").count().show()

print("Invalid quantity records:")
df_collateral.filter(
    F.col("QuantityHeld_flag") == "INVALID_QUANTITY"
).select(
    "CollateralID", "TickerSymbol", "QuantityHeld", "QuantityHeld_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.16: Set is_eligible_for_ltv for collateral records ---
# The Gold layer uses this boolean to filter records before LTV computation.
# Records remain in Silver fully visible. This is a quarantine signal,
# not an exclusion mechanism. The original values are preserved.
#
# FALSE conditions for collateral records:
#   1. QuantityHeld <= 0
#      Zero or negative quantity produces meaningless collateral value.
#   2. CollateralValueAtPledge <= 0
#      Indicates a fundamentally broken record requiring investigation.
#
# Ticker corrections applied in Steps 5.13 and 5.14 do NOT trigger FALSE.
# Those records were corrected and are now eligible for LTV calculation.

df_collateral = df_collateral.withColumn(
    "is_eligible_for_ltv",
    F.when(
        (F.col("QuantityHeld_flag") == "INVALID_QUANTITY") |
        (F.col("PledgeValue_flag") == "INVALID_PLEDGE_VALUE"),
        F.lit(False)
    ).otherwise(
        F.lit(True)
    )
)

# Quick check
print("Step 5.16 complete - is_eligible_for_ltv distribution:")
df_collateral.groupBy("is_eligible_for_ltv").count().show()

print("Ineligible collateral records:")
df_collateral.filter(
    F.col("is_eligible_for_ltv") == False
).select(
    "CollateralID",
    "TickerSymbol",
    "QuantityHeld",
    "CollateralValueAtPledge",
    "QuantityHeld_flag",
    "PledgeValue_flag",
    "is_eligible_for_ltv"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5.17: Build collateral composite data_quality_flag ---
# Combines TickerSymbol_flag, Exchange_flag, QuantityHeld_flag,
# and PledgeValue_flag into a single composite column.
# is_eligible_for_ltv is retained as a separate standalone column.
# Individual flag columns dropped after composite is built.

df_collateral = df_collateral.withColumn(
    "collateral_data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("TickerSymbol_flag"),
                    F.col("Exchange_flag"),
                    F.col("QuantityHeld_flag"),
                    F.col("PledgeValue_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("TickerSymbol_flag"),
                    F.col("Exchange_flag"),
                    F.col("QuantityHeld_flag"),
                    F.col("PledgeValue_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop(
    "TickerSymbol_flag",
    "Exchange_flag",
    "QuantityHeld_flag",
    "PledgeValue_flag"
)

# Quick check
print("Step 5.17 complete - Collateral composite flag distribution:")
df_collateral.groupBy("collateral_data_quality_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ### Join and SCD Type 2
# - **Step 5.18** - Three way join
# - **Step 5.19** - Generate surrogate key
# - **Step 5.20** - Add SCD Type 2 columns
# - **Step 5.21** - Add audit columns
# - **Step 5.22** - Select and order final columns
# - **Step 5.23** - Print summary counts

# MARKDOWN ********************

# ---
# ### **Step 5.18** - Three way join

# CELL ********************

# --- Step 5.18: Resolve duplicate columns and execute three way join ---
# Join sequence follows the natural data model hierarchy:
#   Debtor owns Loans, Loans have Collateral.
#   df_debtor --> df_loan on DebtorID --> df_collateral on LoanID
#
# Duplicate resolution strategy:
#   1. Rename audit columns and flag columns on all three DataFrames
#      before any join to avoid ambiguity during join execution.
#   2. Drop ClientID from df_loan before the first join.
#      ClientID is authoritative on df_debtor. The copy on df_loan
#      is redundant and is not needed as a join key.
#   3. Execute first join: df_debtor to df_loan on DebtorID.
#      Spark deduplicates DebtorID automatically when using on= syntax.
#   4. Run diagnostic to identify remaining duplicates between the
#      joined result and df_collateral before executing the second join.
#      Diagnostic found: DebtorID and LoanID shared between both.
#      LoanID is the join key for the second join so it must remain.
#      DebtorID is not a join key for the second join. The authoritative
#      copy already exists in the joined result from df_debtor.
#   5. Drop DebtorID from df_collateral before the second join.
#   6. Execute second join: df_silver_debtor_loan to df_collateral on LoanID.
#      Spark deduplicates LoanID automatically when using on= syntax.
#
# Result: 300 rows, no duplicate columns, grain confirmed at
# one collateral asset per loan per debtor.
# All three flag columns and both eligibility flags independently visible.

# --- Pre-join: rename audit columns and flag columns ---
df_loan = df_loan \
    .drop("pipeline_run_id") \
    .withColumnRenamed("ingestion_timestamp", "loan_ingestion_timestamp") \
    .withColumnRenamed("source_system", "loan_source_system") \
    .withColumnRenamed("is_eligible_for_ltv", "loan_is_eligible_for_ltv")

df_collateral = df_collateral \
    .drop("pipeline_run_id") \
    .withColumnRenamed("ingestion_timestamp", "collateral_ingestion_timestamp") \
    .withColumnRenamed("source_system", "collateral_source_system") \
    .withColumnRenamed("is_eligible_for_ltv", "collateral_is_eligible_for_ltv")

df_debtor = df_debtor \
    .drop("pipeline_run_id") \
    .withColumnRenamed("ingestion_timestamp", "debtor_ingestion_timestamp") \
    .withColumnRenamed("source_system", "debtor_source_system")

# before executing the first join
# drop the clientID col from the df_loan dataframe
df_loan = df_loan.drop("ClientID")


# --- First join: df_debtor to df_loan on DebtorID ---

df_silver_debtor_loan = df_debtor \
    .join(df_loan, on="DebtorID", how="left") 


df_silver_debtor_loan.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# --- Diagnostic: find duplicates between joined result and df_collateral ---
# DebtorID found in both. Not a join key for second join.

silver_debtor_loan_cols = set(df_silver_debtor_loan.columns)
collateral_cols = set(df_collateral.columns)

print("silver_debtor_loan_cols and collateral_cols :")
print(silver_debtor_loan_cols & collateral_cols)

# Drop from df_collateral before proceeding.
df_collateral = df_collateral.drop("DebtorID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Go on to join df_silver_debtor_loan to df_collateral 

df_silver_debtor_loan_collateral = df_silver_debtor_loan.\
                        join(df_collateral, on="LoanID", how="left")

df_silver_debtor_loan_collateral.printSchema()

# Row count check
total = df_silver_debtor_loan_collateral.count()
print(f"\nStep 5.18 complete - Three way join row count: {total}")
print(f"Expected: 300")
print(f"Match: {total == 300}")

print("\nSample joined record:")
df_silver_debtor_loan_collateral.select(
    "DebtorID",
    "LoanID",
    "CollateralID",
    "TickerSymbol",
    "OutstandingBalance",
    "QuantityHeld",
    "debtor_data_quality_flag",
    "loan_data_quality_flag",
    "collateral_data_quality_flag",
    "loan_is_eligible_for_ltv",
    "collateral_is_eligible_for_ltv"
).show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 5.19** - Generate surrogate key


# CELL ********************

# --- Step 5.19: Generate surrogate key dlc_sk ---
# Three approaches exist for surrogate key generation in PySpark:
#
# Option 1: SHA-256 hash (chosen approach)
#   Produces a 64 character deterministic string.
#   Deterministic means the same input always produces the same key
#   across every pipeline run. Key stability is critical here because
#   Gold layer aggregations and downstream joins rely on consistent keys.
#   Tradeoff: long and not human readable.
#
# Option 2: UUID (F.expr("uuid()"))
#   Produces a shorter 36 character string.
#   NOT deterministic. Every pipeline run generates a different UUID
#   for the same record. This breaks downstream joins that rely on
#   key stability across runs. Rejected for this reason.
#
# Option 3: monotonically_increasing_id()
#   Produces a simple incrementing integer. Short and readable.
#   NOT deterministic. The integer assigned to a row depends on Spark
#   partition processing order which can change between runs.
#   Rejected for the same reason as UUID.
#
# SHA-256 was chosen because determinism is non-negotiable in a
# production pipeline. The length is a storage and readability cost
# worth paying for key stability across daily pipeline runs.
#
# Note: trimming to the first 16 characters is a valid optimisation
# for large datasets. Collision probability remains effectively zero
# at this data volume. Full 64 characters retained here for maximum
# safety in a financial domain context.
#
# dlc_sk = debtor loan collateral surrogate key.
# Natural key components: DebtorID + LoanID + CollateralID.


df_silver_debtor_loan_collateral = df_silver_debtor_loan_collateral.withColumn(
    "dlc_sk",
    F.sha2(
        F.concat_ws("|",
            F.col("DebtorID"),
            F.col("LoanID"),
            F.col("CollateralID")
        ),
        256
    )
)

# Confirm uniqueness of surrogate key
total_rows = df_silver_debtor_loan_collateral.count()
distinct_keys = df_silver_debtor_loan_collateral.select("dlc_sk").distinct().count()

print("Step 5.19 complete - Surrogate key generation:")
print(f"  Total rows       : {total_rows}")
print(f"  Distinct keys    : {distinct_keys}")
print(f"  Keys unique      : {total_rows == distinct_keys}")

print("\nSample surrogate keys:")
df_silver_debtor_loan_collateral.select(
    "DebtorID", "LoanID", "CollateralID", "dlc_sk"
).show(3, truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 5.20** - Add SCD Type 2 columns

# CELL ********************

# --- Step 5.20: Add SCD Type 2 columns ---
# SCD Type 2 is applied for LoanStatus changes.
# When a loan status changes, a new row is inserted with a new
# EffectiveStartDate and the old row gets an EffectiveEndDate.
# This preserves the full history of loan status changes for audit
# and trend analysis in a financial services context.
#
# Initial load: every record is the current version.
# EffectiveStartDate = today's date (first time this record enters Silver)
# EffectiveEndDate   = NULL (no superseding record exists yet)
# IsCurrent          = TRUE (all records are current on initial load)
#
# On subsequent runs, the Delta merge in Section 8 handles:
# - Detecting LoanStatus changes by comparing incoming vs existing rows
# - Closing old rows by setting EffectiveEndDate and IsCurrent = FALSE
# - Inserting new rows with updated EffectiveStartDate and IsCurrent = TRUE

df_silver_debtor_loan_collateral = df_silver_debtor_loan_collateral \
    .withColumn(
        "EffectiveStartDate",
        F.current_date()
    ).withColumn(
        "EffectiveEndDate",
        F.lit(None).cast(DateType())
    ).withColumn(
        "IsCurrent",
        F.lit(True)
    )

# Quick check
print("Step 5.20 complete - SCD Type 2 columns added:")
df_silver_debtor_loan_collateral.select(
    "DebtorID", "LoanID", "LoanStatus",
    "EffectiveStartDate", "EffectiveEndDate", "IsCurrent"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 5.21** - Add audit columns


# CELL ********************

# --- Step 5.21: Add audit columns ---
# silver_ingestion_timestamp and pipeline_run_id defined in Section 1.
# Applied consistently to every Silver table in this notebook.
# Bronze pipeline_run_id was dropped from all three source DataFrames
# in Step 5.18 pre-join resolution. This Silver pipeline_run_id
# replaces all three Bronze versions with a single Silver run identifier.

df_silver_debtor_loan_collateral = df_silver_debtor_loan_collateral \
    .withColumn(
        "silver_ingestion_timestamp",
        F.lit(SILVER_INGESTION_TIMESTAMP).cast(TimestampType())
    ).withColumn(
        "pipeline_run_id", F.lit(PIPELINE_RUN_ID)
    )

print("Step 5.21 complete - Audit columns added.")
print(f"  pipeline_run_id            : {PIPELINE_RUN_ID}")
print(f"  silver_ingestion_timestamp : {SILVER_INGESTION_TIMESTAMP}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 5.22** - Select and order final columns


# CELL ********************

# --- Step 5.22: Select and order final columns ---
# Explicit column selection ensures no intermediate helper columns
# leak into the Silver table.
# Column order convention:
#   Surrogate key first, natural keys, debtor columns, loan columns,
#   collateral columns, SCD Type 2 columns, flag columns, audit columns last.

df_silver_debtor_loan_collateral = df_silver_debtor_loan_collateral.select(

    # Surrogate key
    "dlc_sk",

    # Natural keys
    "DebtorID",
    "LoanID",
    "CollateralID",

    # Debtor business columns
    "ClientID",
    "AssignedOfficerID",
    "assigned_officer_status",
    "FullName",
    "NationalID",
    "PhoneNumber",
    "EmailAddress",
    "ResidentialAddress",
    "Region",
    "DateOnboarded",

    # Loan business columns
    "InitialLoanAmount",
    "OutstandingBalance",
    "LoanStartDate",
    "LoanMaturityDate",
    "DaysPastDue",
    "LoanStatus",
    "LastPaymentDate",
    "LastPaymentAmount",

    # Collateral business columns
    "TickerSymbol",
    "AssetType",
    "Exchange",
    "QuantityHeld",
    "CollateralValueAtPledge",
    "CollateralPledgeDate",

    # SCD Type 2 columns
    "EffectiveStartDate",
    "EffectiveEndDate",
    "IsCurrent",

    # Flag and eligibility columns
    "loan_is_eligible_for_ltv",
    "collateral_is_eligible_for_ltv",
    "debtor_data_quality_flag",
    "loan_data_quality_flag",
    "collateral_data_quality_flag",

    # Audit columns
    "debtor_ingestion_timestamp",
    "loan_ingestion_timestamp",
    "collateral_ingestion_timestamp",
    "debtor_source_system",
    "loan_source_system",
    "collateral_source_system",
    "pipeline_run_id",
    "silver_ingestion_timestamp"
)

print("Step 5.22 complete - Final column selection confirmed.")
print(f"Total columns : {len(df_silver_debtor_loan_collateral.columns)}")
print(f"Total rows    : {df_silver_debtor_loan_collateral.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 5.23** - Print summary counts

# CELL ********************

# --- Step 5.23: Print summary counts ---
# This is the final step in Section 5. It gives a complete picture of what the entire section produced across all three phases.

total                   = df_silver_debtor_loan_collateral.count()
loan_eligible           = df_silver_debtor_loan_collateral.filter(F.col("loan_is_eligible_for_ltv") == True).count()
loan_ineligible         = df_silver_debtor_loan_collateral.filter(F.col("loan_is_eligible_for_ltv") == False).count()
collateral_eligible     = df_silver_debtor_loan_collateral.filter(F.col("collateral_is_eligible_for_ltv") == True).count()
collateral_ineligible   = df_silver_debtor_loan_collateral.filter(F.col("collateral_is_eligible_for_ltv") == False).count()
both_eligible           = df_silver_debtor_loan_collateral.filter(
    (F.col("loan_is_eligible_for_ltv") == True) &
    (F.col("collateral_is_eligible_for_ltv") == True)
).count()
debtor_flagged          = df_silver_debtor_loan_collateral.filter(F.col("debtor_data_quality_flag") != "CLEAN").count()
loan_flagged            = df_silver_debtor_loan_collateral.filter(F.col("loan_data_quality_flag") != "CLEAN").count()
collateral_flagged      = df_silver_debtor_loan_collateral.filter(F.col("collateral_data_quality_flag") != "CLEAN").count()
current_records         = df_silver_debtor_loan_collateral.filter(F.col("IsCurrent") == True).count()

print("=" * 60)
print("silver_debtor_loan_collateral SUMMARY")
print("=" * 60)
print(f"  Total records                    : {total}")
print(f"  Loan eligible for LTV            : {loan_eligible}")
print(f"  Loan ineligible for LTV          : {loan_ineligible}")
print(f"  Collateral eligible for LTV      : {collateral_eligible}")
print(f"  Collateral ineligible for LTV    : {collateral_ineligible}")
print(f"  Both eligible for LTV            : {both_eligible}")
print(f"  Debtor flagged records           : {debtor_flagged}")
print(f"  Loan flagged records             : {loan_flagged}")
print(f"  Collateral flagged records       : {collateral_flagged}")
print(f"  Current SCD records              : {current_records}")
print("=" * 60)
print("Section 5 complete. df_silver_debtor_loan_collateral ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ---
# 
# ## Section 6: Clean and Transform `silver_bank_balance_update`
# 
# Produces `silver_bank_balance_update` from `bronze_bank_balance_update`.
# Kept separate because it arrives from a different source (Amazon S3 via client
# bank daily drops) and serves a specific purpose at Gold: providing the
# authoritative CurrentOutstandingBalance for LTV calculation.
# 
# Per the formal business rule, S3 is the system of record for
# CurrentOutstandingBalance, LastPaymentDate, and LastPaymentAmount.
# SQL Server loan table values are the fallback only.
# 
# ### Steps
# - **Step 6.1** - Deduplicate on LoanID + ReportingDate
# - **Step 6.2** - Resolve string literal null in AccountStatus
# - **Step 6.3** - Standardise AccountStatus to controlled vocabulary
# - **Step 6.4** - Flag NULL CurrentOutstandingBalance
# - **Step 6.5** - Cast ReportingDate and LastPaymentDate from TIMESTAMP to DATE
# - **Step 6.6** - Cast CurrentOutstandingBalance and LastPaymentAmount from DOUBLE to DECIMAL
# - **Step 6.7** - Set is_eligible_for_ltv
# - **Step 6.8** - Build composite data_quality_flag
# - **Step 6.9** - Add audit columns
# - **Step 6.10** - Select and order final columns
# - **Step 6.11** - Print summary counts


# MARKDOWN ********************

# 
# ### **Step 6.1** - Deduplicate on LoanID + ReportingDate


# CELL ********************

# ============================================================
# SECTION 6: CLEAN AND TRANSFORM silver_bank_balance_update
# ============================================================

# Start from the raw Bronze DataFrame
df_balance = df_bronze_bank_balance_update

# --- Step 6.1: Deduplicate on LoanID + ReportingDate ---
# Profiling finding: 24 duplicate composite key combinations.
# Full row duplicates caused by client bank dropping the same file twice.
# Since rows are fully identical, any row can be retained.
# We keep the first ranked record per LoanID + ReportingDate combination.
# Duplicate count captured before removal for pipeline metadata logging.

window_dup = Window.partitionBy("LoanID", "ReportingDate").orderBy(
    F.col("ingestion_timestamp").desc()
)

df_balance = df_balance.withColumn(
    "row_rank", F.row_number().over(window_dup)
).withColumn(
    "IsDuplicate_flag",
    F.when(F.col("row_rank") == 1, F.lit("CLEAN"))
    .otherwise(F.lit("DUPLICATE_REMOVED"))
)

duplicate_count_balance = df_balance.filter(
    F.col("IsDuplicate_flag") == "DUPLICATE_REMOVED"
).count()

df_balance = df_balance.filter(F.col("row_rank") == 1).drop("row_rank")

print(f"Step 6.1 complete - Duplicates removed: {duplicate_count_balance}")
print(f"Rows after deduplication: {df_balance.count()}")
print("\nIsDuplicate_flag distribution:")
df_balance.groupBy("IsDuplicate_flag").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 6.2** - Resolve string literal null in AccountStatus


# CELL ********************

# --- Step 6.2: Resolve string literal null in AccountStatus ---
# Profiling finding: rows where AccountStatus contains the string "null"
# rather than a database NULL. Source system wrote the word null as a
# string value instead of leaving the field empty.
# This passes NULL checks silently and must be caught explicitly.
# Replace string "null" with actual NULL so downstream checks work correctly.

df_balance = df_balance.withColumn(
    "AccountStatus",
    F.when(
        F.lower(F.col("AccountStatus")) == "null",
        F.lit(None).cast(StringType())
    ).otherwise(
        F.col("AccountStatus")
    )
)

# Quick check
print("Step 6.2 complete - AccountStatus NULL check after string literal resolution:")
df_balance.groupBy(
    F.when(F.col("AccountStatus").isNull(), "NULL").otherwise("NOT NULL").alias("AccountStatus_null_check")
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.3** - Standardise AccountStatus to controlled vocabulary


# CELL ********************

# --- Step 6.3: Standardise AccountStatus to controlled vocabulary ---
# Profiling findings:
#   WRITTEN_OFF -> Written-Off (inconsistent formatting)
#   Suspended   -> flag as SUSPECT_ACCOUNT_STATUS (ambiguous meaning)
#   UNKNOWN     -> flag as UNKNOWN_ACCOUNT_STATUS
#   NULL        -> flag as UNKNOWN_ACCOUNT_STATUS (resolved from Step 6.2)
#   Watch       -> valid, added to domain during profiling
#
# Valid domain: NPL, Watch, Performing, Written-Off, Unknown
# Standardisation map applied first, then validation against valid domain.

# Step 1: Apply standardisation map
status_map = {
    "WRITTEN_OFF" : "Written-Off",
    "written_off" : "Written-Off",
    "NPL"         : "NPL",
    "Watch"       : "Watch",
    "Performing"  : "Performing",
    "Unknown"     : "Unknown",
    "UNKNOWN"     : "Unknown"
}

standardised_status = F.col("AccountStatus")
for raw, standard in status_map.items():
    standardised_status = F.when(
        F.col("AccountStatus") == raw,
        F.lit(standard)
    ).otherwise(standardised_status)

df_balance = df_balance.withColumn(
    "AccountStatus", standardised_status
)

# Step 2: Validate against valid domain and flag
VALID_ACCOUNT_STATUSES = ["NPL", "Watch", "Performing", "Written-Off", "Unknown"]

df_balance = df_balance.withColumn(
    "AccountStatus_flag",
    F.when(
        F.col("AccountStatus").isNull(),
        F.lit("UNKNOWN_ACCOUNT_STATUS")
    ).when(
        F.col("AccountStatus") == "Suspended",
        F.lit("SUSPECT_ACCOUNT_STATUS")
    ).when(
        F.col("AccountStatus").isin(VALID_ACCOUNT_STATUSES),
        F.lit("CLEAN")
    ).otherwise(
        F.lit("INVALID_ACCOUNT_STATUS")
    )
)

# Quick check
print("Step 6.3 complete - AccountStatus distribution after standardisation:")
df_balance.groupBy("AccountStatus", "AccountStatus_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.4** - Flag NULL CurrentOutstandingBalance


# CELL ********************

# --- Step 6.4: Flag NULL CurrentOutstandingBalance ---
# Profiling finding: NULL CurrentOutstandingBalance values found.
# This is the authoritative balance field per the S3 system of record
# business rule. A NULL means no authoritative balance exists for that
# loan on that reporting date.
# Fallback to SQL Server OutstandingBalance is applied at Gold layer
# when the two sources are joined.
# We flag here so Gold knows which records require the fallback.

df_balance = df_balance.withColumn(
    "Balance_flag",
    F.when(
        F.col("CurrentOutstandingBalance").isNull(),
        F.lit("BALANCE_UNAVAILABLE")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 6.4 complete - Balance flag distribution:")
df_balance.groupBy("Balance_flag").count().show()

print("NULL balance records sample:")
df_balance.filter(F.col("Balance_flag") == "BALANCE_UNAVAILABLE").select(
    "LoanID", "ReportingDate", "CurrentOutstandingBalance", "Balance_flag"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.5** - Cast ReportingDate and LastPaymentDate from TIMESTAMP to DATE


# CELL ********************

# --- Step 6.5: Cast ReportingDate and LastPaymentDate from TIMESTAMP to DATE ---
# Profiling finding: both fields stored as TIMESTAMP with time component
# 00:00:00. Time component carries no information.
# Type mismatch between TIMESTAMP and DATE causes silent join failures
# when these fields are used as join keys or in date comparisons.
# F.to_date() strips the time component and produces a clean DATE value.

df_balance = df_balance \
    .withColumn(
        "ReportingDate",
        F.to_date(F.col("ReportingDate"))
    ).withColumn(
        "LastPaymentDate",
        F.to_date(F.col("LastPaymentDate"))
    )

# Quick check - confirm types changed
print("Step 6.5 complete - Date column types after cast:")
df_balance.select(
    "ReportingDate", "LastPaymentDate"
).printSchema()

print("Sample date values after cast:")
df_balance.select(
    "LoanID", "ReportingDate", "LastPaymentDate"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 6.6** - Cast CurrentOutstandingBalance and LastPaymentAmount from DOUBLE to DECIMAL


# CELL ********************

# --- Step 6.6: Cast financial columns from DOUBLE to DECIMAL ---
# Profiling finding: CurrentOutstandingBalance and LastPaymentAmount
# stored as DOUBLE. Floating point precision risk on financial calculations.
# DOUBLE stores approximations internally which accumulate as rounding
# errors when aggregating large portfolios.
# DecimalType(18,2) stores exact values to 2 decimal places.
# Matches the schema of the SQL Server loan table for consistency.

df_balance = df_balance \
    .withColumn(
        "CurrentOutstandingBalance",
        F.col("CurrentOutstandingBalance").cast(DecimalType(18, 2))
    ).withColumn(
        "LastPaymentAmount",
        F.col("LastPaymentAmount").cast(DecimalType(18, 2))
    )

# Quick check - confirm types changed
print("Step 6.6 complete - Financial column types after cast:")
df_balance.select(
    "CurrentOutstandingBalance", "LastPaymentAmount"
).printSchema()

print("Sample financial values after cast:")
df_balance.select(
    "LoanID", "ReportingDate",
    "CurrentOutstandingBalance", "LastPaymentAmount"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.7** - Set is_eligible_for_ltv


# CELL ********************

# --- Step 6.7: Set is_eligible_for_ltv for balance update records ---
# A balance update record is ineligible for LTV if CurrentOutstandingBalance
# is NULL. The authoritative balance figure is missing so this record
# cannot contribute to LTV calculation.
# Gold layer applies SQL Server OutstandingBalance as fallback for these records.
# AccountStatus issues do not block LTV eligibility.
# AccountStatus is a risk classification field not part of the LTV formula.

df_balance = df_balance.withColumn(
    "is_eligible_for_ltv",
    F.when(
        F.col("Balance_flag") == "BALANCE_UNAVAILABLE",
        F.lit(False)
    ).otherwise(
        F.lit(True)
    )
)

# Quick check
print("Step 6.7 complete - is_eligible_for_ltv distribution:")
df_balance.groupBy("is_eligible_for_ltv").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.8** - Build composite data_quality_flag


# CELL ********************

# --- Step 6.8: Build composite data_quality_flag ---
# Combines IsDuplicate_flag, AccountStatus_flag, and Balance_flag
# into a single composite column.
# is_eligible_for_ltv retained as a separate standalone column.
# Individual flag columns dropped after composite is built.

df_balance = df_balance.withColumn(
    "data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("AccountStatus_flag"),
                    F.col("Balance_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("AccountStatus_flag"),
                    F.col("Balance_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop("IsDuplicate_flag", "AccountStatus_flag", "Balance_flag")

# Quick check
print("Step 6.8 complete - Composite data_quality_flag distribution:")
df_balance.groupBy("data_quality_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.9** - Add audit columns


# CELL ********************

# --- Step 6.9: Add audit columns ---
# silver_ingestion_timestamp and pipeline_run_id defined in Section 1.
# Applied consistently to every Silver table in this notebook.

df_balance = df_balance \
    .withColumn(
        "silver_ingestion_timestamp",
        F.lit(SILVER_INGESTION_TIMESTAMP).cast(TimestampType())
    ).withColumn(
        "pipeline_run_id", F.lit(PIPELINE_RUN_ID)
    )

print("Step 6.9 complete - Audit columns added.")
print(f"  pipeline_run_id            : {PIPELINE_RUN_ID}")
print(f"  silver_ingestion_timestamp : {SILVER_INGESTION_TIMESTAMP}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.10** - Select and order final columns


# CELL ********************

# --- Step 6.10: Select and order final columns ---
# Explicit column selection ensures no intermediate helper columns
# leak into the Silver table.
# Column order convention:
#   Natural keys first, business columns, flag and eligibility columns,
#   audit columns last.

df_silver_bank_balance_update = df_balance.select(
    # Natural keys
    "LoanID",
    "ClientID",
    "DebtorID",

    # Business columns
    "ReportingDate",
    "CurrentOutstandingBalance",
    "LastPaymentDate",
    "LastPaymentAmount",
    "DaysPastDue",
    "AccountStatus",
    "SourceFileName",

    # Eligibility and flag columns
    "is_eligible_for_ltv",
    "data_quality_flag",

    # Audit columns
    "ingestion_timestamp",
    "source_system",
    "pipeline_run_id",
    "silver_ingestion_timestamp"
)

print("Step 6.10 complete - Final column selection confirmed.")
print(f"Total columns : {len(df_silver_bank_balance_update.columns)}")
print(f"Total rows    : {df_silver_bank_balance_update.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 6.11** - Print summary counts

# CELL ********************

# --- Step 6.11: Print summary counts ---

total               = df_silver_bank_balance_update.count()
clean               = df_silver_bank_balance_update.filter(F.col("data_quality_flag") == "CLEAN").count()
flagged             = df_silver_bank_balance_update.filter(F.col("data_quality_flag") != "CLEAN").count()
balance_unavailable = df_silver_bank_balance_update.filter(F.col("data_quality_flag").contains("BALANCE_UNAVAILABLE")).count()
unknown_status      = df_silver_bank_balance_update.filter(F.col("data_quality_flag").contains("UNKNOWN_ACCOUNT_STATUS")).count()
suspect_status      = df_silver_bank_balance_update.filter(F.col("data_quality_flag").contains("SUSPECT_ACCOUNT_STATUS")).count()
eligible            = df_silver_bank_balance_update.filter(F.col("is_eligible_for_ltv") == True).count()
ineligible          = df_silver_bank_balance_update.filter(F.col("is_eligible_for_ltv") == False).count()
duplicates_removed  = duplicate_count_balance

print("=" * 55)
print("silver_bank_balance_update SUMMARY")
print("=" * 55)
print(f"  Total records              : {total}")
print(f"  Clean records              : {clean}")
print(f"  Flagged records            : {flagged}")
print(f"  Balance unavailable        : {balance_unavailable}")
print(f"  Unknown account status     : {unknown_status}")
print(f"  Suspect account status     : {suspect_status}")
print(f"  Eligible for LTV           : {eligible}")
print(f"  Ineligible for LTV         : {ineligible}")
print(f"  Duplicates removed         : {duplicates_removed}")
print("=" * 55)
print("Section 6 complete. df_silver_bank_balance_update ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Section 7: Clean and Transform `silver_market_prices`
# 
# Produces `silver_market_prices` from `bronze_market_prices`.
# Kept separate as a reference table joined at Gold by TickerSymbol and PriceDate.
# ClosePrice is the value used in the LTV denominator calculation:
# Total Collateral Value = Sum of (QuantityHeld * ClosePrice) per debtor.
# 
# ### Steps
# - **Step 7.1** - Deduplicate on TickerSymbol + PriceDate
# - **Step 7.2** - Baseline normalisation: trim and uppercase TickerSymbol
# - **Step 7.3** - Flag NULL or zero ClosePrice
# - **Step 7.4** - Derive AssetType from TickerSymbol
# - **Step 7.5** - Cast PriceDate from TIMESTAMP to DATE
# - **Step 7.6** - Cast price columns from DOUBLE to DECIMAL
# - **Step 7.7** - Cast Volume from DOUBLE to LONG
# - **Step 7.8** - Build composite data_quality_flag
# - **Step 7.9** - Add audit columns
# - **Step 7.10** - Select and order final columns
# - **Step 7.11** - Print summary counts

# MARKDOWN ********************

# ---
# ### **Step 7.1** - Deduplicate on TickerSymbol + PriceDate

# CELL ********************

# ============================================================
# SECTION 7: CLEAN AND TRANSFORM silver_market_prices
# ============================================================

# Start from the raw Bronze DataFrame
df_prices = df_bronze_market_prices

# --- Step 7.1: Deduplicate on TickerSymbol + PriceDate ---
# Profiling finding: no duplicates at time of profiling.
# Applied defensively because the pipeline runs daily and duplicates
# may accumulate over time.
# Rule: keep the record with the latest ingestion_timestamp per
# TickerSymbol + PriceDate combination.

window_dup = Window.partitionBy("TickerSymbol", "PriceDate").orderBy(
    F.col("ingestion_timestamp").desc()
)

df_prices = df_prices.withColumn(
    "row_rank", F.row_number().over(window_dup)
).withColumn(
    "IsDuplicate_flag",
    F.when(F.col("row_rank") == 1, F.lit("CLEAN"))
    .otherwise(F.lit("DUPLICATE_REMOVED"))
)

duplicate_count_prices = df_prices.filter(
    F.col("IsDuplicate_flag") == "DUPLICATE_REMOVED"
).count()

df_prices = df_prices.filter(F.col("row_rank") == 1).drop("row_rank")

print(f"Step 7.1 complete - Duplicates removed: {duplicate_count_prices}")
print(f"Rows after deduplication: {df_prices.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 7.2** - Baseline normalisation: trim and uppercase TickerSymbol

# MARKDOWN ********************


# CELL ********************

# --- Step 7.2: Baseline normalisation - trim and uppercase TickerSymbol ---
# Applying trim to all STRING columns defensively before any transformation.
# Uppercase TickerSymbol to ensure consistent join with silver_collateral
# which also stores tickers in uppercase after Step 5.13 corrections.
# A case mismatch on TickerSymbol produces a silent null join at Gold.

string_columns_prices = [
    field.name for field in df_prices.schema.fields
    if field.dataType.typeName() == "string"
]

for col_name in string_columns_prices:
    df_prices = df_prices.withColumn(
        col_name, F.trim(F.col(col_name))
    )

df_prices = df_prices.withColumn(
    "TickerSymbol", F.upper(F.col("TickerSymbol"))
)

print("Step 7.2 complete - Baseline normalisation applied.")
print(f"STRING columns trimmed: {string_columns_prices}")
print("\nTickerSymbol distribution after uppercase:")
df_prices.groupBy("TickerSymbol").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 7.3** - Flag NULL or zero ClosePrice

# CELL ********************

# --- Step 7.3: Flag NULL or zero ClosePrice ---
# Profiling finding: 2 rows with all OHLC values NULL simultaneously.
# A NULL or zero ClosePrice produces NULL or zero collateral value in LTV.
# This completely distorts the LTV ratio for that debtor.
# These records must not be used in LTV calculation.
# Flag here. Gold layer applies the 5 business day fallback rule:
# use most recent non-null ClosePrice within 5 business days.
# If none found within 5 days, flag collateral record as STALE_DATA.

df_prices = df_prices.withColumn(
    "Price_flag",
    F.when(
        F.col("Close").isNull() | (F.col("Close") == 0),
        F.lit("INVALID_PRICE")
    ).otherwise(
        F.lit("CLEAN")
    )
)

# Quick check
print("Step 7.3 complete - Price flag distribution:")
df_prices.groupBy("Price_flag").count().show()

print("Invalid price records:")
df_prices.filter(F.col("Price_flag") == "INVALID_PRICE").select(
    "TickerSymbol", "PriceDate", "Close", "High", "Low", "Open", "Price_flag"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# ### **Step 7.4** - Derive AssetType from TickerSymbol

# CELL ********************

# --- Step 7.4: Derive AssetType from TickerSymbol ---
# bronze_market_prices does not contain an AssetType column.
# The yfinance API does not return asset type information.
# AssetType is derived from TickerSymbol using a simple rule:
#   BTC-USD -> Crypto
#   All others -> Stock
# This column supports downstream filtering and segmentation
# in the Gold layer and Power BI Risk Command Centre dashboard.

df_prices = df_prices.withColumn(
    "AssetType",
    F.when(
        F.col("TickerSymbol") == "BTC-USD",
        F.lit("Crypto")
    ).otherwise(
        F.lit("Stock")
    )
)

# Quick check
print("Step 7.4 complete - AssetType distribution:")
df_prices.groupBy("AssetType", "TickerSymbol").count().orderBy(
    "AssetType", "TickerSymbol"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 7.5** - Cast PriceDate from TIMESTAMP to DATE

# MARKDOWN ********************


# CELL ********************

# --- Step 7.5: Cast PriceDate from TIMESTAMP to DATE ---
# Profiling finding: PriceDate stored as TIMESTAMP with time component
# 00:00:00. Time component carries no information.
# PriceDate is the join key between silver_market_prices and
# silver_debtor_loan_collateral at Gold layer.
# A TIMESTAMP to DATE type mismatch causes silent join failures.
# F.to_date() strips the time component and produces a clean DATE value.

df_prices = df_prices.withColumn(
    "PriceDate",
    F.to_date(F.col("PriceDate"))
)

# Quick check
print("Step 7.5 complete - PriceDate type after cast:")
df_prices.select("PriceDate").printSchema()

print("Sample PriceDate values after cast:")
df_prices.select(
    "TickerSymbol", "PriceDate", "Close"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 7.6** - Cast price columns from DOUBLE to DECIMAL

# CELL ********************

# --- Step 7.6: Cast price columns from DOUBLE to DECIMAL ---
# Profiling finding: Close, High, Low, Open all stored as DOUBLE.
# Sample output from Step 7.5 confirmed floating point imprecision:
# 214.30596923828125 instead of 214.31.
# DecimalType(18,6) chosen over DecimalType(18,2) because:
#   Equity prices: 2 decimal places sufficient
#   Crypto prices: BTC-USD trades at values requiring more precision
#   6 decimal places preserves meaningful crypto precision without
#   unnecessary storage overhead.
# Close is the value used in the LTV denominator calculation.
# All OHLC columns cast for consistency.

df_prices = df_prices \
    .withColumn("Close", F.col("Close").cast(DecimalType(18, 6))) \
    .withColumn("High",  F.col("High").cast(DecimalType(18, 6))) \
    .withColumn("Low",   F.col("Low").cast(DecimalType(18, 6))) \
    .withColumn("Open",  F.col("Open").cast(DecimalType(18, 6)))

# Quick check
print("Step 7.6 complete - Price column types after cast:")
df_prices.select("Close", "High", "Low", "Open").printSchema()

print("Sample Close values after cast:")
df_prices.select(
    "TickerSymbol", "PriceDate", "Close"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 7.7** - Cast Volume from DOUBLE to LONG

# CELL ********************

# --- Step 7.7: Cast Volume from DOUBLE to LONG ---
# Volume is a count of shares or units traded.
# It is always a whole number. No exchange records fractional volume.
# DOUBLE is the wrong type for a count field.
# LONG is the correct integer type for large whole numbers in PySpark.
# BTC-USD volume can exceed billions so INTEGER is insufficient.
# LONG supports values up to 9.2 quadrillion which is sufficient.

df_prices = df_prices.withColumn(
    "Volume",
    F.col("Volume").cast(LongType())
)

# Quick check
print("Step 7.7 complete - Volume type after cast:")
df_prices.select("Volume").printSchema()

print("Sample Volume values after cast:")
df_prices.select(
    "TickerSymbol", "PriceDate", "Volume"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###  **Step 7.8** - Build composite data_quality_flag


# CELL ********************

# --- Step 7.8: Build composite data_quality_flag ---
# Combines IsDuplicate_flag and Price_flag into a single composite column.
# AssetType is a derived business column not a quality flag.
# Individual flag columns dropped after composite is built.

df_prices = df_prices.withColumn(
    "data_quality_flag",
    F.when(
        F.size(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("Price_flag")
                ),
                "CLEAN"
            )
        ) == 0,
        F.lit("CLEAN")
    ).otherwise(
        F.array_join(
            F.array_remove(
                F.array(
                    F.col("IsDuplicate_flag"),
                    F.col("Price_flag")
                ),
                "CLEAN"
            ),
            "|"
        )
    )
).drop("IsDuplicate_flag", "Price_flag")

# Quick check
print("Step 7.8 complete - Composite data_quality_flag distribution:")
df_prices.groupBy("data_quality_flag").count().orderBy(
    "count", ascending=False
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 7.9** - Add audit columns


# CELL ********************

# --- Step 7.9: Add audit columns ---
# silver_ingestion_timestamp and pipeline_run_id defined in Section 1.
# Applied consistently to every Silver table in this notebook.

df_prices = df_prices \
    .withColumn(
        "silver_ingestion_timestamp",
        F.lit(SILVER_INGESTION_TIMESTAMP).cast(TimestampType())
    ).withColumn(
        "pipeline_run_id", F.lit(PIPELINE_RUN_ID)
    )

print("Step 7.9 complete - Audit columns added.")
print(f"  pipeline_run_id            : {PIPELINE_RUN_ID}")
print(f"  silver_ingestion_timestamp : {SILVER_INGESTION_TIMESTAMP}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 7.10** - Select and order final columns


# CELL ********************

# --- Step 7.10: Select and order final columns ---
# Explicit column selection ensures no intermediate helper columns
# leak into the Silver table.
# Column order convention:
#   Natural keys first, business columns, derived columns,
#   flag column, audit columns last.

df_silver_market_prices = df_prices.select(
    # Natural keys
    "TickerSymbol",
    "PriceDate",

    # Business columns
    "Close",
    "High",
    "Low",
    "Open",
    "Volume",

    # Derived column
    "AssetType",

    # Flag column
    "data_quality_flag",

    # Audit columns
    "ingestion_timestamp",
    "source_system",
    "pipeline_run_id",
    "silver_ingestion_timestamp"
)

print("Step 7.10 complete - Final column selection confirmed.")
print(f"Total columns : {len(df_silver_market_prices.columns)}")
print(f"Total rows    : {df_silver_market_prices.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### **Step 7.11** - Print summary counts

# CELL ********************

# --- Step 7.11: Print summary counts ---

total           = df_silver_market_prices.count()
clean           = df_silver_market_prices.filter(F.col("data_quality_flag") == "CLEAN").count()
flagged         = df_silver_market_prices.filter(F.col("data_quality_flag") != "CLEAN").count()
invalid_price   = df_silver_market_prices.filter(F.col("data_quality_flag").contains("INVALID_PRICE")).count()
crypto_records  = df_silver_market_prices.filter(F.col("AssetType") == "Crypto").count()
stock_records   = df_silver_market_prices.filter(F.col("AssetType") == "Stock").count()
distinct_tickers = df_silver_market_prices.select("TickerSymbol").distinct().count()
date_range_min  = df_silver_market_prices.agg(F.min("PriceDate")).collect()[0][0]
date_range_max  = df_silver_market_prices.agg(F.max("PriceDate")).collect()[0][0]
duplicates_removed = duplicate_count_prices

print("=" * 55)
print("silver_market_prices SUMMARY")
print("=" * 55)
print(f"  Total records              : {total}")
print(f"  Clean records              : {clean}")
print(f"  Flagged records            : {flagged}")
print(f"  Invalid price records      : {invalid_price}")
print(f"  Crypto records             : {crypto_records}")
print(f"  Stock records              : {stock_records}")
print(f"  Distinct tickers           : {distinct_tickers}")
print(f"  Price date range           : {date_range_min} to {date_range_max}")
print(f"  Duplicates removed         : {duplicates_removed}")
print("=" * 55)
print("Section 7 complete. df_silver_market_prices ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Section 8: Write All Silver Tables to Delta
# 
# Writes all 5 Silver DataFrames to managed Delta tables in the Lakehouse.
# Each table uses a write strategy appropriate to its nature and purpose.
# 
# | Table | Strategy | Reason |
# |---|---|---|
# | `silver_collections_officer` | Full overwrite | Small reference table, overwrite guarantees current state |
# | `silver_officer_client_mapping` | Delta merge with SCD Type 2 | Assignment history must be preserved |
# | `silver_debtor_loan_collateral` | Delta merge with SCD Type 2 | Loan status history must be preserved |
# | `silver_bank_balance_update` | Append with watermark | Timeseries data, history must be preserved |
# | `silver_market_prices` | Append with watermark | Timeseries data, history must be preserved |
# 
# ### Steps
# - **Step 8.1** - Write `silver_collections_officer` (full overwrite)
# - **Step 8.2** - Write `silver_officer_client_mapping` (Delta merge SCD Type 2)
# - **Step 8.3** - Write `silver_debtor_loan_collateral` (Delta merge SCD Type 2)
# - **Step 8.4** - Write `silver_bank_balance_update` (append with watermark)
# - **Step 8.5** - Write `silver_market_prices` (append with watermark)


# MARKDOWN ********************

# ---
# 
# ### **Step 8.1** - Write `silver_collections_officer` (full overwrite)


# CELL ********************

# ============================================================
# SECTION 8: WRITE ALL SILVER TABLES TO DELTA
# ============================================================

# --- Step 8.1: Write silver_collections_officer ---
# Strategy: full overwrite on every run.
# Rationale: small reference table with 20 records.
# Overwriting guarantees Silver always reflects the current state
# of the Bronze source without accumulating stale records.
# SCD Type 2 is not needed here because assignment history is
# tracked in silver_officer_client_mapping, not on the officer record itself.

df_silver_collections_officer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_collections_officer")

# Verify write
count = spark.table("silver_collections_officer").count()
print("Step 8.1 complete - silver_collections_officer written.")
print(f"  Rows written : {count}")
print(f"  Expected     : 20")
print(f"  Match        : {count == 20}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 8.2** - Write `silver_officer_client_mapping` (Delta merge SCD Type 2)


# CELL ********************

# --- Step 8.2: Write silver_officer_client_mapping ---
# Strategy: Delta merge with SCD Type 2 logic.
# Natural key: OfficerID + ClientID composite.
# Tracked column: IsActive, officer_status_at_load.
# On first run: write entire DataFrame as new table.
# On subsequent runs: merge incoming records against existing table.
#   - Match found + tracked column changed:
#       Close old row (EffectiveEndDate = today, IsCurrent = False)
#       Insert new row (new EffectiveStartDate, IsCurrent = True)
#   - No match found: insert as new record.

from delta.tables import DeltaTable

# Check if table exists
table_exists = spark.catalog.tableExists("silver_officer_client_mapping")

if not table_exists:
    # First run: write entire DataFrame as new table
    df_silver_officer_client_mapping.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_officer_client_mapping")
    print("Step 8.2 - First run: silver_officer_client_mapping created.")

else:
    # Subsequent runs: Delta merge with SCD Type 2 logic
    delta_table = DeltaTable.forName(spark, "silver_officer_client_mapping")

    # Step 1: Close old rows where tracked columns have changed
    delta_table.alias("existing") \
        .merge(
            df_silver_officer_client_mapping.alias("incoming"),
            "existing.OfficerID = incoming.OfficerID AND \
             existing.ClientID = incoming.ClientID AND \
             existing.IsCurrent = true"
        ) \
        .whenMatchedUpdate(
            condition="""
                existing.IsActive != incoming.IsActive OR
                existing.officer_status_at_load != incoming.officer_status_at_load
            """,
            set={
                "EffectiveEndDate" : F.current_date(),
                "IsCurrent"        : F.lit(False)
            }
        ) \
        .execute()

    # Step 2: Insert new rows for changed records and brand new records
    # We insert all incoming records that do not already exist as current
    existing_current = delta_table.toDF().filter(F.col("IsCurrent") == True)

    new_rows = df_silver_officer_client_mapping.join(
        existing_current.select("OfficerID", "ClientID", "IsActive", "officer_status_at_load"),
        on=["OfficerID", "ClientID"],
        how="left_anti"
    )

    changed_rows = df_silver_officer_client_mapping.join(
        existing_current.select(
            "OfficerID", "ClientID", "IsActive", "officer_status_at_load"
        ).withColumnRenamed("IsActive", "existing_IsActive") \
         .withColumnRenamed("officer_status_at_load", "existing_officer_status"),
        on=["OfficerID", "ClientID"],
        how="inner"
    ).filter(
        (F.col("IsActive") != F.col("existing_IsActive")) |
        (F.col("officer_status_at_load") != F.col("existing_officer_status"))
    ).drop("existing_IsActive", "existing_officer_status")

    rows_to_insert = new_rows.union(changed_rows)

    if rows_to_insert.count() > 0:
        rows_to_insert.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver_officer_client_mapping")

    print("Step 8.2 - Subsequent run: silver_officer_client_mapping merged.")

# Verify write
count = spark.table("silver_officer_client_mapping").count()
print(f"Step 8.2 complete - silver_officer_client_mapping written.")
print(f"  Rows in table : {count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 8.3** - Write `silver_debtor_loan_collateral` (Delta merge SCD Type 2)


# CELL ********************

# --- Step 8.3: Write silver_debtor_loan_collateral ---
# Strategy: Delta merge with SCD Type 2 logic.
# Natural key: DebtorID + LoanID + CollateralID composite.
# Tracked column: LoanStatus.
# On first run: write entire DataFrame as new table.
# On subsequent runs: merge incoming records against existing table.
#   - Match found + LoanStatus changed:
#       Close old row (EffectiveEndDate = today, IsCurrent = False)
#       Insert new row (new EffectiveStartDate, IsCurrent = True)
#   - No match found: insert as new record.

table_exists_dlc = spark.catalog.tableExists("silver_debtor_loan_collateral")

if not table_exists_dlc:
    # First run: write entire DataFrame as new table
    df_silver_debtor_loan_collateral.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_debtor_loan_collateral")
    print("Step 8.3 - First run: silver_debtor_loan_collateral created.")

else:
    # Subsequent runs: Delta merge with SCD Type 2 logic
    delta_table_dlc = DeltaTable.forName(spark, "silver_debtor_loan_collateral")

    # Step 1: Close old rows where LoanStatus has changed
    delta_table_dlc.alias("existing") \
        .merge(
            df_silver_debtor_loan_collateral.alias("incoming"),
            "existing.DebtorID = incoming.DebtorID AND \
             existing.LoanID = incoming.LoanID AND \
             existing.CollateralID = incoming.CollateralID AND \
             existing.IsCurrent = true"
        ) \
        .whenMatchedUpdate(
            condition="existing.LoanStatus != incoming.LoanStatus",
            set={
                "EffectiveEndDate" : F.current_date(),
                "IsCurrent"        : F.lit(False)
            }
        ) \
        .execute()

    # Step 2: Insert new rows for changed and brand new records
    existing_current_dlc = delta_table_dlc.toDF().filter(
        F.col("IsCurrent") == True
    )

    new_rows_dlc = df_silver_debtor_loan_collateral.join(
        existing_current_dlc.select("DebtorID", "LoanID", "CollateralID", "LoanStatus"),
        on=["DebtorID", "LoanID", "CollateralID"],
        how="left_anti"
    )

    changed_rows_dlc = df_silver_debtor_loan_collateral.join(
        existing_current_dlc.select(
            "DebtorID", "LoanID", "CollateralID", "LoanStatus"
        ).withColumnRenamed("LoanStatus", "existing_LoanStatus"),
        on=["DebtorID", "LoanID", "CollateralID"],
        how="inner"
    ).filter(
        F.col("LoanStatus") != F.col("existing_LoanStatus")
    ).drop("existing_LoanStatus")

    rows_to_insert_dlc = new_rows_dlc.union(changed_rows_dlc)

    if rows_to_insert_dlc.count() > 0:
        rows_to_insert_dlc.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver_debtor_loan_collateral")

    print("Step 8.3 - Subsequent run: silver_debtor_loan_collateral merged.")

# Verify write
count = spark.table("silver_debtor_loan_collateral").count()
print(f"Step 8.3 complete - silver_debtor_loan_collateral written.")
print(f"  Rows in table : {count}")
print(f"  Expected      : 300")
print(f"  Match         : {count == 300}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 8.4** - Write `silver_bank_balance_update` (append with watermark)


# CELL ********************

# --- Step 8.4: Write silver_bank_balance_update ---
# Strategy: append with watermark on ReportingDate.
# Rationale: timeseries data. New balance files arrive daily from client banks.
# Historical records must be preserved for trend analysis and audit.
# Watermark prevents duplicate records accumulating across pipeline runs.
# On first run: write entire DataFrame as new table.
# On subsequent runs: append only records where ReportingDate
# exceeds the maximum ReportingDate already in the Silver table.

table_exists_balance = spark.catalog.tableExists("silver_bank_balance_update")

if not table_exists_balance:
    # First run: write entire DataFrame as new table
    df_silver_bank_balance_update.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_bank_balance_update")
    print("Step 8.4 - First run: silver_bank_balance_update created.")

else:
    # Subsequent runs: append with watermark
    max_reporting_date = spark.table("silver_bank_balance_update") \
        .agg(F.max("ReportingDate")).collect()[0][0]

    print(f"Step 8.4 - Watermark: max ReportingDate in Silver = {max_reporting_date}")

    new_records = df_silver_bank_balance_update.filter(
        F.col("ReportingDate") > max_reporting_date
    )

    new_count = new_records.count()
    print(f"Step 8.4 - New records to append: {new_count}")

    if new_count > 0:
        new_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver_bank_balance_update")

    print("Step 8.4 - Subsequent run: silver_bank_balance_update appended.")

# Verify write
count = spark.table("silver_bank_balance_update").count()
print(f"Step 8.4 complete - silver_bank_balance_update written.")
print(f"  Rows in table : {count}")
print(f"  Expected      : 956")
print(f"  Match         : {count == 956}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 8.5** - Write `silver_market_prices` (append with watermark)

# CELL ********************

# --- Step 8.5: Write silver_market_prices ---
# Strategy: append with watermark on PriceDate.
# Rationale: timeseries data. New price records arrive daily from yfinance.
# Historical price records must be preserved for collateral value trend
# analysis in the Power BI Risk Command Centre dashboard.
# Watermark prevents duplicate price records accumulating across runs.
# On first run: write entire DataFrame as new table.
# On subsequent runs: append only records where PriceDate
# exceeds the maximum PriceDate already in the Silver table.

table_exists_prices = spark.catalog.tableExists("silver_market_prices")

if not table_exists_prices:
    # First run: write entire DataFrame as new table
    df_silver_market_prices.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_market_prices")
    print("Step 8.5 - First run: silver_market_prices created.")

else:
    # Subsequent runs: append with watermark
    max_price_date = spark.table("silver_market_prices") \
        .agg(F.max("PriceDate")).collect()[0][0]

    print(f"Step 8.5 - Watermark: max PriceDate in Silver = {max_price_date}")

    new_records = df_silver_market_prices.filter(
        F.col("PriceDate") > max_price_date
    )

    new_count = new_records.count()
    print(f"Step 8.5 - New records to append: {new_count}")

    if new_count > 0:
        new_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver_market_prices")

    print("Step 8.5 - Subsequent run: silver_market_prices appended.")

# Verify write
count = spark.table("silver_market_prices").count()
print(f"Step 8.5 complete - silver_market_prices written.")
print(f"  Rows in table  : {count}")
print(f"  Expected       : 1888")
print(f"  Match          : {count == 1888}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables = [
    "silver_collections_officer",
    "silver_officer_client_mapping",
    "silver_debtor_loan_collateral",
    "silver_bank_balance_update",
    "silver_market_prices"
]

print("=" * 55)
print("SILVER LAYER - ALL TABLES VERIFICATION")
print("=" * 55)
for table in silver_tables:
    count = spark.table(table).count()
    print(f"  {table:<40} {count:>6} rows")
print("=" * 55)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
