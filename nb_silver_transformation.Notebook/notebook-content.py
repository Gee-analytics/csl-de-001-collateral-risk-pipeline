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

# CELL ********************

df_loan.printSchema()

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

# MARKDOWN ********************


# CELL ********************

demo_silver_debtor_loan_collateral_1 = df_bronze_debtor \
                        .join(df_bronze_loan, on = "DebtorID", how="left") \
                        .join(df_bronze_collateral, on="LoanID", how="left")


# demo_silver_debtor_loan_collateral_1.show()


total_rows = demo_silver_debtor_loan_collateral_1.count()

print(f"The DataFrame has {total_rows} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

demo_silver_debtor_loan_collateral_2 = df_bronze_debtor \
                        .join(df_bronze_loan, on = "DebtorID", how="inner") \
                        .join(df_bronze_collateral, on="LoanID", how="left")


# demo_silver_debtor_loan_collateral_1.show()


total_rows = demo_silver_debtor_loan_collateral_2.count()

print(f"The DataFrame has {total_rows} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_bronze_loan.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_bronze_collateral.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mapping.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_officer.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
