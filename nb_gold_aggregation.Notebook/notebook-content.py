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

# # `nb_gold_aggregation` - Gold Layer Aggregation Notebook
# **CSL-DE-001 | Collection Solutions Limited | Data Engineering Division**
# 
# Reads from 5 confirmed Silver tables and produces 8 Gold tables forming a
# production-ready star schema powering the Power BI Risk Command Centre dashboard.
# This is the final transformation layer of the pipeline. No data moves beyond this point.
# 
# All business logic implemented here traces directly to
# `CSL_DE_001_Business_Rules_v1.0.md`. Any deviation from that document is
# explicitly flagged as a gap.
# 
# `gold_pipeline_metadata` is the Bronze watermark store, seeded by
# `nb_setup_pipeline_metadata`, and is not written to from this notebook.
# All Gold execution audit rows are written to `gold_audit_log` instead.
# 
# ### Inputs (during development)
# - `silver_debtor_loan_collateral` - 300 records, SCD Type 2
# - `silver_bank_balance_update` - 956 records, append with watermark
# - `silver_market_prices` - 1,888 records, append with watermark
# - `silver_collections_officer` - 20 records, full overwrite
# - `silver_officer_client_mapping` - 53 records, SCD Type 2
# 
# ### Outputs
# | Table | Type | Write Strategy |
# |---|---|---|
# | `fact_ltv_daily_snapshot` | Fact | Append with idempotency guard |
# | `fact_market_prices_daily` | Fact | Overwrite first run, watermark append |
# | `dim_debtor` | Dimension | SCD Type 2 merge |
# | `dim_loan` | Dimension | SCD Type 2 merge |
# | `dim_collateral_asset` | Dimension | SCD Type 2 merge |
# | `dim_collections_officer` | Dimension | SCD Type 2 merge |
# | `dim_client_bank` | Dimension | Full overwrite |
# | `dim_date` | Dimension | Write once |
# | `gold_audit_log` | Audit | Append |
# 
# ### Notebook Structure
# - **Section 1** - Imports and configuration
# - **Section 2** - Read all Silver input tables
# - **Section 3** - Populate `dim_date`
# - **Section 4** - Populate `dim_collections_officer` with SCD Type 2
# - **Section 5** - Populate `dim_client_bank`
# - **Section 6** - Populate `dim_debtor` with SCD Type 2
# - **Section 7** - Populate `dim_loan` with SCD Type 2
# - **Section 8** - Populate `dim_collateral_asset` with SCD Type 2
# - **Section 9** - Balance resolution and market price join
# - **Section 10** - LTV calculation, risk flagging, and shortfall computation
# - **Section 11** - Referential integrity validation
# - **Section 12** - Write to `fact_ltv_daily_snapshot`
# - **Section 13** - Write to `gold_audit_log`
# - **Section 14** - Populate `fact_market_prices_daily`


# MARKDOWN ********************

# ---
# 
# ---
# 
# ---
# ## Section 1: Imports and Configuration
# Establishes the Spark session, run identity, all Lakehouse path constants,
# business rule constants, and the audit log accumulator used throughout the notebook.
# 
# Declared once here. Referenced everywhere else. If a path or threshold changes,
# it changes in one place only.
# 
# ### Steps
# - **Step 1.1** - Standard library imports
# - **Step 1.2** - PySpark imports
# - **Step 1.3** - Spark session confirmation
# - **Step 1.4** - Run identity: RunID, pipeline run date, run timestamp
# - **Step 1.5** - Lakehouse name
# - **Step 1.6** - Silver input table paths
# - **Step 1.7** - Gold output table paths
# - **Step 1.8** - LTV threshold constants
# - **Step 1.9** - Stale price lookback window
# - **Step 1.10** - Loan status scope
# - **Step 1.11** - Audit log accumulator


# CELL ********************

# =============================================================================
# SECTION 1: IMPORTS AND CONFIGURATION
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 1.1 Standard library imports ---
import uuid
from datetime import date, datetime, timedelta

# --- 1.2 PySpark imports ---
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    DateType, BooleanType, TimestampType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --- 1.3 Spark session confirmation ---
# In Microsoft Fabric, spark is provided automatically by the runtime.
# We confirm it is active here rather than instantiate it.
print(f"Spark version       : {spark.version}")
print(f"App name            : {spark.sparkContext.appName}")

# --- 1.4 Run identity ---
# One UUID per notebook execution. Every gold_audit_log row for this run
# carries this same RunID, making the full run reconstructible from audit logs.
RUN_ID             = str(uuid.uuid4())
PIPELINE_RUN_DATE  = date.today()
RUN_TIMESTAMP      = datetime.now()

print(f"\nRun ID             : {RUN_ID}")
print(f"Pipeline run date  : {PIPELINE_RUN_DATE}")
print(f"Run timestamp      : {RUN_TIMESTAMP}")

# --- 1.5 Lakehouse name ---
LAKEHOUSE_NAME = "CSL_Collateral_Risk_LH"

# --- 1.6 Silver input table paths ---
SILVER_DEBTOR_LOAN_COLLATERAL = "Tables/dbo/silver_debtor_loan_collateral"
SILVER_BANK_BALANCE_UPDATE    = "Tables/dbo/silver_bank_balance_update"
SILVER_MARKET_PRICES          = "Tables/dbo/silver_market_prices"
SILVER_COLLECTIONS_OFFICER    = "Tables/dbo/silver_collections_officer"
SILVER_OFFICER_CLIENT_MAPPING = "Tables/dbo/silver_officer_client_mapping"


# Silver reference table - dim_client_bank sources from Silver not Bronze
# Silver has applied type casting, trimming, and audit columns.
# Sourcing from Bronze would break the architectural contract that Gold reads from Silver only.
SILVER_CLIENT_BANK = "Tables/dbo/silver_client_bank"


# --- 1.7 Gold output table names ---
# Written using saveAsTable. Fabric assigns dbo schema automatically.
# Do NOT use file paths here.
GOLD_FACT_LTV_SNAPSHOT        = "fact_ltv_daily_snapshot"
GOLD_DIM_DEBTOR               = "dim_debtor"
GOLD_DIM_LOAN                 = "dim_loan"
GOLD_DIM_COLLATERAL_ASSET     = "dim_collateral_asset"
GOLD_DIM_COLLECTIONS_OFFICER  = "dim_collections_officer"
GOLD_DIM_CLIENT_BANK          = "dim_client_bank"
GOLD_DIM_DATE                 = "dim_date"
GOLD_FACT_QUARANTINE_LOG      = "fact_ltv_quarantine_log"
GOLD_AUDIT_LOG                = "gold_audit_log"

# Gold audit table - all execution audit rows from this notebook write here
GOLD_AUDIT_LOG                = "gold_audit_log"

# Watermark store - READ ONLY from this notebook
# Owned by nb_setup_pipeline_metadata and written to by the Stream A pipeline only.
# Note: named gold_pipeline_metadata during Bronze build. Functions as the
# Bronze watermark store. Not written to from any transformation notebook.
GOLD_PIPELINE_METADATA        = "Tables/dbo.gold_pipeline_metadata"

# --- 1.8 LTV threshold constants ---
# Source: CSL_DE_001_Business_Rules_v1.0.md, Section 4
LTV_LOW_UPPER    = 0.60   # LTV < 0.60            = LOW
LTV_MEDIUM_UPPER = 0.80   # 0.60 <= LTV < 0.80    = MEDIUM
LTV_HIGH_UPPER   = 1.00   # 0.80 <= LTV < 1.00    = HIGH, >= 1.00 = CRITICAL

# --- 1.9 Stale price lookback window ---
# Source: CSL_DE_001_Business_Rules_v1.0.md, Section 6.2
STALE_PRICE_LOOKBACK_DAYS = 5

# --- 1.10 Loan status scope ---
# Source: CSL_DE_001_Business_Rules_v1.0.md, Section 10
# Settled loans are excluded from LTV calculation.
LTV_ELIGIBLE_LOAN_STATUSES = ["Active", "Defaulted"]

# --- 1.11 Audit log accumulator ---
# Each section appends a dict here. Section 13 writes the full list
# to gold_audit_log in one batch at the end of the notebook.
# gold_pipeline_metadata is never written to from this notebook.
audit_log = []

print("\nConfiguration complete. All paths, constants, and run identity confirmed.")
print("Ready to proceed to Section 2: Read Silver input tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## **Section 2:** Read All Silver Input Tables
# Reads all 5 Silver tables into Spark DataFrames. No transformations happen here.
# This section exists purely to load inputs and confirm row counts match expectations
# before any Gold logic runs. If a Silver table is empty or missing, we want to
# know immediately, not halfway through a transformation.
# 
# ### Steps
# - **Step 2.1** - Read `silver_debtor_loan_collateral`
# - **Step 2.2** - Read `silver_bank_balance_update`
# - **Step 2.3** - Read `silver_market_prices`
# - **Step 2.4** - Read `silver_collections_officer`
# - **Step 2.5** - Read `silver_officer_client_mapping`
# - **Step 2.6** - Read `bronze_client_bank`
# - **Step 2.7** - Row count validation

# CELL ********************

# =============================================================================
# SECTION 2: READ ALL SILVER INPUT TABLES
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 2.1 Read silver_debtor_loan_collateral ---
df_debtor_loan_collateral = spark.read.format("delta").load(SILVER_DEBTOR_LOAN_COLLATERAL)

# --- 2.2 Read silver_bank_balance_update ---
df_bank_balance_update = spark.read.format("delta").load(SILVER_BANK_BALANCE_UPDATE)

# --- 2.3 Read silver_market_prices ---
df_market_prices = spark.read.format("delta").load(SILVER_MARKET_PRICES)

# --- 2.4 Read silver_collections_officer ---
df_collections_officer = spark.read.format("delta").load(SILVER_COLLECTIONS_OFFICER)

# --- 2.5 Read silver_officer_client_mapping ---
df_officer_client_mapping = spark.read.format("delta").load(SILVER_OFFICER_CLIENT_MAPPING)


# --- 2.6 Read silver_client_bank ---
# Sources from Silver not Bronze to maintain the architectural contract
# that Gold reads exclusively from Silver layer tables.
df_silver_client_bank = spark.read.format("delta").load(SILVER_CLIENT_BANK)

# --- 2.7 Row count validation ---
expected_counts = {
    "silver_debtor_loan_collateral" : (df_debtor_loan_collateral, 300),
    "silver_bank_balance_update"    : (df_bank_balance_update,    956),
    "silver_market_prices"          : (df_market_prices,          1888),
    "silver_collections_officer"    : (df_collections_officer,    20),
    "silver_officer_client_mapping" : (df_officer_client_mapping, 53),
    "silver_client_bank"            : (df_silver_client_bank,      4),
}

print("=== Section 2: Silver Input Table Validation ===\n")

validation_passed = True

for table_name, (df, expected) in expected_counts.items():
    actual = df.count()
    status = "OK" if actual >= expected else "WARNING"
    if actual == 0:
        status = "FAILED"
        validation_passed = False
    print(f"  {status:<8} | {table_name:<35} | Expected >= {expected:<5} | Actual: {actual}")

print()

if not validation_passed:
    raise ValueError(
        "Section 2 validation failed. One or more Silver tables returned 0 rows. "
        "Investigate before proceeding. Gold notebook halted."
    )

print("All Silver input tables loaded successfully.")
print("Ready to proceed to Section 3: Populate dim_date.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ---
# ## Section 3: Populate `dim_date`
# Generates the date dimension programmatically. No Silver input required.<br>
# One row per calendar date covering 2018-01-01 to 2030-12-31.<br>
# Checks if the table already exists and is populated before writing.<br>
# Written once and never overwritten on subsequent pipeline runs.<br>
# 
# ### Steps
# - **Step 3.1** - Check if dim_date already exists and is populated
# - **Step 3.2** - Generate date spine from 2018-01-01 to 2030-12-31
# - **Step 3.3** - Derive all calendar attributes
# - **Step 3.4** - Write to dim_date using saveAsTable
# - **Step 3.5** - Print confirmation

# CELL ********************

# =============================================================================
# SECTION 3: POPULATE dim_date
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 3.1 Check if dim_date already exists and is populated ---
# dim_date is a static calendar table. It is generated once and never
# overwritten on subsequent pipeline runs. Checking before writing
# prevents duplication and unnecessary compute on every daily run.
# We use spark.catalog.tableExists() rather than attempting to load the path
# directly. Loading a non-existent path hits the OneLake REST API and returns
# a Java-level 400 Bad Request that Python cannot cleanly intercept.
# The catalog check queries the metastore registry only, no storage calls made.
dim_date_exists = False

try:
    if spark.catalog.tableExists(GOLD_DIM_DATE):
        # Table is registered in the metastore. Now check if it has rows.
        row_count = spark.table(GOLD_DIM_DATE).count()
        if row_count > 0:
            # Already populated. Nothing to do.
            dim_date_exists = True
            print(f"dim_date already populated with {row_count} rows. Skipping generation.")
        else:
            # Registered but empty. Proceed with generation.
            print("dim_date exists but is empty. Proceeding with generation.")
    else:
        # Not registered in metastore at all. Proceed with generation.
        print("dim_date does not exist yet. Proceeding with generation.")
except Exception as e:
    # Defensive catch. If catalog check itself fails, proceed with generation
    # and let the write step surface any underlying issue.
    print(f"Could not verify dim_date existence: {e}. Proceeding with generation.")

# --- 3.2 Generate date spine ---
# Only runs if dim_date does not already exist and is populated.
# Generates one row per calendar date from 2018-01-01 to 2030-12-31.
# Range covers full project history plus several years of future pipeline runs.
if not dim_date_exists:

    # Use Spark SQL sequence() to generate one row per day between two dates.
    # explode() unpacks the array into individual rows.
    df_date_spine = spark.sql("""
        SELECT explode(sequence(
            to_date('2018-01-01'),
            to_date('2030-12-31'),
            interval 1 day
        )) AS CalendarDate
    """)

    # --- 3.3 Derive all calendar attributes ---
    # DateKey is formatted as yyyyMMdd and cast to integer e.g. 20260326.
    # Integer DateKey is the standard Power BI pattern. Faster joins than
    # string keys and more storage efficient at fact table scale.
    # DayOfWeek in Spark: 1 = Sunday, 2 = Monday ... 7 = Saturday.
    # IsMarketDay models NYSE/NASDAQ weekday schedule only.
    # Public holidays are not modelled. Known gap - documented here.
    # A licensed holiday calendar would be required for production use.
    df_dim_date = df_date_spine.select(
        # Integer date key for fast joins from fact table
        F.date_format(F.col("CalendarDate"), "yyyyMMdd")
         .cast(IntegerType()).alias("DateKey"),
        F.col("CalendarDate"),
        F.dayofmonth(F.col("CalendarDate")).alias("DayOfMonth"),
        # Spark DayOfWeek: 1=Sunday, 7=Saturday
        F.dayofweek(F.col("CalendarDate")).alias("DayOfWeekNumber"),
        F.date_format(F.col("CalendarDate"), "EEEE").alias("DayOfWeek"),
        F.weekofyear(F.col("CalendarDate")).alias("WeekOfYear"),
        F.month(F.col("CalendarDate")).alias("MonthNumber"),
        F.date_format(F.col("CalendarDate"), "MMMM").alias("MonthName"),
        F.quarter(F.col("CalendarDate")).alias("Quarter"),
        # QuarterName concatenates literal "Q" with quarter number e.g. Q1
        F.concat(F.lit("Q"), F.quarter(F.col("CalendarDate"))).alias("QuarterName"),
        F.year(F.col("CalendarDate")).alias("Year"),
        # IsWeekend: TRUE where day is Sunday (1) or Saturday (7)
        F.when(F.dayofweek(F.col("CalendarDate")).isin(1, 7), True)
         .otherwise(False).alias("IsWeekend"),
        # IsBusinessDay: TRUE Monday through Friday only
        F.when(F.dayofweek(F.col("CalendarDate")).isin(1, 7), False)
         .otherwise(True).alias("IsBusinessDay"),
        # IsMarketDay: same as IsBusinessDay for NYSE/NASDAQ scope
        # Public holidays not modelled at this stage
        F.when(F.dayofweek(F.col("CalendarDate")).isin(1, 7), False)
         .otherwise(True).alias("IsMarketDay"),
    )

    # --- 3.4 Write to dim_date ---
    # saveAsTable registers the table in the Fabric metastore and writes
    # Delta files to OneLake. Fabric assigns the dbo schema automatically.
    # Mode is overwrite here as a safety net for the first run only.
    # Subsequent runs never reach this point due to the Step 3.1 check.
    df_dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_DIM_DATE)

    # --- 3.5 Print confirmation ---
    # Re-read row count from the written table to confirm the write succeeded.
    row_count = spark.table(GOLD_DIM_DATE).count()
    print(f"dim_date successfully written. Total rows: {row_count}")
    print("Ready to proceed to Section 4: Populate dim_collections_officer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4: Populate `dim_collections_officer` with SCD Type 2
# Builds the collections officer dimension by joining `silver_collections_officer`
# to `silver_officer_client_mapping` on OfficerID. Produces one row per officer
# per client assignment period, tracking assignment changes over time via SCD Type 2.
# 
# Powers Row Level Security in Power BI by mapping each officer to their
# assigned client portfolio and region.
# 
# ### Steps
# - **Step 4.1** - Join silver_collections_officer to silver_officer_client_mapping
# - **Step 4.2** - Generate OfficerSurrogateKey as SHA-256 hash
# - **Step 4.3** - Select and order final columns
# - **Step 4.4** - Apply SCD Type 2 merge into dim_collections_officer
# - **Step 4.5** - Print confirmation

# CELL ********************

# =============================================================================
# SECTION 4: POPULATE dim_collections_officer WITH SCD TYPE 2
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 4.1 Join silver_collections_officer to silver_officer_client_mapping ---
# silver_collections_officer holds officer personal and employment attributes.
# silver_officer_client_mapping holds the assignment history of which officer
# is responsible for which client bank portfolio and when.
# We join on OfficerID to produce one unified officer dimension record per
# assignment period. INNER JOIN is correct here because every mapping record
# must have a corresponding officer record. Orphaned mapping records with no
# parent officer are a data quality issue surfaced by this join naturally.
df_officer_joined = df_collections_officer.alias("o").join(
    df_officer_client_mapping.alias("m"),
    on="OfficerID",
    how="inner"
)

# --- 4.2 Generate OfficerSurrogateKey as SHA-256 hash ---
# Hash of OfficerID concatenated with EffectiveStartDate from the mapping table.
# Concatenating EffectiveStartDate ensures uniqueness across multiple versions
# of the same officer when their client assignment changes over time.
# Deterministic: same inputs always produce the same key regardless of reload.
df_officer_joined = df_officer_joined.withColumn(
    "OfficerSurrogateKey",
    F.sha2(
        F.concat_ws("|", F.col("OfficerID"), F.col("EffectiveStartDate")),
        256
    )
)

# --- 4.3 Select and order final columns ---
df_dim_collections_officer = df_officer_joined.select(
    F.col("OfficerSurrogateKey"),
    F.col("OfficerID"),
    F.col("o.FullName").alias("OfficerFullName"),
    F.col("o.Email").alias("OfficerEmail"),
    F.col("o.PhoneNumber").alias("OfficerPhoneNumber"),
    F.col("m.Region").alias("OfficerRegion"),
    F.col("o.Status").alias("OfficerStatus"),
    F.col("o.DateJoined"),
    F.col("m.ClientID"),
    F.col("m.EffectiveStartDate").alias("EffectiveStartDate"),
    F.col("m.EffectiveEndDate").alias("EffectiveEndDate"),
    F.col("m.IsCurrent"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 4.4 Apply SCD Type 2 merge into dim_collections_officer ---
# Check if dim_collections_officer already exists in the metastore.
# First run: write directly using saveAsTable.
# Subsequent runs: use DeltaTable merge to insert new versions and close
# out expired versions without touching unchanged records.

if not spark.catalog.tableExists(GOLD_DIM_COLLECTIONS_OFFICER):

    # First run - table does not exist yet. Write directly.
    df_dim_collections_officer.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_DIM_COLLECTIONS_OFFICER)
    print("dim_collections_officer created on first run.")

else:
    # Subsequent runs - table exists. Apply SCD Type 2 merge.
    # Match on OfficerSurrogateKey which is deterministic and unique per version.
    # WHEN MATCHED AND data has changed: update the existing record.
    # WHEN NOT MATCHED: insert the new version as a new row.
    delta_table = DeltaTable.forName(spark, GOLD_DIM_COLLECTIONS_OFFICER)

    delta_table.alias("target").merge(
        df_dim_collections_officer.alias("source"),
        "target.OfficerSurrogateKey = source.OfficerSurrogateKey"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_collections_officer updated via SCD Type 2 merge.")

# --- 4.5 Print confirmation ---
row_count = spark.table(GOLD_DIM_COLLECTIONS_OFFICER).count()
print(f"dim_collections_officer row count: {row_count}")
print("Ready to proceed to Section 5: Populate dim_client_bank.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5: Populate `dim_client_bank`
# Builds the client bank dimension from `silver_client_bank`.
# 4 rows representing the four client banks CSL manages portfolios for.
# Full overwrite on every run. No SCD Type 2 required given the static
# nature of this reference table and the low probability of structural change.
# 
# ### Steps
# - **Step 5.1** - Generate ClientSurrogateKey as SHA-256 hash of ClientID
# - **Step 5.2** - Select and order final columns
# - **Step 5.3** - Write to dim_client_bank using full overwrite
# - **Step 5.4** - Print confirmation


# CELL ********************

# =============================================================================
# SECTION 5: POPULATE dim_client_bank
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 5.1 Generate ClientSurrogateKey ---
# SHA-256 hash of ClientID only. No EffectiveStartDate concatenated because
# this is a full overwrite dimension with no versioning. ClientID is stable
# and unique across all 4 rows making it sufficient as the hash input.
df_dim_client_bank = df_silver_client_bank.withColumn(
    "ClientSurrogateKey",
    F.sha2(F.col("ClientID"), 256)
)

# --- 5.2 Select and order final columns ---
df_dim_client_bank = df_dim_client_bank.select(
    F.col("ClientSurrogateKey"),
    F.col("ClientID"),
    F.col("ClientName"),
    F.col("ContactEmail"),
    F.col("ContactPhone"),
    F.col("S3FolderPath"),
    F.col("IsActive").cast(BooleanType()).alias("IsActive"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 5.3 Write to dim_client_bank using full overwrite ---
# Full overwrite is justified here because:
# 1. Only 4 rows - compute cost is negligible
# 2. Static reference table - changes are rare and deliberate
# 3. No historical versioning required for client bank attributes
# 4. Simpler and more maintainable than SCD Type 2 for a table this size
df_dim_client_bank.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_DIM_CLIENT_BANK)

# --- 5.4 Print confirmation ---
row_count = spark.table(GOLD_DIM_CLIENT_BANK).count()
print(f"dim_client_bank written. Row count: {row_count}")
print("Ready to proceed to Section 6: Populate dim_debtor.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6: Populate `dim_debtor` with SCD Type 2
# Builds the debtor dimension from `silver_debtor_loan_collateral`.
# Deduplicates on DebtorID before building to avoid row inflation from
# the one-to-many relationship between debtors, loans, and collateral assets.
# One row per debtor per version. SCD Type 2 tracks attribute changes over time.
# PII fields PhoneNumber, EmailAddress, and ResidentialAddress pass through
# in readable form. Access is controlled via RLS at the Power BI semantic
# model level, not by hashing at the data layer.
# NationalID passes through as already hashed in Silver.
# 
# ### Steps
# - **Step 6.1** - Filter to IsCurrent = True and deduplicate on DebtorID
# - **Step 6.2** - Generate DebtorSurrogateKey as SHA-256 hash
# - **Step 6.3** - Select and order final columns
# - **Step 6.4** - Apply SCD Type 2 merge into dim_debtor
# - **Step 6.5** - Print confirmation


# CELL ********************

# =============================================================================
# SECTION 6: POPULATE dim_debtor WITH SCD TYPE 2
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 6.1 Filter to IsCurrent = True and deduplicate on DebtorID ---
# silver_debtor_loan_collateral has 300 rows at collateral asset grain.
# 1 debtor can have multiple loans, each loan can have multiple collateral
# assets producing multiple rows per debtor in the Silver table.
# Reading all 300 rows directly would inflate dim_debtor with duplicates.
# We filter to IsCurrent = True first to exclude expired SCD Type 2 versions,
# then deduplicate on DebtorID keeping only debtor-level attributes.
# row_number() over DebtorID ordered by EffectiveStartDate descending ensures
# we keep the most recent version of each debtor record where duplicates exist.

window_debtor = Window.partitionBy("DebtorID").orderBy(F.col("EffectiveStartDate").desc())

df_debtor_deduped = df_debtor_loan_collateral.filter(
    F.col("IsCurrent") == True
).withColumn(
    "row_num", F.row_number().over(window_debtor)
).filter(
    F.col("row_num") == 1
).drop("row_num")

# --- 6.2 Generate DebtorSurrogateKey ---
# SHA-256 hash of DebtorID concatenated with EffectiveStartDate.
# Deterministic and unique per debtor version.
df_debtor_deduped = df_debtor_deduped.withColumn(
    "DebtorSurrogateKey",
    F.sha2(
        F.concat_ws("|", F.col("DebtorID"), F.col("EffectiveStartDate")),
        256
    )
)

# --- 6.3 Select and order final columns ---
# PII fields pass through in readable form per the agreed PII strategy.
# Access controlled via RLS at Power BI semantic model level.
# NationalID is already hashed in Silver. No further action required here.
df_dim_debtor = df_debtor_deduped.select(
    F.col("DebtorSurrogateKey"),
    F.col("DebtorID"),
    F.col("ClientID"),
    F.col("AssignedOfficerID"),
    F.col("FullName"),
    F.col("NationalID"),
    F.col("PhoneNumber"),
    F.col("EmailAddress"),
    F.col("ResidentialAddress"),
    F.col("Region"),
    F.col("DateOnboarded"),
    F.col("EffectiveStartDate"),
    F.col("EffectiveEndDate"),
    F.col("IsCurrent"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 6.4 Apply SCD Type 2 merge into dim_debtor ---
# First run: write directly using saveAsTable.
# Subsequent runs: merge on DebtorSurrogateKey to insert new versions
# and update existing records where attributes have changed.
if not spark.catalog.tableExists(GOLD_DIM_DEBTOR):
    df_dim_debtor.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_DIM_DEBTOR)
    print("dim_debtor created on first run.")
else:
    delta_table = DeltaTable.forName(spark, GOLD_DIM_DEBTOR)
    delta_table.alias("target").merge(
        df_dim_debtor.alias("source"),
        "target.DebtorSurrogateKey = source.DebtorSurrogateKey"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_debtor updated via SCD Type 2 merge.")

# --- 6.5 Print confirmation ---
row_count = spark.table(GOLD_DIM_DEBTOR).count()
print(f"dim_debtor row count: {row_count}")
print("Ready to proceed to Section 7: Populate dim_loan.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 7: Populate `dim_loan` with SCD Type 2
# Builds the loan dimension from `silver_debtor_loan_collateral`.
# Deduplicates on LoanID to eliminate row inflation from the one-to-many
# relationship between loans and collateral assets.
# SCD Type 2 tracks loan status transitions over time e.g. Active to Defaulted.
# Expected row count: 250 distinct loans confirmed via SQL query.
# 
# ### Steps
# - **Step 7.1** - Filter to IsCurrent = True and deduplicate on LoanID
# - **Step 7.2** - Generate LoanSurrogateKey as SHA-256 hash
# - **Step 7.3** - Select and order final columns
# - **Step 7.4** - Apply SCD Type 2 merge into dim_loan
# - **Step 7.5** - Print confirmation


# CELL ********************

# =============================================================================
# SECTION 7: POPULATE dim_loan WITH SCD TYPE 2
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 7.1 Filter to IsCurrent = True and deduplicate on LoanID ---
# silver_debtor_loan_collateral has 300 rows at collateral asset grain.
# One loan can back multiple collateral assets producing duplicate LoanID
# rows in Silver. Deduplicating on LoanID before building dim_loan
# prevents row inflation in the dimension table.
# 250 distinct LoanIDs confirmed via SQL query before this section was built.
# row_number() over LoanID ordered by EffectiveStartDate descending keeps
# the most recent version of each loan record where duplicates exist.

window_loan = Window.partitionBy("LoanID").orderBy(F.col("EffectiveStartDate").desc())

df_loan_deduped = df_debtor_loan_collateral.filter(
    F.col("IsCurrent") == True
).withColumn(
    "row_num", F.row_number().over(window_loan)
).filter(
    F.col("row_num") == 1
).drop("row_num")

# --- 7.2 Generate LoanSurrogateKey ---
# SHA-256 hash of LoanID concatenated with EffectiveStartDate.
# Deterministic and unique per loan version.
# Concatenating EffectiveStartDate ensures uniqueness across multiple
# versions of the same loan when LoanStatus changes over time.
df_loan_deduped = df_loan_deduped.withColumn(
    "LoanSurrogateKey",
    F.sha2(
        F.concat_ws("|", F.col("LoanID"), F.col("EffectiveStartDate")),
        256
    )
)

# --- 7.3 Select and order final columns ---
# Loan-level attributes only. Debtor and collateral attributes excluded.
# They belong in dim_debtor and dim_collateral_asset respectively.
df_dim_loan = df_loan_deduped.select(
    F.col("LoanSurrogateKey"),
    F.col("LoanID"),
    F.col("DebtorID"),
    F.col("ClientID"),
    F.col("InitialLoanAmount"),
    F.col("OutstandingBalance"),
    F.col("LoanStartDate"),
    F.col("LoanMaturityDate"),
    F.col("LoanStatus"),
    F.col("DaysPastDue"),
    F.col("LastPaymentDate"),
    F.col("LastPaymentAmount"),
    F.col("EffectiveStartDate"),
    F.col("EffectiveEndDate"),
    F.col("IsCurrent"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 7.4 Apply SCD Type 2 merge into dim_loan ---
# First run: write directly using saveAsTable.
# Subsequent runs: merge on LoanSurrogateKey to insert new loan versions
# and update existing records where LoanStatus or other attributes change.
if not spark.catalog.tableExists(GOLD_DIM_LOAN):
    df_dim_loan.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_DIM_LOAN)
    print("dim_loan created on first run.")
else:
    delta_table = DeltaTable.forName(spark, GOLD_DIM_LOAN)
    delta_table.alias("target").merge(
        df_dim_loan.alias("source"),
        "target.LoanSurrogateKey = source.LoanSurrogateKey"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_loan updated via SCD Type 2 merge.")

# --- 7.5 Print confirmation ---
row_count = spark.table(GOLD_DIM_LOAN).count()
print(f"dim_loan row count: {row_count}")
print("Ready to proceed to Section 8: Populate dim_collateral_asset.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 8: Populate `dim_collateral_asset` with SCD Type 2
# Builds the collateral asset dimension from `silver_debtor_loan_collateral`.
# Deduplicates on CollateralID. Collateral is the finest grain in the Silver
# table so every row represents a unique collateral asset. 300 distinct
# CollateralIDs confirmed via SQL query before this section was built.
# SCD Type 2 tracks changes to collateral attributes over time.
# 
# ### Steps
# - **Step 8.1** - Filter to IsCurrent = True and deduplicate on CollateralID
# - **Step 8.2** - Generate CollateralSurrogateKey as SHA-256 hash
# - **Step 8.3** - Select and order final columns
# - **Step 8.4** - Apply SCD Type 2 merge into dim_collateral_asset
# - **Step 8.5** - Print confirmation

# CELL ********************

# =============================================================================
# SECTION 8: POPULATE dim_collateral_asset WITH SCD TYPE 2
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 8.1 Filter to IsCurrent = True and deduplicate on CollateralID ---
# 300 distinct CollateralIDs confirmed via SQL query before this section was built.
# Collateral is the finest grain in silver_debtor_loan_collateral so deduplication
# will not collapse any rows here but the pattern is applied for consistency.

window_collateral = Window.partitionBy("CollateralID").orderBy(F.col("EffectiveStartDate").desc())

df_collateral_deduped = df_debtor_loan_collateral.filter(
    F.col("IsCurrent") == True
).withColumn(
    "row_num", F.row_number().over(window_collateral)
).filter(
    F.col("row_num") == 1
).drop("row_num")

# --- 8.2 Generate CollateralSurrogateKey ---
df_collateral_deduped = df_collateral_deduped.withColumn(
    "CollateralSurrogateKey",
    F.sha2(
        F.concat_ws("|", F.col("CollateralID"), F.col("EffectiveStartDate")),
        256
    )
)

# --- 8.3 Select and order final columns ---
df_dim_collateral_asset = df_collateral_deduped.select(
    F.col("CollateralSurrogateKey"),
    F.col("CollateralID"),
    F.col("LoanID"),
    F.col("DebtorID"),
    F.col("TickerSymbol"),
    F.col("AssetType"),
    F.col("QuantityHeld"),
    F.col("CollateralValueAtPledge"),
    F.col("CollateralPledgeDate"),
    F.col("Exchange"),
    F.col("EffectiveStartDate"),
    F.col("EffectiveEndDate"),
    F.col("IsCurrent"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 8.4 Apply SCD Type 2 merge into dim_collateral_asset ---
if not spark.catalog.tableExists(GOLD_DIM_COLLATERAL_ASSET):
    df_dim_collateral_asset.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_DIM_COLLATERAL_ASSET)
    print("dim_collateral_asset created on first run.")
else:
    delta_table = DeltaTable.forName(spark, GOLD_DIM_COLLATERAL_ASSET)
    delta_table.alias("target").merge(
        df_dim_collateral_asset.alias("source"),
        "target.CollateralSurrogateKey = source.CollateralSurrogateKey"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_collateral_asset updated via SCD Type 2 merge.")

# --- 8.5 Print confirmation ---
row_count = spark.table(GOLD_DIM_COLLATERAL_ASSET).count()
print(f"dim_collateral_asset row count: {row_count}")
print("Ready to proceed to Section 9: Balance resolution and market price join.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 9: Balance Resolution and Market Price Join
# Implements the two core data resolution rules from the Business Rules Document
# before LTV can be computed.
# 
# Step 1 resolves the authoritative outstanding balance per loan using the
# Balance Authority Rule. S3 bank balance update is the primary source.
# On-premises SQL Server balance is the fallback.
# Source: CSL_DE_001_Business_Rules_v1.0.md, Section 3.
# 
# Step 2 resolves the current market price per ticker using the Stale Price
# Logic. Today's closing price is primary. Lookback up to 5 business days
# where today's price is missing. MISSING flag where no price found within
# 5 business days.
# Source: CSL_DE_001_Business_Rules_v1.0.md, Section 6.
# 
# ### Steps
# - **Step 9.1** - Filter silver_debtor_loan_collateral to eligible loans
# - **Step 9.2** - Join to silver_bank_balance_update on LoanID
# - **Step 9.3** - Resolve authoritative balance using COALESCE, add BalanceSource
# - **Step 9.4** - Join to silver_market_prices for today's closing price
# - **Step 9.5** - Apply stale price lookback for missing today prices
# - **Step 9.6** - Set DataFreshnessStatus and EligibleForLTV
# - **Step 9.7** - Compute CurrentCollateralValue
# - **Step 9.8** - Print summary counts


# CELL ********************

# =============================================================================
# SECTION 9: BALANCE RESOLUTION AND MARKET PRICE JOIN
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 9.1 Filter silver_debtor_loan_collateral to eligible loans ---
# Per BRD Section 10: only Active and Defaulted loans are included in LTV.
# Settled loans are excluded as the debt obligation has been fulfilled.
# Also filter to IsCurrent = True to exclude expired SCD Type 2 versions.
df_eligible = df_debtor_loan_collateral.filter(
    (F.col("IsCurrent") == True) &
    (F.col("LoanStatus").isin(LTV_ELIGIBLE_LOAN_STATUSES))
)

eligible_count = df_eligible.count()
print(f"Eligible records after loan status filter: {eligible_count}")

# --- 9.2 Join to silver_bank_balance_update on LoanID ---
# LEFT JOIN preserves all eligible collateral records even where no S3
# bank balance update exists for that LoanID. The fallback to on-premises
# balance is handled in Step 9.3 via COALESCE.
# Only the most recent S3 balance per LoanID is used, identified by
# taking the maximum ReportingDate per LoanID before joining.
df_latest_balance = df_bank_balance_update.groupBy("LoanID").agg(
    F.max("ReportingDate").alias("LatestReportingDate")
).join(
    df_bank_balance_update,
    on=["LoanID"],
    how="inner"
).filter(
    F.col("ReportingDate") == F.col("LatestReportingDate")
).select(
    F.col("LoanID"),
    F.col("CurrentOutstandingBalance").alias("S3_OutstandingBalance"),
    F.col("ReportingDate").alias("S3_ReportingDate")
)

df_with_balance = df_eligible.join(
    df_latest_balance,
    on="LoanID",
    how="left"
)

# --- 9.3 Resolve authoritative balance using COALESCE ---
# Per BRD Section 3: S3 bank balance is the primary source.
# On-premises SQL Server balance is the fallback where S3 has no record.
# BalanceSource column records which source was used for audit purposes.
df_with_balance = df_with_balance.withColumn(
    "ResolvedOutstandingBalance",
    F.coalesce(
        F.col("S3_OutstandingBalance"),
        F.col("OutstandingBalance")
    )
).withColumn(
    "BalanceSource",
    F.when(F.col("S3_OutstandingBalance").isNotNull(), "S3_UPDATE")
     .otherwise("ONPREM_ORIGINAL")
)

balance_source_counts = df_with_balance.groupBy("BalanceSource").count()
print("\nBalance source distribution:")
balance_source_counts.show()

# --- 9.4 Join to silver_market_prices for today's closing price ---
# Join on TickerSymbol where PriceDate equals today's pipeline run date.
# LEFT JOIN preserves all collateral records even where no price exists
# for today. Missing prices are handled in Step 9.5.
df_today_prices = df_market_prices.filter(
    F.col("PriceDate") == F.lit(PIPELINE_RUN_DATE)
).select(
    F.col("TickerSymbol"),
    F.col("Close").alias("TodayClosePrice"),
    F.col("PriceDate").alias("TodayPriceDate")
)

df_with_prices = df_with_balance.join(
    df_today_prices,
    on="TickerSymbol",
    how="left"
)

# --- 9.5 Apply stale price lookback for missing today prices ---
# Per BRD Section 6.2: where no price exists for today, look back up to
# 5 business days for the most recent available ClosePrice.
# Records using a fallback price are flagged as STALE.
# This is implemented by finding the most recent price within the lookback
# window for tickers where today's price is missing.

# Identify tickers with no price today
tickers_missing_today = df_with_prices.filter(
    F.col("TodayClosePrice").isNull()
).select("TickerSymbol").distinct()

# Find most recent available price within lookback window for those tickers
lookback_start = F.date_sub(F.lit(PIPELINE_RUN_DATE), STALE_PRICE_LOOKBACK_DAYS * 2)

df_stale_prices = df_market_prices.join(
    tickers_missing_today,
    on="TickerSymbol",
    how="inner"
).filter(
    (F.col("PriceDate") >= lookback_start) &
    (F.col("PriceDate") < F.lit(PIPELINE_RUN_DATE))
).groupBy("TickerSymbol").agg(
    F.max("PriceDate").alias("StalePriceDate")
).join(
    df_market_prices.select(
        "TickerSymbol",
        F.col("PriceDate").alias("StalePriceDate"),
        F.col("Close").alias("StaleClosePrice")
    ),
    on=["TickerSymbol", "StalePriceDate"],
    how="inner"
)

# Bring stale prices back into the main DataFrame
df_with_prices = df_with_prices.join(
    df_stale_prices,
    on="TickerSymbol",
    how="left"
)

# --- 9.6 Set DataFreshnessStatus and EligibleForLTV ---
# CURRENT: today's price was available
# STALE: fallback price used from within 5 business day lookback window
# MISSING: no price found within lookback window, excluded from LTV
# Per BRD Section 6.3: MISSING records remain in fact table for audit
# but EligibleForLTV is set to FALSE.
df_with_prices = df_with_prices.withColumn(
    "CurrentMarketPrice",
    F.coalesce(F.col("TodayClosePrice"), F.col("StaleClosePrice"))
).withColumn(
    "DataFreshnessStatus",
    F.when(F.col("TodayClosePrice").isNotNull(), "CURRENT")
     .when(F.col("StaleClosePrice").isNotNull(), "STALE")
     .otherwise("MISSING")
).withColumn(
    "EligibleForLTV",
    F.when(F.col("DataFreshnessStatus") == "MISSING", False)
     .otherwise(True)
)

# --- 9.7 Compute CurrentCollateralValue ---
# Per BRD Section 2.1: CurrentCollateralValue = QuantityHeld x CurrentMarketPrice
# Only computed where EligibleForLTV is TRUE.
# NULL where price is missing to prevent misleading zero values.
df_resolved = df_with_prices.withColumn(
    "CurrentCollateralValue",
    F.when(
        F.col("EligibleForLTV") == True,
        F.col("QuantityHeld") * F.col("CurrentMarketPrice")
    ).otherwise(None)
)

# --- 9.8 Print summary counts ---
print(f"\nTotal records after resolution: {df_resolved.count()}")
print("\nDataFreshnessStatus distribution:")
df_resolved.groupBy("DataFreshnessStatus").count().show()
print("\nEligibleForLTV distribution:")
df_resolved.groupBy("EligibleForLTV").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 10: LTV Calculation, Risk Flagging, and Shortfall Computation
# Implements all core business logic from CSL_DE_001_Business_Rules_v1.0.md.
# Takes the resolved DataFrame from Section 9 and computes debtor-level
# LTV ratios, assigns risk flags, and calculates collateral coverage shortfall.
# 
# Aggregation happens at debtor level per BRD Section 8. All eligible loans
# and collateral positions for a debtor are summed before LTV is computed.
# LTV at loan or collateral level in isolation would produce misleading
# risk signals for debtors with complex multi-position portfolios.
# 
# ### Steps
# - **Step 10.1** - Aggregate to debtor level: TotalOutstandingBalance and TotalCollateralValue
# - **Step 10.2** - Compute LTV Ratio and LTV Percentage
# - **Step 10.3** - Handle division by zero
# - **Step 10.4** - Assign risk flags per BRD Section 4 thresholds
# - **Step 10.5** - Set ImmediateActionRequired flag
# - **Step 10.6** - Compute CollateralCoverageShortfall per BRD Section 5
# - **Step 10.7** - Rejoin collateral asset level detail for fact table grain
# - **Step 10.8** - Add DateKey for dim_date join
# - **Step 10.9** - Print summary counts and risk distribution


# CELL ********************



# =============================================================================
# SECTION 10: LTV CALCULATION, RISK FLAGGING, AND SHORTFALL COMPUTATION
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 10.1 Aggregate to debtor level ---
# Per BRD Section 8: LTV is calculated at debtor level not loan or collateral level.
# TotalOutstandingBalance = SUM of ResolvedOutstandingBalance across all active
# and defaulted loans for a given DebtorID.
# TotalCollateralValue = SUM of CurrentCollateralValue across all eligible
# collateral positions for a given DebtorID.
# Ineligible collateral positions (EligibleForLTV = FALSE) are excluded from
# TotalCollateralValue but remain in the dataset for audit purposes.

df_debtor_aggregated = df_resolved.groupBy("DebtorID").agg(
    F.sum("ResolvedOutstandingBalance").alias("TotalOutstandingBalance"),
    F.sum(
        F.when(F.col("EligibleForLTV") == True, F.col("CurrentCollateralValue"))
         .otherwise(F.lit(0))
    ).alias("TotalCollateralValue"),
    F.count("CollateralID").alias("TotalCollateralPositions"),
    F.sum(
        F.when(F.col("EligibleForLTV") == False, F.lit(1))
         .otherwise(F.lit(0))
    ).alias("IneligiblePositions")
)

# --- 10.2 Compute LTV Ratio and LTV Percentage ---
# Per BRD Section 2.3: LTV Ratio = TotalOutstandingBalance / TotalCollateralValue
# Per BRD Section 2.4: LTV Percentage = LTV Ratio x 100
# Division by zero handled explicitly in Step 10.3.
df_debtor_aggregated = df_debtor_aggregated.withColumn(
    "LTV_Ratio",
    F.when(
        (F.col("TotalCollateralValue").isNull()) |
        (F.col("TotalCollateralValue") == 0),
        None
    ).otherwise(
        F.col("TotalOutstandingBalance") / F.col("TotalCollateralValue")
    )
).withColumn(
    "LTV_Percentage",
    F.when(F.col("LTV_Ratio").isNotNull(), F.col("LTV_Ratio") * 100)
     .otherwise(None)
)

# --- 10.3 Handle division by zero ---
# Where TotalCollateralValue is zero or NULL, LTV_Ratio is NULL.
# These records are flagged separately so they are not confused with
# legitimate LTV calculations. A zero collateral value means either
# all positions are ineligible or no collateral was pledged.
df_debtor_aggregated = df_debtor_aggregated.withColumn(
    "LTV_Calculation_Status",
    F.when(
        (F.col("TotalCollateralValue").isNull()) |
        (F.col("TotalCollateralValue") == 0),
        "NO_ELIGIBLE_COLLATERAL"
    ).otherwise("CALCULATED")
)

# --- 10.4 Assign risk flags per BRD Section 4 thresholds ---
# CRITICAL : LTV Ratio >= 1.00
# HIGH     : LTV Ratio >= 0.80 and < 1.00
# MEDIUM   : LTV Ratio >= 0.60 and < 0.80
# LOW      : LTV Ratio < 0.60
# NULL     : LTV could not be calculated
df_debtor_aggregated = df_debtor_aggregated.withColumn(
    "RiskFlag",
    F.when(F.col("LTV_Ratio").isNull(), "UNDETERMINED")
     .when(F.col("LTV_Ratio") >= LTV_HIGH_UPPER, "CRITICAL")
     .when(F.col("LTV_Ratio") >= LTV_MEDIUM_UPPER, "HIGH")
     .when(F.col("LTV_Ratio") >= LTV_LOW_UPPER, "MEDIUM")
     .otherwise("LOW")
)

# --- 10.5 Set ImmediateActionRequired flag ---
# Per BRD Section 4.1: TRUE where RiskFlag is HIGH or CRITICAL.
# These accounts appear in the High Risk Accounts view in Power BI.
df_debtor_aggregated = df_debtor_aggregated.withColumn(
    "ImmediateActionRequired",
    F.when(F.col("RiskFlag").isin("HIGH", "CRITICAL"), True)
     .otherwise(False)
)

# --- 10.6 Compute CollateralCoverageShortfall ---
# Per BRD Section 5.1:
# RequiredCollateralValue = TotalOutstandingBalance / 0.80
# CollateralCoverageShortfall = RequiredCollateralValue - TotalCollateralValue
# Set to NULL where result is zero or negative per BRD Section 5.2.
# A positive result is the NGN amount by which collateral must increase
# to bring the account out of HIGH risk status.
df_debtor_aggregated = df_debtor_aggregated.withColumn(
    "RequiredCollateralValue",
    F.when(
        F.col("LTV_Ratio").isNotNull(),
        F.col("TotalOutstandingBalance") / F.lit(0.80)
    ).otherwise(None)
).withColumn(
    "CollateralCoverageShortfall",
    F.when(
        F.col("RequiredCollateralValue").isNotNull() &
        (F.col("RequiredCollateralValue") > F.col("TotalCollateralValue")),
        F.col("RequiredCollateralValue") - F.col("TotalCollateralValue")
    ).otherwise(None)
)

# --- 10.7 Rejoin collateral asset level detail for fact table grain ---
# The fact table grain is one row per debtor per collateral asset per date.
# We aggregated to debtor level for LTV computation then rejoin the
# collateral asset level detail to restore the required fact table grain.
# This preserves asset level detail while carrying debtor level LTV metrics.
df_fact_prep = df_resolved.join(
    df_debtor_aggregated.select(
        "DebtorID",
        "TotalOutstandingBalance",
        "TotalCollateralValue",
        "TotalCollateralPositions",
        "IneligiblePositions",
        "LTV_Ratio",
        "LTV_Percentage",
        "LTV_Calculation_Status",
        "RiskFlag",
        "ImmediateActionRequired",
        "RequiredCollateralValue",
        "CollateralCoverageShortfall"
    ),
    on="DebtorID",
    how="left"
)

# --- 10.8 Add DateKey for dim_date join ---
# DateKey is formatted as integer yyyyMMdd to match dim_date.DateKey.
# Uses PIPELINE_RUN_DATE as the snapshot date for this daily run.
df_fact_prep = df_fact_prep.withColumn(
    "DateKey",
    F.date_format(F.lit(PIPELINE_RUN_DATE), "yyyyMMdd").cast(IntegerType())
)

# --- 10.9 Print summary counts and risk distribution ---
total = df_fact_prep.count()
print(f"Total fact records prepared: {total}")
print("\nRisk flag distribution:")
df_fact_prep.groupBy("RiskFlag").count().orderBy("RiskFlag").show()
print("\nImmediateActionRequired distribution:")
df_fact_prep.groupBy("ImmediateActionRequired").count().show()
print("\nLTV_Calculation_Status distribution:")
df_fact_prep.groupBy("LTV_Calculation_Status").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 11: Referential Integrity Validation
# Validates that every foreign key value in the prepared fact records
# has a corresponding record in its dimension table before writing to
# fact_ltv_daily_snapshot.
# 
# This validation is the substitute for database-level foreign key
# enforcement which is not available in a Lakehouse architecture.
# Records that fail validation are written to fact_ltv_quarantine_log
# rather than the fact table. The count of quarantined records is
# printed for investigation.
# 
# Without this check, orphaned fact records with no matching dimension
# record would silently break Power BI relationships and produce
# incorrect aggregations in the Risk Command Centre dashboard.
# 
# ### Steps
# - **Step 11.1** - Load current dimension keys for validation
# - **Step 11.2** - Validate DebtorID exists in dim_debtor
# - **Step 11.3** - Validate LoanID exists in dim_loan
# - **Step 11.4** - Validate CollateralID exists in dim_collateral_asset
# - **Step 11.5** - Validate AssignedOfficerID exists in dim_collections_officer
# - **Step 11.6** - Validate ClientID exists in dim_client_bank
# - **Step 11.7** - Validate DateKey exists in dim_date
# - **Step 11.8** - Split into clean and quarantine sets
# - **Step 11.9** - Print validation summary


# CELL ********************

# =============================================================================
# SECTION 11: REFERENTIAL INTEGRITY VALIDATION
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 11.1 Load current dimension keys for validation ---
# We extract only the key columns from each dimension table.
# These are used as lookup sets to validate foreign keys in the fact records.
# Using distinct() ensures duplicate keys do not cause false join inflation.

debtor_keys = spark.table(GOLD_DIM_DEBTOR) \
    .select("DebtorID").distinct()

loan_keys = spark.table(GOLD_DIM_LOAN) \
    .select("LoanID").distinct()

collateral_keys = spark.table(GOLD_DIM_COLLATERAL_ASSET) \
    .select("CollateralID").distinct()

officer_keys = spark.table(GOLD_DIM_COLLECTIONS_OFFICER) \
    .select(F.col("OfficerID").alias("AssignedOfficerID")).distinct()

client_keys = spark.table(GOLD_DIM_CLIENT_BANK) \
    .select("ClientID").distinct()

date_keys = spark.table(GOLD_DIM_DATE) \
    .select("DateKey").distinct()

# --- 11.2 Validate DebtorID exists in dim_debtor ---
df_fact_prep = df_fact_prep.withColumn(
    "debtor_fk_valid",
    F.col("DebtorID").isNotNull()
)

df_debtor_valid = df_fact_prep.join(
    debtor_keys,
    on="DebtorID",
    how="left_anti"
).select("DebtorID").distinct()

debtor_orphans = df_debtor_valid.count()
print(f"DebtorID orphans (not in dim_debtor)     : {debtor_orphans}")

# --- 11.3 Validate LoanID exists in dim_loan ---
df_loan_valid = df_fact_prep.join(
    loan_keys,
    on="LoanID",
    how="left_anti"
).select("LoanID").distinct()

loan_orphans = df_loan_valid.count()
print(f"LoanID orphans (not in dim_loan)         : {loan_orphans}")

# --- 11.4 Validate CollateralID exists in dim_collateral_asset ---
df_collateral_valid = df_fact_prep.join(
    collateral_keys,
    on="CollateralID",
    how="left_anti"
).select("CollateralID").distinct()

collateral_orphans = df_collateral_valid.count()
print(f"CollateralID orphans (not in dim_collateral_asset): {collateral_orphans}")

# --- 11.5 Validate AssignedOfficerID exists in dim_collections_officer ---
df_officer_valid = df_fact_prep.join(
    officer_keys,
    on="AssignedOfficerID",
    how="left_anti"
).select("AssignedOfficerID").distinct()

officer_orphans = df_officer_valid.count()
print(f"OfficerID orphans (not in dim_collections_officer): {officer_orphans}")

# --- 11.6 Validate ClientID exists in dim_client_bank ---
df_client_valid = df_fact_prep.join(
    client_keys,
    on="ClientID",
    how="left_anti"
).select("ClientID").distinct()

client_orphans = df_client_valid.count()
print(f"ClientID orphans (not in dim_client_bank): {client_orphans}")

# --- 11.7 Validate DateKey exists in dim_date ---
df_date_valid = df_fact_prep.join(
    date_keys,
    on="DateKey",
    how="left_anti"
).select("DateKey").distinct()

date_orphans = df_date_valid.count()
print(f"DateKey orphans (not in dim_date)        : {date_orphans}")

# --- 11.8 Split into clean and quarantine sets ---
# A record is quarantined if ANY of its foreign keys fail validation.
# left_anti join returns rows in the left DataFrame that have no match
# in the right DataFrame. We use this to identify orphaned records.
# Clean records pass all 6 foreign key checks.

df_fact_clean = df_fact_prep \
    .join(debtor_keys, on="DebtorID", how="inner") \
    .join(loan_keys, on="LoanID", how="inner") \
    .join(collateral_keys, on="CollateralID", how="inner") \
    .join(officer_keys, on="AssignedOfficerID", how="inner") \
    .join(client_keys, on="ClientID", how="inner") \
    .join(date_keys, on="DateKey", how="inner")

df_fact_quarantine = df_fact_prep \
    .join(debtor_keys, on="DebtorID", how="left_anti") \
    .unionByName(
        df_fact_prep.join(loan_keys, on="LoanID", how="left_anti"),
        allowMissingColumns=True
    ).unionByName(
        df_fact_prep.join(collateral_keys, on="CollateralID", how="left_anti"),
        allowMissingColumns=True
    ).unionByName(
        df_fact_prep.join(officer_keys, on="AssignedOfficerID", how="left_anti"),
        allowMissingColumns=True
    ).unionByName(
        df_fact_prep.join(client_keys, on="ClientID", how="left_anti"),
        allowMissingColumns=True
    ).unionByName(
        df_fact_prep.join(date_keys, on="DateKey", how="left_anti"),
        allowMissingColumns=True
    ).distinct()

clean_count = df_fact_clean.count()
quarantine_count = df_fact_quarantine.count()

# --- 11.9 Print validation summary ---
print(f"\n=== Referential Integrity Validation Summary ===")
print(f"Total records entering validation : {df_fact_prep.count()}")
print(f"Clean records passed validation   : {clean_count}")
print(f"Quarantined records               : {quarantine_count}")

if quarantine_count > 0:
    print("\nWARNING: Quarantined records detected. Investigate before next run.")
    print("Quarantined records will be written to fact_ltv_quarantine_log.")
else:
    print("\nAll records passed referential integrity validation.")
    print("Ready to proceed to Section 12: Write to fact_ltv_daily_snapshot.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 12: Write to `fact_ltv_daily_snapshot`
# Appends the validated daily LTV snapshot to fact_ltv_daily_snapshot.
# Never overwrites. Each daily run adds new rows stamped with the current
# PipelineRunDate preserving full LTV history for trend analysis in Power BI.
# Only clean records that passed referential integrity validation in Section 11
# are written here. Quarantined records are written to fact_ltv_quarantine_log.
# 
# ### Steps
# - **Step 12.1** - Select and order final fact columns
# - **Step 12.2** - Write quarantined records to fact_ltv_quarantine_log
# - **Step 12.3** - Append clean records to fact_ltv_daily_snapshot
# - **Step 12.4** - Print confirmation

# CELL ********************

# =============================================================================
# SECTION 12: WRITE TO fact_ltv_daily_snapshot
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 12.1 Select and order final fact columns ---
df_fact_final = df_fact_clean.select(

    # Keys
    F.col("DateKey"),
    F.col("DebtorID"),
    F.col("LoanID"),
    F.col("CollateralID"),
    F.col("ClientID"),
    F.col("AssignedOfficerID"),

    # Balance resolution
    F.col("ResolvedOutstandingBalance"),
    F.col("BalanceSource"),

    # Collateral valuation
    F.col("TickerSymbol"),
    F.col("QuantityHeld"),
    F.col("CurrentMarketPrice"),
    F.col("CurrentCollateralValue"),
    F.col("DataFreshnessStatus"),
    F.col("EligibleForLTV"),

    # Debtor level LTV metrics
    F.col("TotalOutstandingBalance"),
    F.col("TotalCollateralValue"),
    F.col("TotalCollateralPositions"),
    F.col("IneligiblePositions"),
    F.col("LTV_Ratio"),
    F.col("LTV_Percentage"),
    F.col("LTV_Calculation_Status"),

    # Risk flags
    F.col("RiskFlag"),
    F.col("ImmediateActionRequired"),

    # Shortfall
    F.col("RequiredCollateralValue"),
    F.col("CollateralCoverageShortfall"),

    # Snapshot date
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("PipelineRunDate"),

    # Audit columns
    F.lit(RUN_ID).alias("gold_run_id")
)

# --- 12.2 Write quarantined records to fact_ltv_quarantine_log ---
if quarantine_count > 0:
    df_fact_quarantine.withColumn(
        "quarantine_run_id", F.lit(RUN_ID)
    ).withColumn(
        "quarantine_date", F.lit(PIPELINE_RUN_DATE).cast(DateType())
    ).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(GOLD_FACT_QUARANTINE_LOG)
    print(f"Quarantined records written to fact_ltv_quarantine_log: {quarantine_count}")
else:
    print("No quarantined records. fact_ltv_quarantine_log not written to.")

# --- 12.3 Idempotency guard: delete existing rows for today before appending ---
# Prevents duplicate snapshot rows if the notebook is rerun on the same day.
# Delete-then-insert pattern makes the append idempotent.
# Without this guard, rerunning on the same day doubles every metric in Power BI.
if spark.catalog.tableExists(GOLD_FACT_LTV_SNAPSHOT):
    deleted = spark.sql(f"""
        DELETE FROM fact_ltv_daily_snapshot
        WHERE PipelineRunDate = '{PIPELINE_RUN_DATE}'
    """)
    print(f"Idempotency guard: deleted existing rows for {PIPELINE_RUN_DATE}")

# --- 12.4 Append clean records to fact_ltv_daily_snapshot ---
df_fact_final.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(GOLD_FACT_LTV_SNAPSHOT)

# --- 12.5 Print confirmation ---
total_rows = spark.table(GOLD_FACT_LTV_SNAPSHOT).count()
print(f"\nfact_ltv_daily_snapshot append complete.")
print(f"Records written this run : {clean_count}")
print(f"Total rows in table      : {total_rows}")
print(f"Snapshot date            : {PIPELINE_RUN_DATE}")
print(f"Run ID                   : {RUN_ID}")
print("\nReady to proceed to Section 13: Write to gold_audit_log.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 13: Write to `gold_audit_log`
# Writes one audit row per pipeline step to gold_audit_log.
# Append only. Full execution history preserved across all pipeline runs.
# Uses the audit_log accumulator populated throughout the notebook.
# gold_pipeline_metadata is not written to from this notebook.
# That table is owned by nb_setup_pipeline_metadata and the Stream A pipeline.
# 
# ### Steps
# - **Step 13.1** - Build audit log rows from accumulator
# - **Step 13.2** - Add summary row for the full notebook run
# - **Step 13.3** - Write to gold_audit_log
# - **Step 13.4** - Print confirmation

# CELL ********************

# =============================================================================
# SECTION 13: WRITE TO gold_audit_log
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 13.1 Build audit log rows ---
from pyspark.sql.types import StructType, StructField, StringType, LongType

audit_rows = [
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_date",
        "SourceSystem"     : "generated",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 0,
        "RowsWritten"      : spark.table(GOLD_DIM_DATE).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_collections_officer",
        "SourceSystem"     : "silver_collections_officer|silver_officer_client_mapping",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 53,
        "RowsWritten"      : spark.table(GOLD_DIM_COLLECTIONS_OFFICER).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_client_bank",
        "SourceSystem"     : "silver_client_bank",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 4,
        "RowsWritten"      : spark.table(GOLD_DIM_CLIENT_BANK).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_debtor",
        "SourceSystem"     : "silver_debtor_loan_collateral",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 300,
        "RowsWritten"      : spark.table(GOLD_DIM_DEBTOR).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_loan",
        "SourceSystem"     : "silver_debtor_loan_collateral",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 300,
        "RowsWritten"      : spark.table(GOLD_DIM_LOAN).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "dim_collateral_asset",
        "SourceSystem"     : "silver_debtor_loan_collateral",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 300,
        "RowsWritten"      : spark.table(GOLD_DIM_COLLATERAL_ASSET).count(),
        "RowsRejected"     : 0,
        "ErrorMessage"     : None
    },
    {
        "RunID"            : RUN_ID,
        "PipelineStepName" : "fact_ltv_daily_snapshot",
        "SourceSystem"     : "silver_debtor_loan_collateral|silver_bank_balance_update|silver_market_prices",
        "RunStatus"        : "SUCCESS",
        "RowsRead"         : 249,
        "RowsWritten"      : clean_count,
        "RowsRejected"     : quarantine_count,
        "ErrorMessage"     : None
    },

    {
    "RunID"            : RUN_ID,
    "PipelineStepName" : "fact_market_prices_daily",
    "SourceSystem"     : "silver_market_prices",
    "RunStatus"        : "SUCCESS",
    "RowsRead"         : 1888,
    "RowsWritten"      : spark.table(GOLD_FACT_MARKET_PRICES).count(),
    "RowsRejected"     : 0,
    "ErrorMessage"     : None
},

]

audit_rows.append({
    "RunID"            : RUN_ID,
    "PipelineStepName" : "nb_gold_aggregation_COMPLETE",
    "SourceSystem"     : "all_silver_tables",
    "RunStatus"        : "SUCCESS",
    "RowsRead"         : 300,
    "RowsWritten"      : clean_count,
    "RowsRejected"     : quarantine_count,
    "ErrorMessage"     : None
})

# --- 13.2 Add summary row for the full notebook run ---
audit_schema = StructType([
    StructField("RunID",            StringType(), True),
    StructField("PipelineStepName", StringType(), True),
    StructField("SourceSystem",     StringType(), True),
    StructField("RunStatus",        StringType(), True),
    StructField("RowsRead",         LongType(),   True),
    StructField("RowsWritten",      LongType(),   True),
    StructField("RowsRejected",     LongType(),   True),
    StructField("ErrorMessage",     StringType(), True),
])

df_audit = spark.createDataFrame(audit_rows, schema=audit_schema).withColumn(
    "RunTimestamp",
    F.lit(RUN_TIMESTAMP).cast(TimestampType())
).withColumn(
    "PipelineRunDate",
    F.lit(PIPELINE_RUN_DATE).cast(DateType())
)

# --- 13.3 Idempotency guard: delete existing audit rows for this RunID ---
# Prevents duplicate audit rows if the notebook is rerun in the same session.
# Deletes on RunID not PipelineRunDate because a rerun generates a new RunID.
# Deleting on RunID ensures only rows from this exact execution are replaced.
if spark.catalog.tableExists(GOLD_AUDIT_LOG):
    spark.sql(f"""
        DELETE FROM gold_audit_log
        WHERE RunID = '{RUN_ID}'
    """)
    print(f"Idempotency guard: cleared existing audit rows for RunID {RUN_ID}")

# --- 13.4 Write to gold_audit_log ---
df_audit.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(GOLD_AUDIT_LOG)

# --- 13.5 Print confirmation ---
total_audit_rows = spark.table(GOLD_AUDIT_LOG).count()
print("=== Gold Notebook Run Complete ===\n")
print(f"Run ID                        : {RUN_ID}")
print(f"Pipeline run date             : {PIPELINE_RUN_DATE}")
print(f"Fact records written          : {clean_count}")
print(f"Quarantined records           : {quarantine_count}")
print(f"Audit rows written this run   : {len(audit_rows)}")
print(f"Total rows in gold_audit_log  : {total_audit_rows}")
print(f"\nnb_gold_aggregation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT RunID, PipelineRunDate, COUNT(*) as row_count
# MAGIC FROM gold_audit_log
# MAGIC GROUP BY RunID, PipelineRunDate
# MAGIC ORDER BY PipelineRunDate

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Section 14: Populate `fact_market_prices_daily`
# Promotes silver_market_prices into Gold as a dedicated price history fact table.
# Enables the collateral value trend chart in the Power BI Risk Command Centre
# as specified in the project brief Section 3.3.
# 
# Without this table Power BI can only see prices for dates where LTV was
# calculated. The full 365 day price history in silver_market_prices would
# be invisible to the dashboard.
# 
# Grain: one row per ticker per price date.
# 
# First run: full overwrite loading all 1,888 rows of Silver history.
# Subsequent runs: append only new PriceDates using watermark logic.
# Idempotency guard: deletes any existing rows for the current watermark
# range before appending to prevent duplicates on reruns.
# 
# ### Steps
# - **Step 14.1** - Check if fact_market_prices_daily exists
# - **Step 14.2** - First run: full overwrite of all Silver price history
# - **Step 14.3** - Subsequent runs: resolve watermark and filter new records
# - **Step 14.4** - Idempotency guard: delete rows in watermark range
# - **Step 14.5** - Append new records
# - **Step 14.6** - Print confirmation


# CELL ********************

# =============================================================================
# SECTION 14: POPULATE fact_market_prices_daily
# nb_gold_aggregation | CSL-DE-001 | Gold Layer Aggregation Notebook
# =============================================================================

# --- 14.1 Check if fact_market_prices_daily exists ---
# First run: full overwrite loading complete Silver price history.
# Subsequent runs: watermark-based append of new PriceDates only.
# This pattern is consistent with the Bronze incremental load strategy.

GOLD_FACT_MARKET_PRICES = "fact_market_prices_daily"
GOLD_FACT_MARKET_PRICES_READ = "Tables/dbo/fact_market_prices_daily"

fact_prices_exists = spark.catalog.tableExists(GOLD_FACT_MARKET_PRICES)
print(f"fact_market_prices_daily exists: {fact_prices_exists}")

# --- 14.2 Build the Gold market prices DataFrame from Silver ---
# Select and rename columns for Gold layer consistency.
# DateKey added for joining to dim_date in Power BI.
# Audit columns added for lineage tracking.

df_gold_market_prices = df_market_prices.select(
    # Date key for dim_date join
    F.date_format(F.col("PriceDate"), "yyyyMMdd")
     .cast(IntegerType()).alias("DateKey"),
    F.col("PriceDate"),
    F.col("TickerSymbol"),
    F.col("AssetType"),
    F.col("Open").alias("OpenPrice"),
    F.col("High").alias("HighPrice"),
    F.col("Low").alias("LowPrice"),
    F.col("Close").alias("ClosePrice"),
    F.col("Volume"),
    # Audit columns
    F.lit(PIPELINE_RUN_DATE).cast(DateType()).alias("gold_load_date"),
    F.lit(RUN_ID).alias("gold_run_id")
)

if not fact_prices_exists:

    # --- 14.2 First run: full overwrite ---
    # Loads complete 365 day price history from Silver into Gold.
    # Overwrite ensures a clean start with no partial or corrupt state.
    df_gold_market_prices.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_FACT_MARKET_PRICES)

    row_count = spark.table(GOLD_FACT_MARKET_PRICES).count()
    print(f"First run: full overwrite complete. Rows written: {row_count}")

else:

    # --- 14.3 Subsequent runs: resolve watermark ---
    # Find the maximum PriceDate already in fact_market_prices_daily.
    # Only records in Silver with PriceDate greater than this watermark
    # are new and need to be appended.
    watermark_value = spark.table(GOLD_FACT_MARKET_PRICES) \
        .agg(F.max("PriceDate")).collect()[0][0]

    print(f"Current watermark (max PriceDate in Gold): {watermark_value}")

    df_new_prices = df_gold_market_prices.filter(
        F.col("PriceDate") > F.lit(watermark_value)
    )

    new_record_count = df_new_prices.count()
    print(f"New price records to append: {new_record_count}")

    if new_record_count > 0:

        # --- 14.4 Idempotency guard ---
        # Delete any existing rows for the new PriceDates before appending.
        # Prevents duplicates if the notebook is rerun on the same day
        # after new Silver prices have landed.
        spark.sql(f"""
            DELETE FROM fact_market_prices_daily
            WHERE PriceDate > '{watermark_value}'
        """)
        print(f"Idempotency guard: cleared rows where PriceDate > {watermark_value}")

        # --- 14.5 Append new records ---
        df_new_prices.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_FACT_MARKET_PRICES)

        row_count = spark.table(GOLD_FACT_MARKET_PRICES).count()
        print(f"Append complete. New rows added: {new_record_count}")
        print(f"Total rows in fact_market_prices_daily: {row_count}")

    else:
        print("No new price records found beyond watermark. Nothing to append.")

# --- 14.6 Print confirmation ---
total = spark.table(GOLD_FACT_MARKET_PRICES).count()
print(f"\nfact_market_prices_daily total rows: {total}")
print("Ready to proceed to Section 13: Write to gold_audit_log.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
