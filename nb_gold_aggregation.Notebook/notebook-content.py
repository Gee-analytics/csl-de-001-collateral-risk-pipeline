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
# | `fact_ltv_daily_snapshot` | Fact | Append |
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
GOLD_AUDIT_LOG                = "Tables/gold_audit_log"

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

# ### **Step 1.6** - Silver input table paths

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC SELECT Count(distinct LoanID)
# MAGIC FROM silver_debtor_loan_collateral


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 1.7** - Gold output table paths


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 1.8** - LTV threshold constants


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 1.9** - Stale price lookback window


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 1.10** - Loan status scope


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Step 1.11** - Audit log accumulator

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
