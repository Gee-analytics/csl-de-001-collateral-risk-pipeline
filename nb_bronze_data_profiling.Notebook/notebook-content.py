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

# # Bronze Layer Data Profiling
# ### Notebook: nb_bronze_data_profiling
# #### Project: CSL-DE-001 | Collateral Risk Monitoring & Margin Call Automation
# #### Layer: Bronze
# #### Purpose:
# Systematic data profiling of all 8 Bronze Delta tables.<br>
# Documents completeness, uniqueness, validity, consistency,<br>
# and referential integrity issues across all sources.<br>
# This notebook produces a diagnosis, not clean data.<br>
# Transformations are handled in the Silver layer.<br>
# 
# ## Sources covered:
# - Source A: On-premises SQL Server (6 tables via Gateway)
# - Source B: Amazon S3 client bank drops (1 table via Shortcut)
# - Source C: Financial Market API (1 table via Spark Notebook)
# 
# ## Last Updated: March 2026
# ## Author: (Gabriel Obot) Data Engineering Team

# CELL ********************

# imports
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType,
    DoubleType, DateType, TimestampType
)


# ── Configuration ─────────────────────────────────────────────────────────────
# Single source of truth for the Lakehouse name.
# If this changes, update here only.
LAKEHOUSE_NAME = "CSL_Collateral_Risk_LH"
BRONZE_PATH = "Tables/dbo" 

# Bronze table registry.
BRONZE_TABLES = [
    "bronze_client_bank",
    "bronze_collections_officer",
    "bronze_officer_client_mapping",
    "bronze_debtor",
    "bronze_loan",
    "bronze_collateral",
    "bronze_bank_balance_update",
    "bronze_market_prices",
]

print("Configuration loaded.")
print(f"Lakehouse        : {LAKEHOUSE_NAME}")
print(f"Bronze path      : {BRONZE_PATH}")
print(f"Tables registered: {len(BRONZE_TABLES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric provides the SparkSession automatically as `spark`.
# We confirm it is active and print the app name for the run log.
print(f"Spark session active  : {spark.sparkContext.appName}")
print(f"Spark version         : {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_table(table_name: str) -> None:
    """
    Runs baseline data quality checks on a Bronze Delta table.

    Checks performed:
        1. Row count
        2. Column count and column list
        3. Null count per column (absolute and percentage)
        4. Duplicate row count (full row deduplication check)

    This function prints a summary report.
    It does not modify any data.

    Args:
        table_name: The Delta table name as registered in the Lakehouse.
    """

    print("=" * 70)
    print(f"PROFILING TABLE: {table_name}")
    print("=" * 70)

    # ── Load the table ────────────────────────────────────────────────────
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")

    total_rows = df.count()
    total_cols = len(df.columns)

    print(f"\n[1] SHAPE")
    print(f"    Rows    : {total_rows:,}")
    print(f"    Columns : {total_cols}")

    # ── Column list and data types ────────────────────────────────────────
    print(f"\n[2] SCHEMA")
    for field in df.schema.fields:
        print(f"    {field.name:<35} {str(field.dataType)}")

    # ── Null counts per column ────────────────────────────────────────────
    print(f"\n[3] NULL COUNTS")
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    for col_name, null_count in null_counts.items():
        pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        flag = " <-- ATTENTION" if pct > 0 else ""
        print(f"    {col_name:<35} {null_count:>6} nulls  ({pct:5.1f}%){flag}")

    # ── Duplicate row check ───────────────────────────────────────────────
    print(f"\n[4] DUPLICATE ROWS (full row comparison)")
    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows
    print(f"    Total rows     : {total_rows:,}")
    print(f"    Distinct rows  : {distinct_rows:,}")
    print(f"    Duplicate rows : {duplicate_rows:,}")
    if duplicate_rows > 0:
        print(f"    STATUS: DUPLICATES DETECTED <-- ATTENTION")
    else:
        print(f"    STATUS: No full-row duplicates found")

    print("\n" + "-" * 70 + "\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 1: bronze_client_bank
# **Source**: On-premises SQL Server via On-Premises Data Gateway <br>
# **Expected rows**: 4 (one per client bank: CLT-001 through CLT-004)<br>
# **Primary key**: ClientID<br>
# **Role in pipeline**: Parent reference table. All loan, debtor, and<br>
# balance records must resolve to a valid ClientID in this table.<br>
# Integrity failures here cascade to every downstream table.

# CELL ********************

# ── Section 1: bronze_client_bank ─────────────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[0]

df_client_bank = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Primary key uniqueness (ClientID) ───────────────────────
print("TARGETED CHECK 1: ClientID uniqueness")

total_rows    = df_client_bank.count()
distinct_ids  = df_client_bank.select("ClientID").distinct().count()
duplicate_ids = total_rows - distinct_ids

print(f"    Total rows       : {total_rows}")
print(f"    Distinct ClientID: {distinct_ids}")
print(f"    Duplicate IDs    : {duplicate_ids}")

if duplicate_ids > 0:
    print("    STATUS: DUPLICATE ClientIDs DETECTED <-- ATTENTION")
    df_client_bank.groupBy("ClientID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("ClientID") \
        .show(truncate=False)
else:
    print("    STATUS: ClientID is unique across all rows")

# ── Targeted Check 2: ClientID format validation ──────────────────────────────
print("\nTARGETED CHECK 2: ClientID format (expected pattern: CLT-XXX)")

invalid_format = df_client_bank.filter(
    ~F.col("ClientID").rlike("^CLT-[0-9]{3}$")
)
invalid_count = invalid_format.count()

print(f"    Rows with unexpected ClientID format: {invalid_count}")

if invalid_count > 0:
    print("    STATUS: UNEXPECTED FORMAT DETECTED <-- ATTENTION")
    invalid_format.select("ClientID").show(truncate=False)
else:
    print("    STATUS: All ClientIDs match expected format")

# ── Targeted Check 3: Blank or whitespace-only ClientName ─────────────────────
print("\nTARGETED CHECK 3: Blank or whitespace-only ClientName")

blank_names = df_client_bank.filter(
    F.col("ClientName").isNull() |
    (F.trim(F.col("ClientName")) == "")
)
blank_count = blank_names.count()

print(f"    Rows with null or blank ClientName: {blank_count}")

if blank_count > 0:
    print("    STATUS: BLANK CLIENT NAMES DETECTED <-- ATTENTION")
    blank_names.show(truncate=False)
else:
    print("    STATUS: All ClientName values are populated")

# ── Targeted Check 4: Expected row count (4 client banks) ─────────────────────
print("\nTARGETED CHECK 4: Expected row count (4 client banks)")

print(f"    Actual row count  : {total_rows}")
print(f"    Expected row count: 4")

if total_rows != 4:
    print("    STATUS: ROW COUNT MISMATCH <-- ATTENTION")
else:
    print("    STATUS: Row count matches expectation")

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (full table - small reference table)")
df_client_bank.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2: bronze_collections_officer
# **Source**: On-premises SQL Server via On-Premises Data Gateway <br>
# **Primary key**: OfficerID<br>
# **Role in pipeline**: Staff reference table.<br> 
# Every debt assignment must trace back to a valid, active officer record.
# Referential integrity check against bronze_client_bank (ClientID).

# CELL ********************

# ── Section 2: bronze_collections_officer ─────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[1]

df_officer = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Primary key uniqueness (OfficerID) ──────────────────────
print("TARGETED CHECK 1: OfficerID uniqueness")

total_rows    = df_officer.count()
distinct_ids  = df_officer.select("OfficerID").distinct().count()
duplicate_ids = total_rows - distinct_ids

print(f"    Total rows        : {total_rows}")
print(f"    Distinct OfficerID: {distinct_ids}")
print(f"    Duplicate IDs     : {duplicate_ids}")

if duplicate_ids > 0:
    print("    STATUS: DUPLICATE OfficerIDs DETECTED <-- ATTENTION")
    df_officer.groupBy("OfficerID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("OfficerID") \
        .show(truncate=False)
else:
    print("    STATUS: OfficerID is unique across all rows")

# ── Targeted Check 2: Blank or whitespace-only FullName ───────────────────────
print("\nTARGETED CHECK 2: Blank or whitespace-only FullName")

blank_names = df_officer.filter(
    F.col("FullName").isNull() |
    (F.trim(F.col("FullName")) == "")
).count()

flag = " <-- ATTENTION" if blank_names > 0 else ""
print(f"    Blank/null FullName count: {blank_names}{flag}")

# ── Targeted Check 3: Status value distribution ───────────────────────────────
print("\nTARGETED CHECK 3: Status value distribution")
print("    (Expected values: Active, Inactive)")

df_officer.groupBy("Status") \
    .count() \
    .orderBy("Status") \
    .show(truncate=False)

unexpected_status = df_officer.filter(
    ~F.col("Status").isin(["Active", "Inactive"])
).count()

if unexpected_status > 0:
    print(f"    STATUS: UNEXPECTED STATUS VALUES DETECTED <-- ATTENTION")
else:
    print(f"    STATUS: All Status values are within expected domain")

# ── Targeted Check 4: TeamLeadOfficerID nulls - business rule validation ───────
print("\nTARGETED CHECK 4: TeamLeadOfficerID null analysis")
print("    (Nulls expected for team leads who have no supervisor)")

null_teamlead = df_officer.filter(
    F.col("TeamLeadOfficerID").isNull()
).count()

print(f"    Null TeamLeadOfficerID count: {null_teamlead}")
print(f"    NOTE: These are likely team lead officers. Verify count is")
print(f"    consistent with known org structure. Documenting as expected.")

# ── Targeted Check 5: TeamLeadOfficerID referential integrity ─────────────────
print("\nTARGETED CHECK 5: TeamLeadOfficerID resolves to valid OfficerID")
print("    (Non-null TeamLeadOfficerIDs must exist in the OfficerID column)")

valid_officer_ids = df_officer.select(
    F.col("OfficerID").alias("ValidID")
)

orphaned_teamlead = df_officer.filter(
    F.col("TeamLeadOfficerID").isNotNull()
).join(
    valid_officer_ids,
    df_officer["TeamLeadOfficerID"] == valid_officer_ids["ValidID"],
    how="left_anti"
)
orphaned_count = orphaned_teamlead.count()

print(f"    TeamLeadOfficerIDs with no matching OfficerID: {orphaned_count}")

if orphaned_count > 0:
    print("    STATUS: ORPHANED TEAM LEAD REFERENCES DETECTED <-- ATTENTION")
    orphaned_teamlead.select("OfficerID", "TeamLeadOfficerID").show(truncate=False)
else:
    print("    STATUS: All non-null TeamLeadOfficerIDs resolve to a valid officer")

# ── Targeted Check 6: DateJoined null investigation ───────────────────────────
print("\nTARGETED CHECK 6: DateJoined null investigation")

null_dates = df_officer.filter(F.col("DateJoined").isNull())
null_date_count = null_dates.count()

print(f"    Officers with null DateJoined: {null_date_count}")

if null_date_count > 0:
    print("    STATUS: MISSING JOIN DATES DETECTED <-- ATTENTION")
    null_dates.select("OfficerID", "FullName", "Status", "DateJoined") \
        .show(truncate=False)

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_officer.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3: bronze_officer_client_mapping
# **Source**: On-premises SQL Server via On-Premises Data Gateway<br>
# **Primary key**: Composite (OfficerID + ClientID)<br>
# **Role in pipeline**: Junction table linking officers to client banks.<br>
# Drives RLS logic in Power BI. Both foreign keys must resolve cleanly.
# Referential integrity checks against bronze_collections_officer and
# bronze_client_bank.

# CELL ********************

# ── Section 3: bronze_officer_client_mapping ──────────────────────────────────

TABLE_NAME = BRONZE_TABLES[2]

df_mapping = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Composite primary key uniqueness (OfficerID + ClientID) ─
print("TARGETED CHECK 1: Composite key uniqueness (OfficerID + ClientID)")

total_rows       = df_mapping.count()
distinct_combos  = df_mapping.select("OfficerID", "ClientID").distinct().count()
duplicate_combos = total_rows - distinct_combos

print(f"    Total rows              : {total_rows}")
print(f"    Distinct OfficerID+ClientID combinations: {distinct_combos}")
print(f"    Duplicate combinations  : {duplicate_combos}")

if duplicate_combos > 0:
    print("    STATUS: DUPLICATE MAPPINGS DETECTED <-- ATTENTION")
    df_mapping.groupBy("OfficerID", "ClientID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("OfficerID", "ClientID") \
        .show(truncate=False)
else:
    print("    STATUS: All OfficerID + ClientID combinations are unique")

# ── Targeted Check 2: OfficerID referential integrity ─────────────────────────
print("\nTARGETED CHECK 2: OfficerID referential integrity")
print("    (Every OfficerID must exist in bronze_collections_officer)")

df_officer = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_collections_officer"
)

valid_officer_ids = df_officer.select("OfficerID")

orphaned_officers = df_mapping.join(
    valid_officer_ids,
    on="OfficerID",
    how="left_anti"
)
orphaned_officer_count = orphaned_officers.count()

print(f"    Mapping rows with no matching OfficerID: {orphaned_officer_count}")

if orphaned_officer_count > 0:
    print("    STATUS: ORPHANED OfficerID MAPPINGS DETECTED <-- ATTENTION")
    orphaned_officers.select("OfficerID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All OfficerIDs resolve to a valid officer")

# ── Targeted Check 3: ClientID referential integrity ──────────────────────────
print("\nTARGETED CHECK 3: ClientID referential integrity")
print("    (Every ClientID must exist in bronze_client_bank)")

df_client_bank = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_client_bank"
)

valid_client_ids = df_client_bank.select("ClientID")

orphaned_clients = df_mapping.join(
    valid_client_ids,
    on="ClientID",
    how="left_anti"
)
orphaned_client_count = orphaned_clients.count()

print(f"    Mapping rows with no matching ClientID: {orphaned_client_count}")

if orphaned_client_count > 0:
    print("    STATUS: ORPHANED ClientID MAPPINGS DETECTED <-- ATTENTION")
    orphaned_clients.select("OfficerID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All ClientIDs resolve to a valid client bank")

# ── Targeted Check 4: Suspended or inactive officers mapped to clients ─────────
print("\nTARGETED CHECK 4: Non-active officers still mapped to client portfolios")
print("    (Suspended or Inactive officers should not have active mappings)")

df_officer_status = df_officer.select("OfficerID", "Status")

non_active_mapped = df_mapping.join(
    df_officer_status,
    on="OfficerID",
    how="left"
).filter(
    F.col("Status").isin(["Inactive", "Suspended"])
)
non_active_count = non_active_mapped.count()

print(f"    Non-active officers with active mappings: {non_active_count}")

if non_active_count > 0:
    print("    STATUS: NON-ACTIVE OFFICERS MAPPED TO CLIENTS <-- ATTENTION")
    non_active_mapped.select("OfficerID", "ClientID", "Status") \
        .orderBy("Status", "OfficerID") \
        .show(truncate=False)
else:
    print("    STATUS: No non-active officers found in mapping table")

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_mapping.show(20, truncate=False)


# ── Targeted Check 5: AssignmentEndDate set but IsActive = true ───────────────
print("\nTARGETED CHECK 5: Logical contradiction - EndDate populated but IsActive = true")
print("    (A closed assignment must not be marked as active)")

contradictions = df_mapping.filter(
    F.col("AssignmentEndDate").isNotNull() &
    (F.col("IsActive") == True)
)
contradiction_count = contradictions.count()

print(f"    Rows with EndDate set and IsActive = true: {contradiction_count}")

if contradiction_count > 0:
    print("    STATUS: LOGICAL CONTRADICTION DETECTED <-- ATTENTION")
    contradictions.select(
        "MappingID", "OfficerID", "ClientID",
        "AssignmentEndDate", "IsActive"
    ).orderBy("OfficerID").show(truncate=False)
else:
    print("    STATUS: No contradictions found between EndDate and IsActive")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4: bronze_debtor
# **Source**: On-premises SQL Server via On-Premises Data Gateway<br>
# **Primary key**: DebtorID<br>
# **Role in pipeline**: Core entity table. Every loan, collateral position,<br>
# and collection action resolves to a debtor record.<br>
# DebtorID provenance: adopted from client banks during debt assignment,not CSL-generated.<br>
# Referential integrity check against bronze_client_bank (ClientID).<br>
# PII fields present: FullName, PhoneNumber, Address, DateOfBirth.<br>
# These fields are subject to masking at Silver-to-Gold transition.

# CELL ********************

# ── Section 4: bronze_debtor ──────────────────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[3]

df_debtor = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Primary key uniqueness (DebtorID) ───────────────────────
print("TARGETED CHECK 1: DebtorID uniqueness")

total_rows    = df_debtor.count()
distinct_ids  = df_debtor.select("DebtorID").distinct().count()
duplicate_ids = total_rows - distinct_ids

print(f"    Total rows        : {total_rows}")
print(f"    Distinct DebtorID : {distinct_ids}")
print(f"    Duplicate IDs     : {duplicate_ids}")

if duplicate_ids > 0:
    print("    STATUS: DUPLICATE DebtorIDs DETECTED <-- ATTENTION")
    df_debtor.groupBy("DebtorID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("DebtorID") \
        .show(truncate=False)
else:
    print("    STATUS: DebtorID is unique across all rows")

# ── Targeted Check 2: ClientID referential integrity ──────────────────────────
print("\nTARGETED CHECK 2: ClientID referential integrity")
print("    (Every ClientID must exist in bronze_client_bank)")

df_client_bank = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_client_bank"
)

valid_client_ids = df_client_bank.select("ClientID")

orphaned_debtors = df_debtor.join(
    valid_client_ids,
    on="ClientID",
    how="left_anti"
)
orphaned_count = orphaned_debtors.count()

print(f"    Debtor rows with no matching ClientID: {orphaned_count}")

if orphaned_count > 0:
    print("    STATUS: ORPHANED DEBTOR RECORDS DETECTED <-- ATTENTION")
    orphaned_debtors.select("DebtorID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All debtor ClientIDs resolve to a valid client bank")

# ── Targeted Check 3: Blank or whitespace-only critical fields ────────────────
print("\nTARGETED CHECK 3: Blank or whitespace-only critical fields")

for critical_col in ["FullName", "PhoneNumber", "EmailAddress", "ResidentialAddress"]:
    blank = df_debtor.filter(
        F.col(critical_col).isNull() |
        (F.trim(F.col(critical_col)) == "")
    ).count()
    flag = " <-- ATTENTION" if blank > 0 else ""
    print(f"    {critical_col:<25} blank/null count: {blank}{flag}")

# ── Targeted Check 4: NationalID uniqueness and format ────────────────────────
print("\nTARGETED CHECK 4: NationalID uniqueness")
print("    (NationalID is a government identifier - must be unique per debtor)")

distinct_nids  = df_debtor.select("NationalID").distinct().count()
duplicate_nids = total_rows - distinct_nids

print(f"    Total rows          : {total_rows}")
print(f"    Distinct NationalIDs: {distinct_nids}")
print(f"    Duplicate NationalIDs: {duplicate_nids}")

if duplicate_nids > 0:
    print("    STATUS: DUPLICATE NationalIDs DETECTED <-- ATTENTION")
    df_debtor.groupBy("NationalID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("NationalID") \
        .show(truncate=False)
else:
    print("    STATUS: NationalID is unique across all rows")

# ── Targeted Check 5: DateOnboarded sanity checks ─────────────────────────────
print("\nTARGETED CHECK 5: DateOnboarded sanity checks")

# Future onboarding date - cannot be onboarded in the future
future_onboarded = df_debtor.filter(
    F.col("DateOnboarded") > F.current_date()
).count()
print(f"    Future DateOnboarded        : {future_onboarded}", end="")
print(" <-- ATTENTION" if future_onboarded > 0 else "")

# Implausibly old - before CSL was operationally active
ancient_onboarded = df_debtor.filter(
    F.col("DateOnboarded") < F.lit("2000-01-01")
).count()
print(f"    DateOnboarded before 2000   : {ancient_onboarded}", end="")
print(" <-- ATTENTION" if ancient_onboarded > 0 else "")

# ── Targeted Check 6: AssignedOfficerID referential integrity ─────────────────
print("\nTARGETED CHECK 6: AssignedOfficerID referential integrity")
print("    (Every AssignedOfficerID must exist in bronze_collections_officer)")

df_officer = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_collections_officer"
)

valid_officer_ids = df_officer.select("OfficerID")

orphaned_assignments = df_debtor.join(
    valid_officer_ids,
    df_debtor["AssignedOfficerID"] == valid_officer_ids["OfficerID"],
    how="left_anti"
)
orphaned_assignment_count = orphaned_assignments.count()

print(f"    Debtors with no matching AssignedOfficerID: {orphaned_assignment_count}")

if orphaned_assignment_count > 0:
    print("    STATUS: ORPHANED OFFICER ASSIGNMENTS DETECTED <-- ATTENTION")
    orphaned_assignments.select("DebtorID", "AssignedOfficerID") \
        .show(truncate=False)
else:
    print("    STATUS: All AssignedOfficerIDs resolve to a valid officer")

# ── Targeted Check 7: Debtors assigned to non-active officers ─────────────────
print("\nTARGETED CHECK 7: Debtors assigned to non-active officers")
print("    (Suspended or Inactive officers should not manage active debtors)")

df_officer_status = df_officer.select("OfficerID", "Status")

non_active_assignments = df_debtor.join(
    df_officer_status,
    df_debtor["AssignedOfficerID"] == df_officer_status["OfficerID"],
    how="left"
).filter(
    F.col("Status").isin(["Inactive", "Suspended"])
)
non_active_count = non_active_assignments.count()

print(f"    Debtors assigned to non-active officers: {non_active_count}")

if non_active_count > 0:
    print("    STATUS: DEBTORS ASSIGNED TO NON-ACTIVE OFFICERS <-- ATTENTION")
    non_active_assignments.select(
        "DebtorID", "AssignedOfficerID", "Status"
    ).orderBy("Status", "AssignedOfficerID").show(truncate=False)
else:
    print("    STATUS: All debtor assignments point to active officers")

# ── Targeted Check 8: Region value distribution ───────────────────────────────
print("\nTARGETED CHECK 8: Region value distribution")
print("    (Documents all regions present - flags unexpected values)")

df_debtor.groupBy("Region") \
    .count() \
    .orderBy("Region") \
    .show(truncate=False)

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_debtor.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Targeted Check 9: NationalID length validation ────────────────────────────
print("TARGETED CHECK 9: NationalID length validation")
print("    (Nigerian NIN is 11 digits - checking for non-conforming lengths)")

df_debtor.groupBy(F.length("NationalID").alias("NationalID_length")) \
    .count() \
    .orderBy("NationalID_length") \
    .show(truncate=False)

invalid_length = df_debtor.filter(
    F.length("NationalID") != 11
).count()

print(f"    NationalIDs not 11 digits: {invalid_length}", end="")
print(" <-- ATTENTION" if invalid_length > 0 else "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5: bronze_loan
# **Source**: On-premises SQL Server via On-Premises Data Gateway<br>
# **Primary key**: LoanID<br>
# **Role in pipeline**: Core financial table. Drives LTV calculation.<br>
# OutstandingBalance is the denominator in the LTV ratio formula.<br>
# NOTE: LastPaymentDate and LastPaymentAmount are superseded by the <br>
# S3 bank balance update files (bronze_bank_balance_update) per the
# formal business rule: S3 is the system of record for payment data.<br>
# SQL Server is the fallback only.
# Referential integrity checks against bronze_debtor (DebtorID) and
# bronze_client_bank (ClientID).

# CELL ********************

# ── Section 5: bronze_loan ────────────────────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[4]

df_loan = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Primary key uniqueness (LoanID) ─────────────────────────
print("TARGETED CHECK 1: LoanID uniqueness")

total_rows    = df_loan.count()
distinct_ids  = df_loan.select("LoanID").distinct().count()
duplicate_ids = total_rows - distinct_ids

print(f"    Total rows       : {total_rows}")
print(f"    Distinct LoanID  : {distinct_ids}")
print(f"    Duplicate IDs    : {duplicate_ids}")

if duplicate_ids > 0:
    print("    STATUS: DUPLICATE LoanIDs DETECTED <-- ATTENTION")
    df_loan.groupBy("LoanID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("LoanID") \
        .show(truncate=False)
else:
    print("    STATUS: LoanID is unique across all rows")

# ── Targeted Check 2: DebtorID referential integrity ──────────────────────────
print("\nTARGETED CHECK 2: DebtorID referential integrity")
print("    (Every DebtorID must exist in bronze_debtor)")

df_debtor = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_debtor"
)

valid_debtor_ids = df_debtor.select("DebtorID")

orphaned_loans = df_loan.join(
    valid_debtor_ids,
    on="DebtorID",
    how="left_anti"
)
orphaned_loan_count = orphaned_loans.count()

print(f"    Loans with no matching DebtorID: {orphaned_loan_count}")

if orphaned_loan_count > 0:
    print("    STATUS: ORPHANED LOAN RECORDS DETECTED <-- ATTENTION")
    orphaned_loans.select("LoanID", "DebtorID").show(truncate=False)
else:
    print("    STATUS: All LoanIDs resolve to a valid debtor")

# ── Targeted Check 3: ClientID referential integrity ──────────────────────────
print("\nTARGETED CHECK 3: ClientID referential integrity")
print("    (Every ClientID must exist in bronze_client_bank)")

df_client_bank = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_client_bank"
)

valid_client_ids = df_client_bank.select("ClientID")

orphaned_client_loans = df_loan.join(
    valid_client_ids,
    on="ClientID",
    how="left_anti"
)
orphaned_client_count = orphaned_client_loans.count()

print(f"    Loans with no matching ClientID: {orphaned_client_count}")

if orphaned_client_count > 0:
    print("    STATUS: ORPHANED CLIENT REFERENCES DETECTED <-- ATTENTION")
    orphaned_client_loans.select("LoanID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All loan ClientIDs resolve to a valid client bank")

# ── Targeted Check 4: Negative or zero financial amounts ──────────────────────
print("\nTARGETED CHECK 4: Negative or zero financial amounts")
print("    (All financial amounts must be positive)")

for amount_col in ["InitialLoanAmount", "OutstandingBalance", "LastPaymentAmount"]:
    invalid = df_loan.filter(F.col(amount_col) <= 0).count()
    flag = " <-- ATTENTION" if invalid > 0 else ""
    print(f"    {amount_col:<25} zero/negative count: {invalid}{flag}")

# ── Targeted Check 5: OutstandingBalance exceeds InitialLoanAmount ─────────────
print("\nTARGETED CHECK 5: OutstandingBalance exceeds InitialLoanAmount")
print("    (Outstanding balance cannot exceed the original loan amount)")

balance_exceeds = df_loan.filter(
    F.col("OutstandingBalance") > F.col("InitialLoanAmount")
)
balance_exceeds_count = balance_exceeds.count()

print(f"    Loans where OutstandingBalance > InitialLoanAmount: {balance_exceeds_count}")

if balance_exceeds_count > 0:
    print("    STATUS: BALANCE EXCEEDS INITIAL AMOUNT <-- ATTENTION")
    balance_exceeds.select(
        "LoanID", "DebtorID", "InitialLoanAmount",
        "OutstandingBalance", "LoanStatus"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All outstanding balances are within initial loan amount")

# ── Targeted Check 6: LoanMaturityDate before LoanStartDate ───────────────────
print("\nTARGETED CHECK 6: LoanMaturityDate before LoanStartDate")
print("    (Maturity date cannot precede the start date)")

inverted_dates = df_loan.filter(
    F.col("LoanMaturityDate") < F.col("LoanStartDate")
)
inverted_count = inverted_dates.count()

print(f"    Loans with MaturityDate before StartDate: {inverted_count}")

if inverted_count > 0:
    print("    STATUS: INVERTED LOAN DATES DETECTED <-- ATTENTION")
    inverted_dates.select(
        "LoanID", "LoanStartDate", "LoanMaturityDate"
    ).show(truncate=False)
else:
    print("    STATUS: All loan date ranges are logically ordered")

# ── Targeted Check 7: LastPaymentDate before LoanStartDate ────────────────────
print("\nTARGETED CHECK 7: LastPaymentDate before LoanStartDate")
print("    (A payment cannot be made before the loan was issued)")

payment_before_loan = df_loan.filter(
    F.col("LastPaymentDate").isNotNull() &
    (F.col("LastPaymentDate") < F.col("LoanStartDate"))
)
payment_before_count = payment_before_loan.count()

print(f"    Loans with LastPaymentDate before LoanStartDate: {payment_before_count}")

if payment_before_count > 0:
    print("    STATUS: PAYMENT BEFORE LOAN START DETECTED <-- ATTENTION")
    payment_before_loan.select(
        "LoanID", "DebtorID", "LoanStartDate", "LastPaymentDate"
    ).show(truncate=False)
else:
    print("    STATUS: All payment dates fall after loan start dates")

# ── Targeted Check 8: Future LastPaymentDate ──────────────────────────────────
print("\nTARGETED CHECK 8: Future LastPaymentDate")
print("    (A payment cannot be recorded in the future)")

future_payment = df_loan.filter(
    F.col("LastPaymentDate").isNotNull() &
    (F.col("LastPaymentDate") > F.current_date())
)
future_payment_count = future_payment.count()

print(f"    Loans with future LastPaymentDate: {future_payment_count}")

if future_payment_count > 0:
    print("    STATUS: FUTURE PAYMENT DATES DETECTED <-- ATTENTION")
    future_payment.select(
        "LoanID", "DebtorID", "LastPaymentDate"
    ).show(truncate=False)
else:
    print("    STATUS: No future payment dates found")

# ── Targeted Check 9: Settled loans with non-zero OutstandingBalance ──────────
print("\nTARGETED CHECK 9: Settled loans with non-zero OutstandingBalance")
print("    (A settled loan must have zero outstanding balance)")

settled_with_balance = df_loan.filter(
    (F.col("LoanStatus") == "Settled") &
    (F.col("OutstandingBalance") > 0)
)
settled_balance_count = settled_with_balance.count()

print(f"    Settled loans with OutstandingBalance > 0: {settled_balance_count}")

if settled_balance_count > 0:
    print("    STATUS: LOGICAL CONTRADICTION DETECTED <-- ATTENTION")
    settled_with_balance.select(
        "LoanID", "DebtorID", "LoanStatus", "OutstandingBalance"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All settled loans have zero outstanding balance")

# ── Targeted Check 10: Settled loans with DaysPastDue > 0 ────────────────────
print("\nTARGETED CHECK 10: Settled loans with DaysPastDue > 0")
print("    (A settled loan cannot be past due)")

settled_past_due = df_loan.filter(
    (F.col("LoanStatus") == "Settled") &
    (F.col("DaysPastDue") > 0)
)
settled_past_due_count = settled_past_due.count()

print(f"    Settled loans with DaysPastDue > 0: {settled_past_due_count}")

if settled_past_due_count > 0:
    print("    STATUS: LOGICAL CONTRADICTION DETECTED <-- ATTENTION")
    settled_past_due.select(
        "LoanID", "DebtorID", "LoanStatus", "DaysPastDue"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All settled loans have zero days past due")

# ── Targeted Check 11: LoanStatus value distribution ─────────────────────────
print("\nTARGETED CHECK 11: LoanStatus value distribution")
print("    (Expected values: Active, Settled, Defaulted, Written-Off)")

df_loan.groupBy("LoanStatus") \
    .count() \
    .orderBy("LoanStatus") \
    .show(truncate=False)

unexpected_status = df_loan.filter(
    ~F.col("LoanStatus").isin(["Active", "Settled", "Defaulted", "Written-Off"])
).count()

if unexpected_status > 0:
    print(f"    STATUS: UNEXPECTED LoanStatus VALUES DETECTED <-- ATTENTION")
else:
    print(f"    STATUS: All LoanStatus values are within expected domain")

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_loan.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Supplementary Check: Zero OutstandingBalance by LoanStatus ────────────────
print("SUPPLEMENTARY CHECK: Zero OutstandingBalance breakdown by LoanStatus")
print("    (Zero balance is valid for Settled loans only)")

df_loan.filter(F.col("OutstandingBalance") == 0) \
    .groupBy("LoanStatus") \
    .count() \
    .orderBy("LoanStatus") \
    .show(truncate=False)

zero_balance_non_settled = df_loan.filter(
    (F.col("OutstandingBalance") == 0) &
    (F.col("LoanStatus") != "Settled")
).count()

print(f"    Zero balance on non-Settled loans: {zero_balance_non_settled}")

if zero_balance_non_settled > 0:
    print("    STATUS: LOGICAL CONTRADICTION DETECTED <-- ATTENTION")
    df_loan.filter(
        (F.col("OutstandingBalance") == 0) &
        (F.col("LoanStatus") != "Settled")
    ).select("LoanID", "DebtorID", "LoanStatus", "OutstandingBalance") \
     .orderBy("LoanStatus", "LoanID") \
     .show(truncate=False)
else:
    print("    STATUS: All zero balances belong to Settled loans only")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## Section 6: bronze_collateral
# **Source**: On-premises SQL Server via On-Premises Data Gateway<br>
# **Primary key**: CollateralID<br>
# **Role in pipeline**: Core asset table. QuantityHeld multiplied by<br>
# current market price drives the LTV denominator.<br>
# Data quality failures here directly corrupt LTV calculations.<br>
# Key join field for market price lookup: TickerSymbol.<br>
# Case inconsistency in TickerSymbol will cause silent join failures at the Silver layer.<br>
# Referential integrity checks against bronze_loan (LoanID) and bronze_debtor (DebtorID).<br>
# Cross-table check: DebtorID on collateral must match DebtorID on the corresponding loan record. <br>

# CELL ********************

# ── Section 6: bronze_collateral ──────────────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[5]

df_collateral = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Primary key uniqueness (CollateralID) ───────────────────
print("TARGETED CHECK 1: CollateralID uniqueness")

total_rows    = df_collateral.count()
distinct_ids  = df_collateral.select("CollateralID").distinct().count()
duplicate_ids = total_rows - distinct_ids

print(f"    Total rows          : {total_rows}")
print(f"    Distinct CollateralID: {distinct_ids}")
print(f"    Duplicate IDs       : {duplicate_ids}")

if duplicate_ids > 0:
    print("    STATUS: DUPLICATE CollateralIDs DETECTED <-- ATTENTION")
    df_collateral.groupBy("CollateralID") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("CollateralID") \
        .show(truncate=False)
else:
    print("    STATUS: CollateralID is unique across all rows")

# ── Targeted Check 2: LoanID referential integrity ────────────────────────────
print("\nTARGETED CHECK 2: LoanID referential integrity")
print("    (Every LoanID must exist in bronze_loan)")

df_loan = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_loan"
)

valid_loan_ids = df_loan.select("LoanID")

orphaned_collateral = df_collateral.join(
    valid_loan_ids,
    on="LoanID",
    how="left_anti"
)
orphaned_loan_count = orphaned_collateral.count()

print(f"    Collateral rows with no matching LoanID: {orphaned_loan_count}")

if orphaned_loan_count > 0:
    print("    STATUS: ORPHANED COLLATERAL RECORDS DETECTED <-- ATTENTION")
    orphaned_collateral.select("CollateralID", "LoanID").show(truncate=False)
else:
    print("    STATUS: All LoanIDs resolve to a valid loan")

# ── Targeted Check 3: DebtorID referential integrity ──────────────────────────
print("\nTARGETED CHECK 3: DebtorID referential integrity")
print("    (Every DebtorID must exist in bronze_debtor)")

df_debtor = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_debtor"
)

valid_debtor_ids = df_debtor.select("DebtorID")

orphaned_debtors = df_collateral.join(
    valid_debtor_ids,
    on="DebtorID",
    how="left_anti"
)
orphaned_debtor_count = orphaned_debtors.count()

print(f"    Collateral rows with no matching DebtorID: {orphaned_debtor_count}")

if orphaned_debtor_count > 0:
    print("    STATUS: ORPHANED DEBTOR REFERENCES DETECTED <-- ATTENTION")
    orphaned_debtors.select("CollateralID", "DebtorID").show(truncate=False)
else:
    print("    STATUS: All DebtorIDs resolve to a valid debtor")

# ── Targeted Check 4: Cross-table consistency - DebtorID matches loan record ───
print("\nTARGETED CHECK 4: Cross-table DebtorID consistency")
print("    (DebtorID on collateral must match DebtorID on the linked loan)")

df_loan_debtor = df_loan.select(
    F.col("LoanID"),
    F.col("DebtorID").alias("LoanDebtorID")
)

mismatched_debtors = df_collateral.join(
    df_loan_debtor,
    on="LoanID",
    how="left"
).filter(
    F.col("DebtorID") != F.col("LoanDebtorID")
)
mismatch_count = mismatched_debtors.count()

print(f"    Collateral rows where DebtorID mismatches loan DebtorID: {mismatch_count}")

if mismatch_count > 0:
    print("    STATUS: CROSS-TABLE DebtorID MISMATCH DETECTED <-- ATTENTION")
    mismatched_debtors.select(
        "CollateralID", "LoanID",
        "DebtorID", "LoanDebtorID"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All collateral DebtorIDs match their linked loan record")

# ── Targeted Check 5: TickerSymbol case consistency ───────────────────────────
print("\nTARGETED CHECK 5: TickerSymbol case consistency")
print("    (Tickers must be uppercase for reliable join to market price table)")

mixed_case_tickers = df_collateral.filter(
    F.col("TickerSymbol") != F.upper(F.col("TickerSymbol"))
)
mixed_case_count = mixed_case_tickers.count()

print(f"    TickerSymbols not in uppercase: {mixed_case_count}")

if mixed_case_count > 0:
    print("    STATUS: CASE INCONSISTENCY DETECTED <-- ATTENTION")
    print("    Silver fix: apply F.upper() to TickerSymbol on all rows")
    mixed_case_tickers.groupBy("TickerSymbol") \
        .count() \
        .orderBy("TickerSymbol") \
        .show(truncate=False)
else:
    print("    STATUS: All TickerSymbols are uppercase")

# ── Targeted Check 6: QuantityHeld and CollateralValueAtPledge ────────────────
print("\nTARGETED CHECK 6: Zero or negative QuantityHeld and CollateralValueAtPledge")
print("    (Both must be positive for LTV calculation to be valid)")

for amount_col in ["QuantityHeld", "CollateralValueAtPledge"]:
    invalid = df_collateral.filter(F.col(amount_col) <= 0).count()
    flag = " <-- ATTENTION" if invalid > 0 else ""
    print(f"    {amount_col:<30} zero/negative count: {invalid}{flag}")

# ── Targeted Check 7: CollateralPledgeDate before LoanStartDate ───────────────
print("\nTARGETED CHECK 7: CollateralPledgeDate before LoanStartDate")
print("    (Collateral cannot be pledged before the loan was issued)")

df_loan_dates = df_loan.select(
    F.col("LoanID"),
    F.col("LoanStartDate")
)

pledge_before_loan = df_collateral.join(
    df_loan_dates,
    on="LoanID",
    how="left"
).filter(
    F.col("CollateralPledgeDate") < F.col("LoanStartDate")
)
pledge_before_count = pledge_before_loan.count()

print(f"    Collateral pledged before loan start date: {pledge_before_count}")

if pledge_before_count > 0:
    print("    STATUS: PLEDGE DATE BEFORE LOAN START DETECTED <-- ATTENTION")
    pledge_before_loan.select(
        "CollateralID", "LoanID",
        "CollateralPledgeDate", "LoanStartDate"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All collateral pledge dates fall after loan start dates")

# ── Targeted Check 8: AssetType value distribution ────────────────────────────
print("\nTARGETED CHECK 8: AssetType value distribution")
print("    (Expected values: Stock, Crypto)")

df_collateral.groupBy("AssetType") \
    .count() \
    .orderBy("AssetType") \
    .show(truncate=False)

unexpected_asset_type = df_collateral.filter(
    ~F.col("AssetType").isin(["Stock", "Crypto"])
).count()

if unexpected_asset_type > 0:
    print(f"    STATUS: UNEXPECTED AssetType VALUES DETECTED <-- ATTENTION")
else:
    print(f"    STATUS: All AssetType values are within expected domain")

# ── Targeted Check 9: TickerSymbol and Exchange distribution ──────────────────
print("\nTARGETED CHECK 9: TickerSymbol distribution by AssetType")
print("    (Documents all tickers present - used for market price API planning)")

df_collateral.groupBy("AssetType", "TickerSymbol", "Exchange") \
    .count() \
    .orderBy("AssetType", "TickerSymbol") \
    .show(50, truncate=False)

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_collateral.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Section 7: bronze_bank_balance_update
# **Source**: Amazon S3 via Fabric Shortcut (client bank daily drops)<br>
# **Primary key**: LoanID + ReportingDate (composite)<br>
# **Role in pipeline**: Authoritative source for CurrentOutstandingBalance,<br>
# LastPaymentDate, and LastPaymentAmount per the formal business rule.<br>
# Overrides SQL Server loan table values at Silver layer.<br>
# Referential integrity checks against bronze_loan, bronze_debtor,
# and bronze_client_bank.<br>
# Cross-table check: DebtorID and ClientID must match the corresponding
# loan record in bronze_loan.

# CELL ********************

# ── Section 7: bronze_bank_balance_update ─────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[6]

df_balance = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Composite key uniqueness (LoanID + ReportingDate) ────────
print("TARGETED CHECK 1: Composite key uniqueness (LoanID + ReportingDate)")
print("    (Each loan should appear only once per reporting date)")

total_rows      = df_balance.count()
distinct_combos = df_balance.select("LoanID", "ReportingDate").distinct().count()
duplicate_combos = total_rows - distinct_combos

print(f"    Total rows                        : {total_rows}")
print(f"    Distinct LoanID + ReportingDate   : {distinct_combos}")
print(f"    Duplicate combinations            : {duplicate_combos}")

if duplicate_combos > 0:
    print("    STATUS: DUPLICATE LOAN+DATE COMBINATIONS DETECTED <-- ATTENTION")
    df_balance.groupBy("LoanID", "ReportingDate") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("ReportingDate", "LoanID") \
        .show(truncate=False)
else:
    print("    STATUS: All LoanID + ReportingDate combinations are unique")

# ── Targeted Check 2: LoanID referential integrity ────────────────────────────
print("\nTARGETED CHECK 2: LoanID referential integrity")
print("    (Every LoanID must exist in bronze_loan)")

df_loan = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_loan"
)

valid_loan_ids = df_loan.select("LoanID")

orphaned_loans = df_balance.join(
    valid_loan_ids,
    on="LoanID",
    how="left_anti"
)
orphaned_loan_count = orphaned_loans.count()

print(f"    Balance rows with no matching LoanID: {orphaned_loan_count}")

if orphaned_loan_count > 0:
    print("    STATUS: ORPHANED LoanID REFERENCES DETECTED <-- ATTENTION")
    orphaned_loans.select("LoanID", "DebtorID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All LoanIDs resolve to a valid loan")

# ── Targeted Check 3: DebtorID referential integrity ──────────────────────────
print("\nTARGETED CHECK 3: DebtorID referential integrity")
print("    (Every DebtorID must exist in bronze_debtor)")

df_debtor = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_debtor"
)

valid_debtor_ids = df_debtor.select("DebtorID")

orphaned_debtors = df_balance.join(
    valid_debtor_ids,
    on="DebtorID",
    how="left_anti"
)
orphaned_debtor_count = orphaned_debtors.count()

print(f"    Balance rows with no matching DebtorID: {orphaned_debtor_count}")

if orphaned_debtor_count > 0:
    print("    STATUS: ORPHANED DebtorID REFERENCES DETECTED <-- ATTENTION")
    orphaned_debtors.select("LoanID", "DebtorID").show(truncate=False)
else:
    print("    STATUS: All DebtorIDs resolve to a valid debtor")

# ── Targeted Check 4: ClientID referential integrity ──────────────────────────
print("\nTARGETED CHECK 4: ClientID referential integrity")
print("    (Every ClientID must exist in bronze_client_bank)")

df_client_bank = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_client_bank"
)

valid_client_ids = df_client_bank.select("ClientID")

orphaned_clients = df_balance.join(
    valid_client_ids,
    on="ClientID",
    how="left_anti"
)
orphaned_client_count = orphaned_clients.count()

print(f"    Balance rows with no matching ClientID: {orphaned_client_count}")

if orphaned_client_count > 0:
    print("    STATUS: ORPHANED ClientID REFERENCES DETECTED <-- ATTENTION")
    orphaned_clients.select("LoanID", "ClientID").show(truncate=False)
else:
    print("    STATUS: All ClientIDs resolve to a valid client bank")

# ── Targeted Check 5: Cross-table consistency - DebtorID and ClientID ──────────
print("\nTARGETED CHECK 5: Cross-table consistency (DebtorID and ClientID vs bronze_loan)")
print("    (DebtorID and ClientID must match the corresponding loan record)")

df_loan_keys = df_loan.select(
    F.col("LoanID"),
    F.col("DebtorID").alias("LoanDebtorID"),
    F.col("ClientID").alias("LoanClientID")
)

cross_check = df_balance.join(
    df_loan_keys,
    on="LoanID",
    how="left"
)

mismatched_debtor = cross_check.filter(
    F.col("DebtorID") != F.col("LoanDebtorID")
).count()

mismatched_client = cross_check.filter(
    F.col("ClientID") != F.col("LoanClientID")
).count()

print(f"    Rows where DebtorID mismatches loan record : {mismatched_debtor}", end="")
print(" <-- ATTENTION" if mismatched_debtor > 0 else "")

print(f"    Rows where ClientID mismatches loan record : {mismatched_client}", end="")
print(" <-- ATTENTION" if mismatched_client > 0 else "")

if mismatched_debtor > 0:
    cross_check.filter(F.col("DebtorID") != F.col("LoanDebtorID")) \
        .select("LoanID", "DebtorID", "LoanDebtorID") \
        .show(truncate=False)

if mismatched_client > 0:
    cross_check.filter(F.col("ClientID") != F.col("LoanClientID")) \
        .select("LoanID", "ClientID", "LoanClientID") \
        .show(truncate=False)

# ── Targeted Check 6: Negative or zero CurrentOutstandingBalance ───────────────
print("\nTARGETED CHECK 6: Negative or zero CurrentOutstandingBalance")
print("    (Balance must be positive for active accounts)")

zero_or_negative = df_balance.filter(
    F.col("CurrentOutstandingBalance") <= 0
)
zero_negative_count = zero_or_negative.count()

print(f"    Rows with zero or negative balance: {zero_negative_count}")

if zero_negative_count > 0:
    print("    STATUS: INVALID BALANCE VALUES DETECTED <-- ATTENTION")
    zero_or_negative.select(
        "LoanID", "DebtorID", "CurrentOutstandingBalance", "AccountStatus"
    ).orderBy("LoanID").show(truncate=False)
else:
    print("    STATUS: All balance values are positive")

# ── Targeted Check 7: Future LastPaymentDate ──────────────────────────────────
print("\nTARGETED CHECK 7: Future LastPaymentDate")
print("    (A payment cannot be recorded in the future)")

future_payment = df_balance.filter(
    F.col("LastPaymentDate").isNotNull() &
    (F.col("LastPaymentDate") > F.current_date())
)
future_payment_count = future_payment.count()

print(f"    Rows with future LastPaymentDate: {future_payment_count}")

if future_payment_count > 0:
    print("    STATUS: FUTURE PAYMENT DATES DETECTED <-- ATTENTION")
    future_payment.select(
        "LoanID", "DebtorID", "LastPaymentDate"
    ).show(truncate=False)
else:
    print("    STATUS: No future payment dates found")

# ── Targeted Check 8: ReportingDate distribution per ClientID ─────────────────
print("\nTARGETED CHECK 8: ReportingDate distribution per ClientID")
print("    (Each client drop should represent a single reporting date)")

df_balance.groupBy("ClientID", "ReportingDate") \
    .count() \
    .orderBy("ClientID", "ReportingDate") \
    .show(truncate=False)

# ── Targeted Check 9: AccountStatus value distribution ────────────────────────
print("\nTARGETED CHECK 9: AccountStatus value distribution")
print("    (Expected values: Performing, NPL, Settled, Written-Off)")

df_balance.groupBy("AccountStatus") \
    .count() \
    .orderBy("AccountStatus") \
    .show(truncate=False)

unexpected_status = df_balance.filter(
    ~F.col("AccountStatus").isin(["Performing", "NPL", "Settled", "Written-Off"])
).count()

if unexpected_status > 0:
    print(f"    STATUS: UNEXPECTED AccountStatus VALUES DETECTED <-- ATTENTION")
else:
    print(f"    STATUS: All AccountStatus values are within expected domain")

# ── Targeted Check 10: SourceFileName naming convention ───────────────────────
print("\nTARGETED CHECK 10: SourceFileName naming convention")
print("    (Expected pattern: client-drops/CLT-XXX/balance_update_YYYY_MM_DD.parquet)")

invalid_filename = df_balance.filter(
    ~F.col("SourceFileName").rlike(
        r"^client-drops/CLT-[0-9]{3}/balance_update_[0-9]{4}_[0-9]{2}_[0-9]{2}\.parquet$"
    )
)
invalid_filename_count = invalid_filename.count()

print(f"    Rows with unexpected SourceFileName format: {invalid_filename_count}")

if invalid_filename_count > 0:
    print("    STATUS: UNEXPECTED FILENAME FORMAT DETECTED <-- ATTENTION")
    invalid_filename.select("SourceFileName").distinct().show(truncate=False)
else:
    print("    STATUS: All SourceFileNames match expected naming convention")

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_balance.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 8: bronze_market_prices
# **Source**: Yahoo Finance API via Python Spark Notebook (yfinance)<br>
# **Primary key**: TickerSymbol + PriceDate (composite)<br>
# **Role in pipeline**: Provides current market price for LTV denominator.<br>
# LTV formula: OutstandingBalance / (Sum of QuantityHeld * Close price)<br>
# Edge case rule: if no price within 5 business days, flag as STALE_DATA.<br>
# Cross-table check: all tickers in bronze_collateral must have a
# corresponding price record in this table.

# CELL ********************

# ── Section 8: bronze_market_prices ───────────────────────────────────────────

TABLE_NAME = BRONZE_TABLES[7]

df_prices = spark.read.format("delta").load(
    f"{BRONZE_PATH}/{TABLE_NAME}"
)

# ── Baseline profile ──────────────────────────────────────────────────────────
profile_table(TABLE_NAME)

# ── Targeted Check 1: Composite key uniqueness (TickerSymbol + PriceDate) ──────
print("TARGETED CHECK 1: Composite key uniqueness (TickerSymbol + PriceDate)")
print("    (Each ticker should have exactly one price record per date)")

total_rows      = df_prices.count()
distinct_combos = df_prices.select("TickerSymbol", "PriceDate").distinct().count()
duplicate_combos = total_rows - distinct_combos

print(f"    Total rows                          : {total_rows}")
print(f"    Distinct TickerSymbol + PriceDate   : {distinct_combos}")
print(f"    Duplicate combinations              : {duplicate_combos}")

if duplicate_combos > 0:
    print("    STATUS: DUPLICATE PRICE RECORDS DETECTED <-- ATTENTION")
    df_prices.groupBy("TickerSymbol", "PriceDate") \
        .count() \
        .filter(F.col("count") > 1) \
        .orderBy("TickerSymbol", "PriceDate") \
        .show(truncate=False)
else:
    print("    STATUS: All TickerSymbol + PriceDate combinations are unique")

# ── Targeted Check 2: TickerSymbol distribution ───────────────────────────────
print("\nTARGETED CHECK 2: TickerSymbol distribution")
print("    (Documents all tickers present and their record counts)")

df_prices.groupBy("TickerSymbol") \
    .count() \
    .orderBy("TickerSymbol") \
    .show(truncate=False)

# ── Targeted Check 3: Cross-table ticker coverage ─────────────────────────────
print("\nTARGETED CHECK 3: Cross-table ticker coverage")
print("    (Every valid ticker in bronze_collateral must exist in bronze_market_prices)")
print("    (Using uppercase normalised tickers from collateral as the baseline)")

df_collateral = spark.read.format("delta").load(
    f"{BRONZE_PATH}/bronze_collateral"
)

# Get distinct normalised tickers from collateral
# We uppercase and exclude known dirty tickers that cannot be resolved
collateral_tickers = df_collateral.select(
    F.upper(F.col("TickerSymbol")).alias("TickerSymbol")
).distinct()

price_tickers = df_prices.select("TickerSymbol").distinct()

# Tickers in collateral with no matching price record
missing_prices = collateral_tickers.join(
    price_tickers,
    on="TickerSymbol",
    how="left_anti"
)
missing_price_count = missing_prices.count()

print(f"    Collateral tickers with no price record: {missing_price_count}")

if missing_price_count > 0:
    print("    STATUS: MISSING PRICE COVERAGE DETECTED <-- ATTENTION")
    print("    NOTE: Some may be known dirty tickers from Section 6")
    missing_prices.show(truncate=False)
else:
    print("    STATUS: All collateral tickers have at least one price record")

# ── Targeted Check 4: Tickers in prices not in collateral ─────────────────────
print("\nTARGETED CHECK 4: Tickers in bronze_market_prices not in bronze_collateral")
print("    (Identifies prices being fetched for assets not in the portfolio)")

extra_tickers = price_tickers.join(
    collateral_tickers,
    on="TickerSymbol",
    how="left_anti"
)
extra_ticker_count = extra_tickers.count()

print(f"    Price tickers with no collateral record: {extra_ticker_count}")

if extra_ticker_count > 0:
    print("    STATUS: EXTRA TICKERS IN PRICE TABLE <-- ATTENTION")
    extra_tickers.show(truncate=False)
else:
    print("    STATUS: All price tickers correspond to a collateral record")

# ── Targeted Check 5: Zero or negative price values ──────────────────────────
print("\nTARGETED CHECK 5: Zero or negative price values")
print("    (All OHLC values must be positive)")

for price_col in ["Open", "High", "Low", "Close"]:
    invalid = df_prices.filter(F.col(price_col) <= 0).count()
    flag = " <-- ATTENTION" if invalid > 0 else ""
    print(f"    {price_col:<10} zero/negative count: {invalid}{flag}")

# ── Targeted Check 6: High/Low logical consistency ────────────────────────────
print("\nTARGETED CHECK 6: High/Low logical consistency")
print("    (High must always be greater than or equal to Low)")

high_below_low = df_prices.filter(
    F.col("High") < F.col("Low")
).count()

print(f"    Rows where High < Low: {high_below_low}", end="")
print(" <-- ATTENTION" if high_below_low > 0 else "")

# ── Targeted Check 7: Close price within High/Low range ───────────────────────
print("\nTARGETED CHECK 7: Close price within High/Low range")
print("    (Close must be between Low and High inclusive)")

close_out_of_range = df_prices.filter(
    (F.col("Close") > F.col("High")) |
    (F.col("Close") < F.col("Low"))
).count()

print(f"    Rows where Close is outside High/Low range: {close_out_of_range}", end="")
print(" <-- ATTENTION" if close_out_of_range > 0 else "")

# ── Targeted Check 8: PriceDate range and coverage ────────────────────────────
print("\nTARGETED CHECK 8: PriceDate range and coverage")
print("    (Documents available price history for stale data edge case handling)")

df_prices.select(
    F.min("PriceDate").alias("Earliest PriceDate"),
    F.max("PriceDate").alias("Latest PriceDate"),
    F.countDistinct("PriceDate").alias("Distinct dates")
).show(truncate=False)

# ── Targeted Check 9: PriceDate stored as Timestamp not Date ──────────────────
print("\nTARGETED CHECK 9: PriceDate data type check")
print("    (PriceDate is TimestampType - must be cast to DateType in Silver)")
print("    (Timestamp vs Date mismatch causes silent join failures with other tables)")

price_date_type = dict(df_prices.dtypes)["PriceDate"]
print(f"    PriceDate stored as: {price_date_type}")

if price_date_type != "date":
    print("    STATUS: TYPE MISMATCH DETECTED <-- ATTENTION")
    print("    Silver fix: cast PriceDate to DateType")
else:
    print("    STATUS: PriceDate is correctly typed as DateType")

# ── Preview ───────────────────────────────────────────────────────────────────
print("\nTABLE PREVIEW (first 20 rows)")
df_prices.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
