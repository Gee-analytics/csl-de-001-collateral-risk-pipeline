# Gold Layer Transformation Summary
## CSL-DE-001 | Collateral Risk Monitoring and Margin Call Automation System

| Field | Detail |
|---|---|
| Document Code | CSL-DE-001-GOLD-SUMMARY |
| Version | v1.0 |
| Status | Complete |
| Prepared By | Data Engineering Team |
| Date | March 2026 |
| Notebook | `nb_gold_aggregation` |
| Target Layer | Gold |
| Platform | Microsoft Fabric |

---

## 1. Purpose

This document summarises the design, implementation, and output of the Gold layer
aggregation notebook for CSL-DE-001. The Gold layer is the final transformation
layer of the Medallion pipeline. It reads from 5 confirmed Silver tables and
produces 9 Gold tables forming a production-ready star schema that powers the
Power BI Risk Command Centre dashboard.

All business logic implemented in this layer traces directly to
`CSL_DE_001_Business_Rules_v1.0.md`. Any deviation from that document is
explicitly flagged as a gap in Section 7 of this report.

---

## 2. Input Tables

| Table | Source | Row Count | Load Pattern |
|---|---|---|---|
| `silver_debtor_loan_collateral` | On-premises SQL Server | 300 | SCD Type 2 |
| `silver_bank_balance_update` | Amazon S3 | 956 | Append with watermark |
| `silver_market_prices` | yfinance API | 1,888 | Append with watermark |
| `silver_collections_officer` | On-premises SQL Server | 20 | Full overwrite |
| `silver_officer_client_mapping` | On-premises SQL Server | 53 | SCD Type 2 |
| `silver_client_bank` | On-premises SQL Server | 4 | Full overwrite |

All inputs sourced exclusively from the Silver layer. No Bronze tables read
directly in the Gold notebook with the exception of the original design
which was corrected during build when `silver_client_bank` was confirmed to exist.

---

## 3. Output Tables

### 3.1 Fact Tables

| Table | Grain | Row Count | Write Strategy |
|---|---|---|---|
| `fact_ltv_daily_snapshot` | One row per debtor per collateral asset per date | 249 per daily run | Append with idempotency guard |
| `fact_market_prices_daily` | One row per ticker per date | 1,888 (full history on first run) | Overwrite first run, watermark append |

### 3.2 Dimension Tables

| Table | Row Count | Write Strategy | SCD Type |
|---|---|---|---|
| `dim_date` | 4,748 | Write once with idempotency check | Static |
| `dim_collections_officer` | 53 | Delta merge | SCD Type 2 |
| `dim_client_bank` | 4 | Full overwrite | None |
| `dim_debtor` | 204 | Delta merge | SCD Type 2 |
| `dim_loan` | 250 | Delta merge | SCD Type 2 |
| `dim_collateral_asset` | 300 | Delta merge | SCD Type 2 |

### 3.3 Audit Table

| Table | Row Count | Write Strategy |
|---|---|---|
| `gold_audit_log` | 8 per pipeline run | Append with idempotency guard |

---

## 4. Business Logic Implemented

All rules reference `CSL_DE_001_Business_Rules_v1.0.md` by section.

### 4.1 Balance Authority Rule
**Reference:** BRD Section 3

The authoritative outstanding balance per loan is resolved using a LEFT JOIN
from `silver_debtor_loan_collateral` to `silver_bank_balance_update` on `LoanID`.
COALESCE selects the S3 balance where available, falling back to the on-premises
SQL Server balance where not. A `BalanceSource` column records which source was
used for audit purposes.

**Results from first pipeline run:**

| Balance Source | Record Count |
|---|---|
| S3_UPDATE | 236 |
| ONPREM_ORIGINAL | 13 |

### 4.2 Stale Price Logic
**Reference:** BRD Section 6

The current market price per ticker is resolved in three tiers:

| Status | Condition | Action |
|---|---|---|
| CURRENT | Price exists for today's pipeline run date | Use today's ClosePrice |
| STALE | No price today but price exists within 5 business days | Use most recent available price |
| MISSING | No price within 5 business day lookback window | Exclude from LTV, retain for audit |

**Results from first pipeline run:**

| DataFreshnessStatus | Record Count |
|---|---|
| STALE | 232 |
| MISSING | 17 |

All 17 MISSING records belong to BTC-USD. Root cause is yfinance returning
NULL ClosePrice for BTC-USD on certain weekend dates. Equity tickers resolved
cleanly. See Section 7 for documented gap and remediation path.

Note: STALE status on first run is expected. Silver layer was not yet automated
and attached to the master pipeline at time of first Gold run. Bronze had
ingested today's prices but Silver had not yet processed them. Once the full
pipeline is wired, equity tickers will resolve as CURRENT on every weekday run.

### 4.3 Collateral Eligibility
**Reference:** BRD Section 7

`EligibleForLTV` is set at Silver layer. Gold respects this flag and does not
re-evaluate eligibility logic. Ineligible positions are excluded from
`TotalCollateralValue` but retained in the fact table for audit.

### 4.4 LTV Calculation
**Reference:** BRD Sections 2 and 8

LTV is calculated at debtor level, not loan or collateral level.

```
CurrentCollateralValue  = QuantityHeld x CurrentMarketPrice
TotalCollateralValue    = SUM(CurrentCollateralValue) across all eligible positions per DebtorID
TotalOutstandingBalance = SUM(ResolvedOutstandingBalance) across all active and defaulted loans per DebtorID
LTV_Ratio               = TotalOutstandingBalance / TotalCollateralValue
LTV_Percentage          = LTV_Ratio x 100
```

Division by zero is handled explicitly. Where `TotalCollateralValue` is zero
or NULL, `LTV_Ratio` is set to NULL and `LTV_Calculation_Status` is set to
`NO_ELIGIBLE_COLLATERAL`.

**Loan status scope:** Active and Defaulted loans only. Settled loans excluded
per BRD Section 10.

### 4.5 Risk Flag Assignment
**Reference:** BRD Section 4

| Risk Flag | LTV Ratio Condition | ImmediateActionRequired |
|---|---|---|
| LOW | LTV Ratio < 0.60 | FALSE |
| MEDIUM | 0.60 <= LTV Ratio < 0.80 | FALSE |
| HIGH | 0.80 <= LTV Ratio < 1.00 | TRUE |
| CRITICAL | LTV Ratio >= 1.00 | TRUE |
| UNDETERMINED | LTV could not be calculated | FALSE |

**Risk distribution from first pipeline run:**

| Risk Flag | Count |
|---|---|
| CRITICAL | 221 |
| HIGH | 4 |
| MEDIUM | 6 |
| LOW | 8 |
| UNDETERMINED | 10 |

225 out of 249 accounts carry `ImmediateActionRequired = TRUE`. This is
consistent with a debt recovery portfolio where CSL manages defaulted and
delinquent accounts rather than performing loans.

### 4.6 Collateral Coverage Shortfall
**Reference:** BRD Section 5

```
RequiredCollateralValue      = TotalOutstandingBalance / 0.80
CollateralCoverageShortfall  = RequiredCollateralValue - TotalCollateralValue
```

Set to NULL where result is zero or negative. A positive result represents
the NGN amount by which collateral must increase to return the account to
safe status. Provides collection officers with an actionable figure rather
than just a ratio.

---

## 5. Dimensional Modelling Decisions

### 5.1 Surrogate Key Strategy

All dimension surrogate keys are generated as SHA-256 hashes of the natural
key concatenated with `EffectiveStartDate`. This approach is deterministic,
meaning the same inputs always produce the same surrogate key regardless of
how many times the pipeline runs. This is preferable to auto-increment integers
which reset on reload, and to UUID which is non-deterministic across runs.

### 5.2 Deduplication Before Dimension Builds

`silver_debtor_loan_collateral` is at collateral asset grain. Reading it
directly into `dim_debtor` or `dim_loan` without deduplication would cause
row inflation due to the one-to-many relationships between debtors, loans,
and collateral assets.

Deduplication strategy applied before each dimension build:

| Dimension | Deduplicate On | Confirmed Count |
|---|---|---|
| `dim_debtor` | DebtorID | 204 distinct debtors |
| `dim_loan` | LoanID | 250 distinct loans (confirmed via SQL query) |
| `dim_collateral_asset` | CollateralID | 300 distinct assets (no collapse, collateral is finest grain) |

### 5.3 PII Handling

`NationalID` is already hashed at Silver layer. No further action at Gold.

`PhoneNumber`, `EmailAddress`, and `ResidentialAddress` pass through in
readable form in `dim_debtor`. Access is controlled via Row Level Security
and Object Level Security at the Power BI semantic model layer. Hashing
these fields was considered and rejected because collections officers
require contact details to reach debtors. Hashing is irreversible and
would destroy operational utility.

### 5.4 dim_client_bank Source Correction

Initial design sourced `dim_client_bank` directly from `bronze_client_bank`.
This was corrected during build when `silver_client_bank` was confirmed to
exist with all 4 rows clean and correctly typed. Sourcing from Bronze would
have broken the architectural contract that Gold reads exclusively from Silver.

### 5.5 gold_audit_log Separation

The original system prompt specified writing audit rows to `gold_pipeline_metadata`.
During build it was confirmed that `gold_pipeline_metadata` is the Bronze watermark
store, seeded by `nb_setup_pipeline_metadata` and owned by the Stream A Fabric
Data Pipeline. Writing Gold audit rows to this table would have mixed two
fundamentally different concerns in one table.

A dedicated `gold_audit_log` table was created to receive all Gold execution
audit rows. `gold_pipeline_metadata` is read-only from this notebook and is
never written to.

---

## 6. Idempotency

The notebook is designed to be safely rerunnable without producing duplicate
or corrupt data.

| Table | Idempotency Mechanism |
|---|---|
| `dim_date` | Catalog check before write. Skips entirely if already populated. |
| `dim_collections_officer` | Delta merge on surrogate key. Idempotent by nature. |
| `dim_client_bank` | Full overwrite. Safe by definition. |
| `dim_debtor` | Delta merge on surrogate key. Idempotent by nature. |
| `dim_loan` | Delta merge on surrogate key. Idempotent by nature. |
| `dim_collateral_asset` | Delta merge on surrogate key. Idempotent by nature. |
| `fact_ltv_daily_snapshot` | Delete-then-insert on PipelineRunDate before append. |
| `fact_market_prices_daily` | Watermark-based append. Delete-then-insert on new PriceDates. |
| `gold_audit_log` | Delete on RunID before append. |

Rerunning the full notebook on the same day produces identical results with
no duplication in any table.

---

## 7. Known Gaps and Technical Debt

| Gap | Severity | Remediation |
|---|---|---|
| BTC-USD weekend price gaps from yfinance | MEDIUM | Analytics team to source weekend prices from Coinbase or Binance API. Affected records flagged as MISSING and excluded from LTV until resolved. |
| `Close` column in `silver_market_prices` should be renamed `ClosePrice` | LOW | Silver transformation notebook to be updated to rename `Close` to `ClosePrice` for consistency with BRD terminology. |
| Public holidays not modelled in `IsMarketDay` in `dim_date` | LOW | Licensed holiday calendar required for production. Current model treats all weekdays as market days. |
| Silver notebook not yet wired into master pipeline | HIGH | Orchestration task pending. Bronze ingests daily but Silver and Gold are not yet on automated schedule. |
| Per-step timing not captured in `gold_audit_log` | LOW | Future enhancement. RunTimestamp reflects notebook-level start time not individual step timing. |
| `fact_market_prices_daily` not included in referential integrity validation | LOW | Section 11 validates fact_ltv_daily_snapshot foreign keys only. fact_market_prices_daily joins to dim_date and dim_collateral_asset on TickerSymbol. Validation to be added in next iteration. |

---

## 8. Referential Integrity Validation

A pre-write validation check was implemented in Section 11 confirming every
foreign key value in `fact_ltv_daily_snapshot` has a corresponding record in
its dimension table before the fact write occurs. This is the substitute for
database-level foreign key enforcement which is not available in a Lakehouse.

**Validation results from first pipeline run:**

| Foreign Key | Orphan Count |
|---|---|
| DebtorID in dim_debtor | 0 |
| LoanID in dim_loan | 0 |
| CollateralID in dim_collateral_asset | 0 |
| AssignedOfficerID in dim_collections_officer | 0 |
| ClientID in dim_client_bank | 0 |
| DateKey in dim_date | 0 |

All 249 records passed validation. Zero records quarantined.

Records that fail validation are written to `fact_ltv_quarantine_log` rather
than the fact table and logged to `gold_audit_log` for investigation.

---

## 9. Pipeline Run Results

**First pipeline run: 26 March 2026**

| Metric | Value |
|---|---|
| Run ID | d485fb22-cb1c-4d94-bcba-978fb4fbeb73 |
| Pipeline Run Date | 2026-03-26 |
| Eligible records after loan status filter | 249 |
| Records written to fact_ltv_daily_snapshot | 249 |
| Records quarantined | 0 |
| Audit rows written | 8 |

**Second pipeline run: 27 March 2026**

| Metric | Value |
|---|---|
| Run ID | 43a778c8-6041-40c9-8ed6-519fc4ec8e9f |
| Pipeline Run Date | 2026-03-27 |
| Records written to fact_ltv_daily_snapshot | 249 |
| Records quarantined | 0 |
| Total rows in fact_ltv_daily_snapshot | 498 (2 daily snapshots) |

---

## 10. Star Schema Overview

```
                        dim_date
                           |
                           | DateKey
                           |
dim_collections_officer    |          dim_client_bank
        |                  |                 |
        | AssignedOfficerID|                 | ClientID
        |                  |                 |
        +------ fact_ltv_daily_snapshot -----+
        |                  |                 |
        | DebtorID         | CollateralID    | LoanID
        |                  |                 |
     dim_debtor    dim_collateral_asset    dim_loan


fact_market_prices_daily
        |
        | DateKey --------- dim_date
        | TickerSymbol ----- dim_collateral_asset
```

Two fact tables share conformed dimensions `dim_date` and `dim_collateral_asset`.
This allows Power BI to cross filter between LTV metrics and price trend analysis
using shared dimension context.

---

## 11. What Comes Next

- Wire `nb_gold_aggregation` into `pl_master_orchestration` as Step 5 in the pipeline dependency chain
- Automate Silver layer so it runs on Bronze success before Gold executes
- Connect Power BI to Gold layer via Direct Lake mode
- Build the Risk Command Centre dashboard including LTV summary, high risk accounts table, collateral value trend chart, and price history view
- Implement Row Level Security mapping AgentID to ClientID and Region
- Implement Object Level Security restricting sensitive PII columns to authorised roles
- Complete Data Dictionary covering all Gold layer tables and column definitions

---

## 12. Document Version History

| Version | Date | Author | Change Summary |
|---|---|---|---|
| v1.0 | March 2026 | (Gabriel Obot) Data Engineering Team | Initial version. Gold layer complete and validated. |

---

*CSL-DE-001 | ******** | Data Engineering Division | March 2026*
