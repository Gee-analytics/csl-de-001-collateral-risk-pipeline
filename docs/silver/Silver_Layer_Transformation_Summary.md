

# Silver Layer Transformation Summary

## Project: CSL-DE-001 | Collateral Risk Monitoring & Margin Call Automation
## Notebook: `nb_silver_transformation`
## Author: Gabriel Obot(Data Engineering Team)
## Date: March 2026
## Status: Complete

---

## Overview

`nb_silver_transformation` is the most technically complex notebook in the
CSL-DE-001 pipeline. It takes raw Bronze layer data from 3 source systems
and produces 5 clean, typed, flagged, and audit-ready Silver Delta tables
that the Gold layer aggregation notebook reads from directly.

The notebook consists of 9 sections and 50+ steps. Every transformation
decision is evidence-based, traceable to findings documented in
`docs/data-quality/DQ_Report_Bronze_Layer.md`.

---

## Silver Tables Produced

| Table | Rows | Write Strategy | Source |
|---|---|---|---|
| `silver_collections_officer` | 20 | Full overwrite | On-premises SQL Server |
| `silver_officer_client_mapping` | 53 | Delta merge SCD Type 2 | On-premises SQL Server |
| `silver_debtor_loan_collateral` | 300 | Delta merge SCD Type 2 | On-premises SQL Server |
| `silver_bank_balance_update` | 956 | Append with watermark | Amazon S3 |
| `silver_market_prices` | 1888 | Append with watermark | yfinance API |

---

## Notebook Structure

| Section | Description |
|---|---|
| Section 1 | Imports, configuration, and audit logging helper |
| Section 2 | Read all 8 Bronze tables with row count baseline |
| Section 3 | Clean and transform `silver_collections_officer` |
| Section 4 | Clean and transform `silver_officer_client_mapping` |
| Section 5 | Clean and transform `silver_debtor_loan_collateral` (three phase) |
| Section 6 | Clean and transform `silver_bank_balance_update` |
| Section 7 | Clean and transform `silver_market_prices` |
| Section 8 | Write all 5 Silver tables to Delta |
| Section 9 | Log results to `silver_pipeline_audit` |

---

## Key Design Decisions

### Quarantine Pattern
No record is ever silently dropped. Every data quality issue is captured
in a `data_quality_flag` column carrying pipe-separated issue codes.
Records with critical issues are flagged as ineligible for LTV calculation
via `is_eligible_for_ltv = False` but remain fully visible in Silver for
investigation and remediation.

### Composite `data_quality_flag` Column
Each Silver table carries a single composite flag column rather than
multiple individual flag columns. This pattern is more scalable, easier
to query, and avoids schema proliferation as new issue types are discovered.
Multiple issues on a single record are concatenated with a pipe separator.
Example: `PAYMENT_BEFORE_LOAN_START|SETTLED_WITH_BALANCE`

### `is_eligible_for_ltv` Boolean
A single boolean column on loan, collateral, and balance tables that the
Gold layer uses as the sole filter before computing LTV ratios. Silver
makes the eligibility decision. Gold respects it. This separates
data quality concerns from business logic cleanly.

### SCD Type 2 at Silver
SCD Type 2 is applied to `silver_officer_client_mapping` (assignment
changes) and `silver_debtor_loan_collateral` (loan status changes) at
the Silver layer rather than the Gold layer.

Rationale: the Gold layer in CSL-DE-001 produces a daily LTV snapshot
fact table, not a dimensional star schema. There are no formal dimension
tables at Gold to carry versioned history. Silver is the system of record
in this Lakehouse. Preserving historical state at Silver gives the
strongest audit chain for a financial services context.

CSL-DE-002 will demonstrate the alternative Kimball pattern with SCD
Type 2 at Gold in a Fabric Data Warehouse with a proper star schema.

### Deterministic Surrogate Keys
Surrogate keys on `silver_officer_client_mapping` (`mapping_sk`) and
`silver_debtor_loan_collateral` (`dlc_sk`) are generated as SHA-256
hashes of the natural key components. Deterministic hashing ensures
key stability across daily pipeline runs. The same input always produces
the same key. UUID was rejected because it is non-deterministic.

### PII Handling
`NationalID` is SHA-256 hashed at Silver. It has no dashboard business
purpose and no user role needs to see the raw value. Hashing at Silver
means the raw NIN never reaches Power BI.

`PhoneNumber`, `EmailAddress`, and `ResidentialAddress` are passed through
in readable form. Collections officers need these values to make recovery
calls. These fields are protected at the Power BI semantic model level
via Object Level Security. Static hashing was evaluated and rejected
because it destroys operational usability.

### Baseline STRING Normalisation
A trim sweep is applied to all STRING columns at the start of each
transformation phase before any specific cleaning logic runs. This
eliminates whitespace as a variable and prevents downstream logic from
being tripped by invisible leading or trailing spaces.

### Dynamic Audit Logging
The `silver_pipeline_audit` table records one row per Silver table per
pipeline run with dynamic row counts computed at runtime. A deduplication
guard prevents duplicate audit entries on notebook re-runs during
development. The audit table is separate from the Bronze watermark
control table `gold_pipeline_metadata` which serves a different purpose.

### Idempotency
The Silver layer is fully idempotent. Running the notebook multiple times 
produces identical results:
- Full overwrite tables: always produce the same row count
- Append with watermark tables: watermark prevents duplicate appends
- SCD Type 2 merge tables: natural key guard prevents duplicate inserts
  on same-day re-runs. If a row with the same natural key already exists
  in the table, the insert is skipped entirely.

---

## Data Quality Issues Handled

### silver_collections_officer
| Issue | Finding | Action |
|---|---|---|
| Phone number format | Mixed formats: local, missing prefix, malformed | Standardise to +234XXXXXXXXX, flag PHONE_STANDARDISED or PHONE_INVALID |
| Suspect email | OFF-0008 email does not match officer name | Flag SUSPECT_EMAIL, retain for HR escalation |
| Status validation | Suspended confirmed as valid status during profiling | Validate against Active, Inactive, Suspended domain |
| Missing DateJoined | 2 NULL values on active officers | Replace with sentinel 1900-01-01, flag MISSING_JOIN_DATE |

### silver_officer_client_mapping
| Issue | Finding | Action |
|---|---|---|
| Duplicate mappings | 3 duplicate OfficerID + ClientID combinations | Deduplicate keeping latest AssignmentStartDate |
| IsActive contradiction | 4 rows with past AssignmentEndDate but IsActive = True | Correct IsActive to False, flag CORRECTED |
| Inactive officer mappings | 9 mappings belong to Inactive or Suspended officers | Flag INACTIVE_OFFICER_MAPPING, escalate for reassignment |

### silver_debtor_loan_collateral
| Issue | Finding | Action |
|---|---|---|
| NationalID padding | 10 NationalIDs missing leading zero | Pad to 11 digits using F.lpad |
| Duplicate NationalIDs | 4 NationalIDs shared across 8 DebtorIDs | Flag DUPLICATE_NATIONAL_ID, escalate for human review |
| Inactive officer assignments | 41 debtors assigned to inactive officers | Flag INACTIVE_OFFICER_ASSIGNMENT |
| Missing email | 3 NULL EmailAddress values | Flag MISSING_EMAIL, retain NULL |
| Payment before loan start | 83 loans with LastPaymentDate before LoanStartDate | Flag PAYMENT_BEFORE_LOAN_START, S3 supersedes SQL Server value |
| Settled with balance | 7 loans Settled but OutstandingBalance > 0 | Flag SETTLED_WITH_BALANCE, set is_eligible_for_ltv = False |
| Balance exceeds initial | 8 loans where OutstandingBalance > InitialLoanAmount | Flag BALANCE_EXCEEDS_INITIAL, set is_eligible_for_ltv = False |
| Inverted loan dates | 3 loans where LoanMaturityDate before LoanStartDate | Flag INVERTED_LOAN_DATES, set is_eligible_for_ltv = False |
| Ticker corrections | APPLE INC, BTC/USD, mixed case tickers | Map to correct tickers, uppercase all |
| Asset type corrections | NVDA classified as Crypto, BTC/USD as Stock | Correct AssetType and Exchange using ticker lookup |
| NYSE misclassification | 222 NASDAQ stocks incorrectly mapped to NYSE | Correct Exchange using ticker lookup dictionary |
| Invalid quantity | 4 records with zero or negative QuantityHeld | Flag INVALID_QUANTITY, set is_eligible_for_ltv = False |
| Invalid pledge value | 5 records with zero CollateralValueAtPledge | Flag INVALID_PLEDGE_VALUE, set is_eligible_for_ltv = False |

### silver_bank_balance_update
| Issue | Finding | Action |
|---|---|---|
| Duplicate records | Client bank dropped same file twice | Deduplicate on LoanID + ReportingDate |
| String literal null | AccountStatus contains word null as string | Replace with actual NULL |
| AccountStatus standardisation | WRITTEN_OFF, Suspended, UNKNOWN variants | Standardise to controlled vocabulary |
| Missing balance | NULL CurrentOutstandingBalance | Flag BALANCE_UNAVAILABLE, set is_eligible_for_ltv = False |
| TIMESTAMP to DATE | ReportingDate and LastPaymentDate stored as TIMESTAMP | Cast to DATE using F.to_date() |
| DOUBLE to DECIMAL | Financial columns stored as DOUBLE | Cast to DecimalType(18,2) |

### silver_market_prices
| Issue | Finding | Action |
|---|---|---|
| NULL price records | 3 BTC-USD records with all OHLC values NULL | Flag INVALID_PRICE, Gold applies 5 business day fallback |
| TIMESTAMP to DATE | PriceDate stored as TIMESTAMP | Cast to DATE using F.to_date() |
| DOUBLE to DECIMAL | OHLC columns stored as DOUBLE | Cast to DecimalType(18,6) |
| DOUBLE to LONG | Volume stored as DOUBLE | Cast to LongType() |

---

## Open Items for Remediation

| Item | Table | Action Required |
|---|---|---|
| 3 NULL BTC-USD price records | silver_market_prices | Source closing prices from Binance or Coinbase for 2026-03-19, 2026-03-21, 2026-03-24. Document source and retrieval date for audit trail. |
| 8 duplicate NationalID records | silver_debtor_loan_collateral | Human review required. Both records may represent real individuals. Escalate to source system owner. |
| 41 debtors with inactive officer assignments | silver_debtor_loan_collateral | Portfolio reassignment required. Escalate to operations team. |
| 83 loans with payment before loan start date | silver_debtor_loan_collateral | Systemic source system error. Escalate to IT for investigation. S3 values supersede for LTV calculation. |

---

## What Comes Next

The Gold layer notebook `nb_gold_aggregation` reads from these 5 Silver
tables and:

- Joins `silver_debtor_loan_collateral` to `silver_market_prices` on
  TickerSymbol and PriceDate
- Joins to `silver_bank_balance_update` on LoanID and ReportingDate
  for authoritative CurrentOutstandingBalance
- Computes LTV ratios per debtor across all collateral positions
- Applies alert flags for high risk accounts
- Populates Gold star schema tables for the Power BI Risk Command Centre

---

*CSL-DE-001 | Confidential | Data Engineering Division | March 2026*
*Generated from notebook: nb_silver_transformation*
*Repository: csl-de-001-collateral-risk-pipeline*
```

---

Create the file at `docs/silver/Silver_Layer_Transformation_Summary.md` and commit with:
```
docs: add Silver layer transformation summary document
