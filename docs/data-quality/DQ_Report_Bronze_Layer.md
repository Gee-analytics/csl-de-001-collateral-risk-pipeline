# Data Quality Report: Bronze Layer
## Project: CSL-DE-001 | Collateral Risk Monitoring & Margin Call Automation
## Prepared by: Data Engineering Team
## Date: March 2026
## Status: Final - All 8 Bronze Tables Profiled

---

## Purpose

This report documents all data quality findings identified during systematic
profiling of the Bronze layer in the CSL-DE-001 Medallion Lakehouse. It serves
two purposes:

1. A reference document for Silver layer transformation logic. Every finding
   listed here has a corresponding Silver action that must be implemented before
   any Gold layer aggregation runs.

2. A repository artefact demonstrating data quality awareness and professional
   engineering practice for audit and portfolio review purposes.

Profiling was executed in notebook `nb_bronze_data_profiling` located at
`notebooks/profiling/` in the project repository.

---

## Severity Classification

| Severity | Definition |
|----------|------------|
| HIGH | Blocks pipeline correctness. Must be fixed in Silver before any aggregation runs. |
| MEDIUM | Degrades data quality but does not break the pipeline. Must be handled with a defined rule. |
| LOW | Informational. Documents a business rule or known pattern. No transformation required. |
| INFO | Clean. No action required. Documented for completeness. |

---

## Summary Dashboard

| Table | Rows | HIGH | MEDIUM | LOW | INFO | Overall Status |
|-------|------|------|--------|-----|------|----------------|
| bronze_client_bank | 4 | 0 | 0 | 0 | 5 | PASS |
| bronze_collections_officer | 20 | 0 | 2 | 1 | 3 | ISSUES FOUND |
| bronze_officer_client_mapping | 56 | 3 | 0 | 1 | 2 | ISSUES FOUND |
| bronze_debtor | 204 | 2 | 2 | 0 | 2 | ISSUES FOUND |
| bronze_loan | 250 | 5 | 1 | 0 | 1 | ISSUES FOUND |
| bronze_collateral | 300 | 5 | 1 | 0 | 2 | ISSUES FOUND |
| bronze_bank_balance_update | 741 | 4 | 4 | 1 | 1 | ISSUES FOUND |
| bronze_market_prices | 1,880 | 2 | 3 | 0 | 1 | ISSUES FOUND |
| **TOTAL** | **3,455** | **21** | **13** | **3** | **17** | |

---

## Section 1: bronze_client_bank

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: ClientID
**Role**: Parent reference table. All loan, debtor, and balance records must
resolve to a valid ClientID in this table.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | ALL | 4 rows, 0 nulls, 0 duplicates across all columns | INFO | None required |
| 2 | ClientID | All 4 IDs unique and match expected format CLT-XXX | INFO | None required |
| 3 | ClientName | All names populated, no blank or whitespace values | INFO | None required |
| 4 | IsActive | All 4 clients marked active | INFO | None required |
| 5 | Row count | Exactly 4 records matching expected client count | INFO | None required |

### Notes

This table is clean. No transformations required beyond standard type casting
and audit column propagation.

---

## Section 2: bronze_collections_officer

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: OfficerID
**Role**: Staff reference table. Every debt assignment must trace back to a
valid, active officer record.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | TeamLeadOfficerID | 4 nulls (20%). Officers OFF-0001, OFF-0002, OFF-0003, OFF-0004 have no supervisor. | LOW | Document as expected business behaviour. These are team leads with no supervisor above them. No fix required. |
| 2 | DateJoined | 2 nulls (10%) on OFF-0009 and OFF-0010, both Active status. | MEDIUM | Flag these records with a `data_quality_flag = 'MISSING_JOIN_DATE'` column in Silver. Do not drop. Escalate to HR or source system owner for correction. |
| 3 | Status | Unexpected value `Suspended` on 2 officers (OFF-0019, OFF-0020). Expected domain was Active and Inactive only. | MEDIUM | Add `Suspended` to the valid status domain. Update the Business Rules Document. Apply downstream filtering logic to treat Suspended the same as Inactive for assignment eligibility. |
| 4 | OfficerID | All 20 IDs unique. No duplicates. | INFO | None required |
| 5 | TeamLeadOfficerID | All non-null TeamLeadOfficerIDs resolve to a valid OfficerID in the same table. | INFO | None required |
| 6 | Email | OFF-0008 (Yemi Ibrahim) has email `Emeka.Obi@CollectionSolutionsLtd.com`. Email belongs to a different person based on naming evidence. | MEDIUM | Flag with `data_quality_flag = 'SUSPECT_EMAIL'`. Do not overwrite. Escalate to HR for verification. PII masking still applies at Gold transition. |

### Notes

The `Suspended` status finding has downstream implications. Sections 3 and 4
both surface consequences of suspended officers still having active assignments
and debtor records. This is a cross-table logical contradiction rooted here.

Phone number format inconsistency also observed in preview data. OFF-0005 has
`08012345678` and OFF-0006 has `234-801-234-5678` versus the standard
`+234XXXXXXXXXX` format used by other officers. Silver action: standardise
PhoneNumber to E.164 format `+234XXXXXXXXXX` using pattern-based cleaning.

---

## Section 3: bronze_officer_client_mapping

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: Composite (OfficerID + ClientID)
**Role**: Junction table linking officers to client portfolios. Drives RLS
logic in Power BI.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | OfficerID + ClientID | 3 duplicate composite key combinations. OFF-0001 is mapped twice each to CLT-001, CLT-002, and CLT-003. | HIGH | Deduplicate on OfficerID + ClientID keeping the record with the latest AssignmentStartDate. Log removed duplicates to the pipeline metadata table. |
| 2 | OfficerID + Status | 9 mapping records belong to Inactive or Suspended officers (OFF-0017, OFF-0018, OFF-0019, OFF-0020) who still have active client portfolio assignments. | HIGH | In Silver, join to bronze_collections_officer on OfficerID and add a derived column `officer_status_at_load`. Flag rows where officer is non-active with `data_quality_flag = 'INACTIVE_OFFICER_MAPPING'`. Do not delete. These portfolios require reassignment by the business. |
| 3 | AssignmentEndDate + IsActive | 4 rows (MAP-0005 through MAP-0008, all OFF-0002) have AssignmentEndDate populated with a past date but IsActive = true. Logical contradiction within the same row. | HIGH | In Silver, apply rule: where AssignmentEndDate is not null and AssignmentEndDate < current_date, set IsActive = false. Log overrides to audit column. |
| 4 | AssignmentEndDate | 52 nulls (92.9%). | LOW | Expected business behaviour. A null AssignmentEndDate means the assignment is currently open with no defined end date. Document as expected. No fix required. |
| 5 | MappingID | All 56 IDs unique. No duplicates. | INFO | None required |
| 6 | Referential integrity | All OfficerIDs and ClientIDs resolve to valid parent records. | INFO | None required |

---

## Section 4: bronze_debtor

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: DebtorID
**Role**: Core entity table. Every loan, collateral position, and collection
action resolves to a debtor record. Contains PII fields subject to masking.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | NationalID | 4 duplicate NationalIDs across 8 rows. IDs 19252910991, 26632601180, 56607977055, 91758564086 each appear on 2 different DebtorIDs. | HIGH | Flag all 8 affected rows with `data_quality_flag = 'DUPLICATE_NATIONAL_ID'`. Do not deduplicate automatically. Escalate to source system owner. Both records may represent real individuals with a data entry error. Human review required. |
| 2 | AssignedOfficerID | 41 debtors (20% of portfolio) are assigned to Inactive or Suspended officers. | HIGH | In Silver, join to bronze_collections_officer and add `officer_status_at_load` derived column. Flag affected rows with `data_quality_flag = 'INACTIVE_OFFICER_ASSIGNMENT'`. These debtors have no active officer managing their recovery. Operational risk. |
| 3 | EmailAddress | 3 nulls (1.5%). | MEDIUM | Flag with `data_quality_flag = 'MISSING_EMAIL'`. Retain record. Email is a contact field, not an identifier. Missing email degrades outreach capability but does not invalidate the debtor record. |
| 4 | NationalID | 10 NationalIDs are 10 digits instead of 11. Nigerian NIN is 11 digits. Leading zero stripped during source system export. | MEDIUM | In Silver, apply: where length(NationalID) = 10, left-pad with a single leading zero using `F.lpad('NationalID', 11, '0')`. |
| 5 | DebtorID | All 204 IDs unique. | INFO | None required |
| 6 | Referential integrity | All ClientIDs and AssignedOfficerIDs resolve to valid parent records. | INFO | None required |

### PII Fields Identified

The following fields in this table are subject to masking at the Silver-to-Gold
transition per the project security design:

- `FullName`
- `PhoneNumber`
- `EmailAddress`
- `ResidentialAddress`
- `NationalID`

Masking strategy: semantic layer security via Row Level Security and Object
Level Security at the Power BI semantic model level. Static hashing was
evaluated and rejected due to irreversibility and operational usability concerns.

---

## Section 5: bronze_loan

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: LoanID
**Role**: Core financial table. OutstandingBalance is the numerator in the
LTV ratio formula. NOTE: OutstandingBalance in this table is superseded by
CurrentOutstandingBalance from bronze_bank_balance_update per the formal
business rule. SQL Server is the fallback only.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | LastPaymentDate + LastPaymentAmount | 83 loans (33% of portfolio) have LastPaymentDate before LoanStartDate. Payment recorded before the loan was issued. Systemic source system failure. | HIGH | Flag all 83 rows with `data_quality_flag = 'PAYMENT_BEFORE_LOAN_START'`. Since S3 is the system of record for payment data, the SQL Server LastPaymentDate is superseded at Silver anyway. However these records must be flagged for audit purposes. Do not use SQL Server LastPaymentDate for any calculation on these rows. |
| 2 | LoanStatus + OutstandingBalance | 7 loans with LoanStatus = Settled but OutstandingBalance > 0. A settled loan must have zero balance. | HIGH | Flag with `data_quality_flag = 'SETTLED_WITH_BALANCE'`. Exclude from LTV calculation. Escalate to source system owner. Do not auto-correct the balance to zero as that assumption may be wrong. |
| 3 | LoanStatus + DaysPastDue | 6 loans with LoanStatus = Settled but DaysPastDue > 0. A settled loan cannot be past due. | HIGH | Flag with `data_quality_flag = 'SETTLED_PAST_DUE'`. 5 of these 6 loans overlap with finding 2 above. Apply both flags where applicable. |
| 4 | OutstandingBalance + InitialLoanAmount | 8 loans where OutstandingBalance exceeds InitialLoanAmount. Balance cannot exceed the original loan without capitalised penalties, which are not modelled in this schema. | HIGH | Flag with `data_quality_flag = 'BALANCE_EXCEEDS_INITIAL'`. Exclude from LTV calculation pending business clarification. Do not auto-correct. |
| 5 | LoanMaturityDate + LoanStartDate | 3 loans (LN-00025, LN-00026, LN-00027) where LoanMaturityDate is before LoanStartDate. Dates appear to have been swapped at data entry. | HIGH | Flag with `data_quality_flag = 'INVERTED_LOAN_DATES'`. Do not auto-swap dates. Escalate to source system owner for manual correction. |
| 6 | LastPaymentDate + LastPaymentAmount | 5 loans have both fields null simultaneously. No payment has ever been made on these loans. | LOW | Expected business behaviour. Null pair indicates no payment history. No fix required. Document as valid state. |
| 7 | LoanStatus | No Written-Off loans present. Expected domain included Written-Off but none exist in the current dataset. | INFO | Not an error. Portfolio currently contains Active, Defaulted, and Settled loans only. Update expected domain documentation if Written-Off loans are confirmed not present by design. |

### Notes

The 83-row LastPaymentDate finding is the single largest finding in the entire
Bronze layer profiling exercise. 33% of the loan portfolio has a payment date
that predates the loan itself. This is a systemic data entry or system migration
error in the source SQL Server. Because S3 is the system of record for payment
data, the practical impact on LTV calculation is mitigated. However the records
must be flagged and the source system owner must be notified.

---

## Section 6: bronze_collateral

**Source**: On-premises SQL Server via On-Premises Data Gateway
**Primary key**: CollateralID
**Role**: Core asset table. QuantityHeld multiplied by current market Close
price forms the LTV denominator. Data quality failures here directly corrupt
LTV calculations.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | TickerSymbol | `APPLE INC` appears as a TickerSymbol on 1 row (COL-0002). This is a company name not a valid ticker. Applying F.upper() will produce `APPLE INC` which will never join to a market price record. | HIGH | In Silver, apply a ticker correction lookup: map `APPLE INC` to `AAPL`. Flag with `data_quality_flag = 'TICKER_CORRECTED'` and log the original value. |
| 2 | TickerSymbol + AssetType + Exchange | COL-0005 has TickerSymbol `BTC/USD`, AssetType `Stock`, Exchange `NYSE`. BTC/USD is a crypto asset. The correct yfinance ticker is `BTC-USD` not `BTC/USD`. Three simultaneous errors on one row. | HIGH | In Silver, map `BTC/USD` to `BTC-USD`, set AssetType to `Crypto`, set Exchange to `CRYPTO`. Flag with `data_quality_flag = 'TICKER_AND_ASSET_TYPE_CORRECTED'`. |
| 3 | AssetType + Exchange | COL-0009 has TickerSymbol `NVDA`, AssetType `Crypto`, Exchange `CRYPTO`. NVDA is NVIDIA, a stock. | HIGH | In Silver, set AssetType to `Stock` and Exchange to `NASDAQ` for all rows where TickerSymbol = `NVDA` and AssetType = `Crypto`. Flag with `data_quality_flag = 'ASSET_TYPE_CORRECTED'`. |
| 4 | TickerSymbol | 6 rows with lowercase or mixed case tickers: `aapl`, `amzn`, `googl`, `tsla`, `Tsla`. Will not join to market price records which store tickers in uppercase. | HIGH | In Silver, apply `F.upper()` to TickerSymbol on all rows as a standard transformation. This resolves all case variants except `APPLE INC` and `BTC/USD` which require explicit mapping. |
| 5 | QuantityHeld | 4 rows with zero or negative QuantityHeld. | HIGH | Flag with `data_quality_flag = 'INVALID_QUANTITY'`. Exclude from LTV calculation. Zero or negative quantity produces meaningless or negative collateral value. |
| 6 | Exchange | 6 nulls (2%). All correspond to known tickers (AAPL, AMZN, MSFT, NVDA). Exchange is derivable from ticker. | MEDIUM | In Silver, impute Exchange using a ticker-to-exchange lookup dictionary: AAPL=NYSE, AMZN=NASDAQ, MSFT=NASDAQ, NVDA=NASDAQ, GOOGL=NASDAQ, TSLA=NASDAQ, BTC-USD=CRYPTO. |
| 7 | CollateralValueAtPledge | 5 rows with zero or negative CollateralValueAtPledge. | HIGH | Flag with `data_quality_flag = 'INVALID_PLEDGE_VALUE'`. Retain record but exclude from any historical valuation analysis. Current LTV uses live market price not pledge value, so LTV calculation is not directly impacted. |
| 8 | CollateralID | All 300 IDs unique. | INFO | None required |
| 9 | Referential integrity | All LoanIDs and DebtorIDs resolve to valid parent records. Cross-table DebtorID consistency check passed. | INFO | None required |

### Critical Note for Silver Transformation

The TickerSymbol field is the join key between `bronze_collateral` and
`bronze_market_prices`. Any TickerSymbol that does not exactly match a value
in the market prices table will produce a silent null join. The LTV for that
debtor will be calculated without that asset, understating collateral value and
overstating risk. The ticker corrections in findings 1 through 4 above are
mandatory before any LTV computation runs.

---

## Section 7: bronze_bank_balance_update

**Source**: Amazon S3 via Fabric Shortcut (client bank daily drops)
**Primary key**: Composite (LoanID + ReportingDate)
**Role**: Authoritative source for CurrentOutstandingBalance, LastPaymentDate,
and LastPaymentAmount. Overrides SQL Server loan table values at Silver layer
per formal business rule.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | LoanID + ReportingDate | 24 duplicate composite key combinations across reporting dates 2026-03-17, 2026-03-19, and 2026-03-20. Full row duplicates, meaning the same file was dropped twice by the client bank. | HIGH | In Silver, deduplicate on LoanID + ReportingDate keeping one row. Since rows are fully identical, any row can be retained. Log deduplication count to pipeline metadata table per run. |
| 2 | CurrentOutstandingBalance | 12 nulls (1.6%). This is the authoritative balance field per the S3 system-of-record business rule. A null here means no authoritative balance exists for that loan on that date. | HIGH | In Silver, flag with `data_quality_flag = 'BALANCE_UNAVAILABLE'`. Fall back to SQL Server OutstandingBalance for these loans only. Document the fallback in the pipeline run log. |
| 3 | AccountStatus | String literal `null` (6 rows). This is the word null as a string value, not a database NULL. Passes null checks silently. | HIGH | In Silver, apply: where AccountStatus = 'null', replace with actual NULL then flag with `data_quality_flag = 'UNKNOWN_ACCOUNT_STATUS'`. |
| 4 | AccountStatus | Value `UNKNOWN` on 3 rows. Cannot be used in risk classification. | HIGH | Flag with `data_quality_flag = 'UNKNOWN_ACCOUNT_STATUS'`. Exclude from any status-based risk segmentation. Escalate to client bank for clarification. |
| 5 | AccountStatus | Value `WRITTEN_OFF` on 2 rows. Inconsistent formatting versus expected `Written-Off`. Will not group correctly with other written-off records. | MEDIUM | In Silver, standardise: replace `WRITTEN_OFF` with `Written-Off` using a status normalisation map. |
| 6 | AccountStatus | Value `Suspended` on 1 row. Not a standard loan account status. Ambiguous meaning. | MEDIUM | Flag with `data_quality_flag = 'SUSPECT_ACCOUNT_STATUS'`. Escalate to client bank for clarification before applying any business rule. |
| 7 | ReportingDate + LastPaymentDate | Both stored as TimestampType with time component 00:00:00. SQL Server date fields are DateType. Type mismatch causes silent join failures at Silver. | MEDIUM | In Silver, cast ReportingDate and LastPaymentDate to DateType using `F.to_date()`. |
| 8 | CurrentOutstandingBalance | Stored as DoubleType. SQL Server loan table uses DecimalType(18,2). Type inconsistency introduces floating point precision risk on financial calculations. | MEDIUM | In Silver, cast CurrentOutstandingBalance and LastPaymentAmount to DecimalType(18,2) to match SQL Server schema. |
| 9 | AccountStatus | Value `Watch` on 187 rows. This is a valid banking term meaning the account is under close monitoring but not yet NPL. Was not in the original expected domain. | LOW | Add `Watch` to the valid AccountStatus domain. Update the Business Rules Document. No data fix required. |
| 10 | SourceFileName | All 741 rows match expected naming convention. | INFO | None required |

### Notes

The ReportingDate distribution confirms 3 distinct reporting dates across all 4
client banks: 2026-03-17, 2026-03-19, and 2026-03-20. Each client bank dropped
files on all 3 dates with consistent row counts per date. This is expected
behaviour from the simulation script.

---

## Section 8: bronze_market_prices

**Source**: Yahoo Finance API via Python Spark Notebook (yfinance)
**Primary key**: Composite (TickerSymbol + PriceDate)
**Role**: Provides closing market price for LTV denominator calculation.
Price history covers 2025-03-19 to 2026-03-21 (368 distinct dates).
This exceeds the 5 business day stale data threshold defined in the
project business rules.

### Findings

| # | Field | Finding | Severity | Silver Action |
|---|-------|---------|----------|---------------|
| 1 | APPLE INC | No price record exists for ticker `APPLE INC`. This is a dirty ticker from bronze_collateral with no corresponding market price. | HIGH | Silver action is on the collateral side: map `APPLE INC` to `AAPL` as documented in Section 6 Finding 1. Once corrected, the join will resolve. |
| 2 | BTC/USD | No price record exists for ticker `BTC/USD`. Wrong format, should be `BTC-USD`. | HIGH | Silver action is on the collateral side: map `BTC/USD` to `BTC-USD` as documented in Section 6 Finding 2. Once corrected, the join will resolve. |
| 3 | Close + High + Low + Open | 2 rows have all OHLC values null simultaneously. TickerSymbol and PriceDate are populated but no price data exists. | MEDIUM | In Silver, flag these rows with `data_quality_flag = 'NULL_PRICE_RECORD'`. Exclude from price lookup. Apply the 5 business day fallback rule: use the most recent non-null Close price within 5 business days. If none found, flag the collateral record as `STALE_DATA`. |
| 4 | PriceDate | Stored as TimestampType with time component 00:00:00. Other date fields across the pipeline use DateType. Type mismatch causes silent join failures. | MEDIUM | In Silver, cast PriceDate to DateType using `F.to_date()`. |
| 5 | Close + High + Low + Open | All stored as DoubleType. Financial precision risk on high-value assets such as BTC-USD where values exceed 80,000. | MEDIUM | In Silver, cast all OHLC columns to DecimalType(18,6) to preserve precision across the LTV calculation chain. |
| 6 | TickerSymbol + PriceDate | All 1,880 combinations unique. No duplicate price records. | INFO | None required |

### Notes

The price history of 368 distinct dates covering one full year is sufficient to
satisfy the stale data edge case rule: if no current market price exists for a
ticker, use the most recent price available within the last 5 business days.
The BTC-USD ticker has more records than equity tickers because cryptocurrency
trades on weekends and holidays, producing additional price dates.

---

## Cross-Table Issues Summary

These findings span multiple tables and represent systemic issues rather than
isolated table-level defects.

| Issue | Tables Affected | Severity | Silver Action |
|-------|----------------|----------|---------------|
| Inactive and Suspended officers with active assignments and debtor records | bronze_collections_officer, bronze_officer_client_mapping, bronze_debtor | HIGH | Apply officer status join at Silver. Flag all downstream records where officer is non-active. |
| TickerSymbol dirty values causing silent LTV join failures | bronze_collateral, bronze_market_prices | HIGH | All ticker corrections must be applied to bronze_collateral before the market price join executes. |
| Timestamp vs Date type inconsistency across sources | bronze_bank_balance_update, bronze_market_prices | MEDIUM | Standardise all date fields to DateType in Silver as a mandatory first transformation step. |
| Double vs Decimal type inconsistency for financial amounts | bronze_bank_balance_update, bronze_market_prices | MEDIUM | Cast all financial columns to DecimalType(18,2) or DecimalType(18,6) as appropriate in Silver. |

---

## Silver Transformation Priority Order

Based on findings across all 8 tables, Silver transformations must be applied
in this order to avoid cascading errors:

1. Cast all date and timestamp fields to DateType
2. Cast all financial amount fields to DecimalType
3. Deduplicate bronze_bank_balance_update on LoanID + ReportingDate
4. Apply TickerSymbol corrections on bronze_collateral before any join
5. Apply F.upper() to all TickerSymbol values
6. Resolve string literal `null` in AccountStatus fields
7. Standardise AccountStatus values across sources
8. Apply all data_quality_flag columns
9. Join officer status and apply cross-table contradiction flags
10. Execute market price join only after ticker corrections are confirmed

---

*CSL-DE-001 | Confidential | Data Engineering Division | March 2026*
*Generated from profiling notebook: nb_bronze_data_profiling*
*Repository: csl-de-001-collateral-risk-pipeline*
