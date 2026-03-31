# Data Dictionary - Gold Layer
## CSL-DE-001 | Collateral Risk Monitoring and Margin Call Automation System

| Field | Detail |
|---|---|
| Document Code | CSL-DE-001-DD-GOLD |
| Version | v1.0 |
| Status | Complete |
| Prepared By | Data Engineering Team |
| Date | March 2026 |
| Layer | Gold |
| Platform | Microsoft Fabric |
| Notebook | `nb_gold_aggregation` |

---

## 1. Purpose

This document defines every table and column in the Gold layer of the CSL-DE-001 pipeline. The Gold layer produces a production-ready star schema consumed by the Power BI Risk Command Centre dashboard via Direct Lake mode. All business logic referenced here traces to `CSL_DE_001_Business_Rules_v1.0.md`.

---

## 2. Table Inventory

| Table | Type | Grain | Row Count | Write Strategy |
|---|---|---|---|---|
| `fact_ltv_daily_snapshot` | Fact | One row per debtor per collateral asset per date | 249 per daily run | Append with idempotency guard |
| `fact_market_prices_daily` | Fact | One row per ticker per date | 1,888 on first run | Watermark append |
| `dim_debtor` | Dimension | One current row per debtor | 204 | Delta merge SCD Type 2 |
| `dim_loan` | Dimension | One current row per loan | 250 | Delta merge SCD Type 2 |
| `dim_collateral_asset` | Dimension | One current row per collateral position | 300 | Delta merge SCD Type 2 |
| `dim_collections_officer` | Dimension | One current row per officer | 53 | Delta merge SCD Type 2 |
| `dim_client_bank` | Dimension | One row per client bank | 4 | Full overwrite |
| `dim_date` | Dimension | One row per calendar date | 4,748 | Write once |
| `gold_audit_log` | Audit | One row per pipeline step per run | 8 per run | Append with idempotency guard |

---

## 3. fact_ltv_daily_snapshot

**Purpose:** The central fact table of the star schema. Records the daily LTV risk position for every debtor across every collateral asset. Each daily pipeline run appends a new snapshot preserving full history for trend analysis.

**Grain:** One row per debtor per collateral asset per pipeline run date.

**Business Rules Referenced:** BRD Sections 2, 3, 4, 5, 6, 7, 8, 10.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `SnapshotID` | STRING | No | Surrogate primary key. SHA-256 hash of DebtorID, CollateralID, and PipelineRunDate concatenated. Deterministic and unique per row. | `a3f5c9d2...` |
| `DebtorID` | STRING | No | Foreign key to `dim_debtor`. Natural key from the on-premises SQL Server debtor table. | `DBT-0042` |
| `LoanID` | STRING | No | Foreign key to `dim_loan`. Natural key from the on-premises SQL Server loan table. | `LN-00087` |
| `CollateralID` | STRING | No | Foreign key to `dim_collateral_asset`. Natural key from the on-premises SQL Server collateral table. | `COL-0123` |
| `AssignedOfficerID` | STRING | No | Foreign key to `dim_collections_officer`. The OfficerID responsible for this debtor account. Used for RLS filtering in Power BI. | `OFF-0007` |
| `ClientID` | STRING | No | Foreign key to `dim_client_bank`. The client bank that referred this debtor to CSL. | `CLT-001` |
| `DateKey` | DATE | No | Foreign key to `dim_date`. The pipeline run date for this snapshot. | `2026-03-26` |
| `PipelineRunDate` | DATE | No | The calendar date on which this pipeline run executed. Used for idempotency guard. Duplicate of DateKey retained for explicit idempotency delete logic. | `2026-03-26` |
| `TickerSymbol` | STRING | No | The ticker symbol of the collateral asset for this row. Standardised to uppercase with no trailing spaces. | `AAPL` |
| `AssetType` | STRING | No | The asset class of the collateral. Derived from TickerSymbol. | `Stock` or `Crypto` |
| `Exchange` | STRING | No | The exchange on which the asset trades. Corrected from source during Silver transformation. | `NYSE` or `CRYPTO` |
| `QuantityHeld` | DECIMAL(18,4) | No | Number of units of the collateral asset held at the time of pledge. Sourced from the on-premises collateral table. Does not update to reflect subsequent asset sales unless a new collateral record is created. | `250.0000` |
| `CurrentMarketPrice` | DECIMAL(18,4) | Yes | The closing price of the ticker on the pipeline run date. NULL where DataFreshnessStatus is MISSING. Sourced from `silver_market_prices` via yfinance API. | `187.4200` |
| `CurrentCollateralValue` | DECIMAL(18,2) | Yes | QuantityHeld multiplied by CurrentMarketPrice. The current market value of this specific collateral position. NULL where CurrentMarketPrice is NULL. | `46855000.00` |
| `CollateralValueAtPledge` | DECIMAL(18,2) | No | The market value of the collateral at the time it was pledged against the loan. Used for comparison against CurrentCollateralValue to measure deterioration. | `55000000.00` |
| `CollateralValueChange` | DECIMAL(18,2) | Yes | CurrentCollateralValue minus CollateralValueAtPledge. Negative values indicate collateral has lost value since pledge. | `-8145000.00` |
| `ResolvedOutstandingBalance` | DECIMAL(18,2) | Yes | The authoritative outstanding balance for this specific loan resolved per the Balance Authority Rule. S3 bank balance where available, on-premises balance as fallback. | `32500000.00` |
| `BalanceSource` | STRING | No | Records which source provided the authoritative balance for audit purposes. | `S3_UPDATE` or `ONPREM_ORIGINAL` |
| `TotalDebtorOutstandingBalance` | DECIMAL(18,2) | Yes | Sum of ResolvedOutstandingBalance across all active and defaulted loans for this DebtorID on this date. This is the numerator in the LTV calculation. Settled loans are excluded per BRD Section 10. | `65000000.00` |
| `TotalDebtorCollateralValue` | DECIMAL(18,2) | Yes | Sum of CurrentCollateralValue across all eligible collateral positions for this DebtorID on this date. This is the denominator in the LTV calculation. | `72000000.00` |
| `LTV_Ratio` | DECIMAL(10,6) | Yes | TotalDebtorOutstandingBalance divided by TotalDebtorCollateralValue. Expressed as a decimal. NULL where TotalDebtorCollateralValue is zero or NULL. | `0.902778` |
| `LTV_Percentage` | DECIMAL(10,4) | Yes | LTV_Ratio multiplied by 100. Expressed as a percentage for display in Power BI. NULL where LTV_Ratio is NULL. | `90.2778` |
| `LTV_Calculation_Status` | STRING | No | Status of the LTV calculation for this record. | `CALCULATED`, `NO_ELIGIBLE_COLLATERAL`, or `DIVISION_BY_ZERO` |
| `RiskFlag` | STRING | No | Four-tier risk classification per BRD Section 4. CRITICAL where LTV >= 1.00. HIGH where 0.80 <= LTV < 1.00. MEDIUM where 0.60 <= LTV < 0.80. LOW where LTV < 0.60. UNDETERMINED where LTV could not be calculated. | `CRITICAL` |
| `ImmediateActionRequired` | BOOLEAN | No | TRUE for HIGH and CRITICAL accounts. These accounts appear in the High Risk Accounts dashboard view and are prioritised for collection action. | `TRUE` |
| `RequiredCollateralValue` | DECIMAL(18,2) | Yes | TotalDebtorOutstandingBalance divided by 0.80. The collateral value required to bring the account to the boundary of HIGH risk status. NULL where LTV is below 0.80. | `81250000.00` |
| `CollateralCoverageShortfall` | DECIMAL(18,2) | Yes | RequiredCollateralValue minus TotalDebtorCollateralValue. The NGN amount by which collateral must increase to return the account to safe status. NULL where result is zero or negative. Provides collection officers with an actionable figure. | `9250000.00` |
| `DataFreshnessStatus` | STRING | No | Indicates the age of the market price used for this record. CURRENT means today's price was available. STALE means a fallback price within the last 5 business days was used. MISSING means no price was available within 5 business days and the record is excluded from LTV. | `CURRENT` |
| `PriceDateUsed` | DATE | Yes | The actual date of the ClosePrice used in the LTV calculation. Equal to PipelineRunDate for CURRENT records. Earlier than PipelineRunDate for STALE records. NULL for MISSING records. | `2026-03-26` |
| `EligibleForLTV` | BOOLEAN | No | FALSE where the collateral position was excluded from LTV calculation due to data quality issues flagged at Silver layer or missing price data. TRUE records are included in TotalDebtorCollateralValue aggregation. | `TRUE` |
| `DaysPastDue` | INTEGER | Yes | The number of days the associated loan is past its due date. Sourced from the most recent S3 bank balance update where available. | `365` |
| `LoanStatus` | STRING | No | The current status of the associated loan. Active, Defaulted, or Settled. Only Active and Defaulted loans are included in LTV calculation per BRD Section 10. | `Defaulted` |
| `PipelineRunID` | STRING | No | The unique run identifier linking this row to the corresponding entry in gold_audit_log. UUID generated at notebook start. | `d485fb22-cb1c-4d94-bcba-978fb4fbeb73` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | The datetime at which this row was written to the Gold layer Delta table. | `2026-03-26 23:47:12` |

---

## 4. fact_market_prices_daily

**Purpose:** Records the full daily price history for all tracked ticker symbols. Enables the collateral value trend chart in Power BI. Shares conformed dimensions `dim_date` and `dim_collateral_asset` with `fact_ltv_daily_snapshot`, allowing cross-filtering between LTV metrics and price trend analysis.

**Grain:** One row per ticker symbol per date.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `PriceID` | STRING | No | Surrogate primary key. SHA-256 hash of TickerSymbol and PriceDate concatenated. | `b7e2a1f4...` |
| `TickerSymbol` | STRING | No | Foreign key to `dim_collateral_asset`. Standardised uppercase ticker. | `TSLA` |
| `DateKey` | DATE | No | Foreign key to `dim_date`. The date of this price record. | `2026-03-26` |
| `PriceDate` | DATE | No | The calendar date of the closing price. | `2026-03-26` |
| `OpenPrice` | DECIMAL(18,4) | Yes | The opening price of the ticker on this date. | `172.3300` |
| `HighPrice` | DECIMAL(18,4) | Yes | The intraday high price on this date. | `179.8800` |
| `LowPrice` | DECIMAL(18,4) | Yes | The intraday low price on this date. | `170.1200` |
| `ClosePrice` | DECIMAL(18,4) | Yes | The end-of-day closing price. This is the value used in LTV calculation. | `176.4500` |
| `AdjustedClose` | DECIMAL(18,4) | Yes | The closing price adjusted for stock splits and dividends. | `176.4500` |
| `Volume` | LONG | Yes | The number of shares traded on this date. | `82450000` |
| `AssetType` | STRING | No | Stock or Crypto. Derived from TickerSymbol. | `Stock` |
| `PipelineRunID` | STRING | No | Links to gold_audit_log for the run that loaded this record. | `d485fb22-cb1c-4d94-bcba-978fb4fbeb73` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 5. dim_debtor

**Purpose:** Descriptive context about each debtor. SCD Type 2 is implemented to track attribute changes over time. Contact PII fields are passed through in readable form and protected at the Power BI semantic model level via RLS and OLS. NationalID is already SHA-256 hashed at the Silver layer.

**Write Strategy:** Delta merge on DebtorSurrogateKey. IsCurrent flag distinguishes active from historical records.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `DebtorSurrogateKey` | STRING | No | Primary key. SHA-256 hash of DebtorID concatenated with EffectiveStartDate. Deterministic across pipeline runs. New key generated on each SCD Type 2 change. | `c9a2e4f1...` |
| `DebtorID` | STRING | No | Natural key from the on-premises SQL Server debtor table. | `DBT-0042` |
| `ClientID` | STRING | No | The client bank that referred this debtor to CSL. Foreign key to `dim_client_bank`. | `CLT-002` |
| `FullName` | STRING | No | The debtor's full name. Not masked. Access controlled via RLS at the semantic model level. | `Chukwuemeka Okafor` |
| `NationalID` | STRING | No | SHA-256 hash of the debtor's BVN. Hashed at Silver layer. Original value is not recoverable from this layer. | `a8f3c2b1...` |
| `PhoneNumber` | STRING | Yes | The debtor's contact phone number in +234XXXXXXXXXX format. Access controlled via OLS. Visible to CollectionsOfficer and Admin roles only. | `+2348012345678` |
| `EmailAddress` | STRING | Yes | The debtor's email address. Access controlled via OLS. Visible to CollectionsOfficer and Admin roles only. | `c.okafor@email.com` |
| `ResidentialAddress` | STRING | Yes | The debtor's residential address. Access controlled via OLS. Visible to CollectionsOfficer and Admin roles only. | `24 Adeola Odeku Street, Victoria Island, Lagos` |
| `Region` | STRING | No | The geographic region of the debtor. Used for RLS filtering. | `Lagos` |
| `AssignedOfficerID` | STRING | No | The OfficerID of the collections officer assigned to this debtor. Foreign key to `dim_collections_officer`. | `OFF-0007` |
| `DateOnboarded` | DATE | No | The date this debtor was onboarded into CSL's system. | `2021-04-15` |
| `EffectiveStartDate` | DATE | No | SCD Type 2 start date. The date from which this version of the record is valid. | `2021-04-15` |
| `EffectiveEndDate` | DATE | Yes | SCD Type 2 end date. NULL for the current active record. Populated when a new version of the record is created. | `NULL` |
| `IsCurrent` | BOOLEAN | No | TRUE for the active version of the record. FALSE for historical versions. Power BI filters on IsCurrent = TRUE for live dashboard views. | `TRUE` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 6. dim_loan

**Purpose:** Descriptive context about each loan. SCD Type 2 tracks LoanStatus transitions over time, which are significant business events in a debt recovery context. A loan transitioning from Active to Defaulted is an event that must be historically traceable.

**Write Strategy:** Delta merge on LoanSurrogateKey.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `LoanSurrogateKey` | STRING | No | Primary key. SHA-256 hash of LoanID concatenated with EffectiveStartDate. | `f2b8d7a3...` |
| `LoanID` | STRING | No | Natural key from the on-premises SQL Server loan table. | `LN-00087` |
| `DebtorID` | STRING | No | Natural key of the debtor associated with this loan. Foreign key reference to `dim_debtor`. | `DBT-0042` |
| `ClientID` | STRING | No | The client bank that issued this loan. Foreign key to `dim_client_bank`. | `CLT-001` |
| `InitialLoanAmount` | DECIMAL(18,2) | No | The original loan amount in Nigerian Naira at the time of issuance. | `15000000.00` |
| `LoanStartDate` | DATE | No | The date the loan was issued. | `2021-04-15` |
| `LoanMaturityDate` | DATE | No | The date the loan is scheduled to mature. Records where LoanMaturityDate is before LoanStartDate are flagged at Silver layer and excluded from LTV calculation. | `2024-04-15` |
| `LoanStatus` | STRING | No | The current status of the loan. Active, Defaulted, or Settled. This is the SCD Type 2 tracked column. A new row is inserted when LoanStatus changes. | `Defaulted` |
| `DaysPastDueAtRecord` | INTEGER | Yes | The DaysPastDue value at the time this SCD Type 2 record was created. Snapshot of the DPD at the moment of status change. | `180` |
| `LastPaymentDate` | DATE | Yes | The date of the most recent payment received on this loan. NULL where no payment has been made. | `2022-11-30` |
| `LastPaymentAmount` | DECIMAL(18,2) | Yes | The amount of the most recent payment in Nigerian Naira. NULL where no payment has been made. | `500000.00` |
| `EffectiveStartDate` | DATE | No | SCD Type 2 start date. | `2021-04-15` |
| `EffectiveEndDate` | DATE | Yes | SCD Type 2 end date. NULL for current record. | `NULL` |
| `IsCurrent` | BOOLEAN | No | TRUE for the active version of the loan record. | `TRUE` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 7. dim_collateral_asset

**Purpose:** Descriptive context about each collateral asset pledged against a loan. Records the terms of the pledge at the time it was made. QuantityHeld reflects the quantity at pledge date and does not update unless a new collateral record is created at source.

**Write Strategy:** Delta merge on CollateralSurrogateKey. SCD Type 2 applied in case pledge terms change.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `CollateralSurrogateKey` | STRING | No | Primary key. SHA-256 hash of CollateralID concatenated with EffectiveStartDate. | `e1d4c8b2...` |
| `CollateralID` | STRING | No | Natural key from the on-premises SQL Server collateral table. | `COL-0123` |
| `LoanID` | STRING | No | Natural key of the loan this collateral is pledged against. | `LN-00087` |
| `DebtorID` | STRING | No | Natural key of the debtor who pledged this collateral. | `DBT-0042` |
| `TickerSymbol` | STRING | No | The standardised ticker symbol of the asset. Corrected from source during Silver transformation. Uppercase, no trailing spaces, BTC/USD corrected to BTC-USD. | `AAPL` |
| `AssetType` | STRING | No | The asset class. Stock for NYSE equities. Crypto for BTC-USD. | `Stock` |
| `Exchange` | STRING | No | The exchange on which the asset trades. Corrected from source during Silver transformation. NASDAQ misclassification resolved. | `NYSE` |
| `QuantityHeld` | DECIMAL(18,4) | No | Number of units of the asset held at the time of pledge. Negative values corrected to absolute value at Silver layer and flagged. | `250.0000` |
| `CollateralValueAtPledge` | DECIMAL(18,2) | No | The market value of the collateral position at the time of pledge in Nigerian Naira. Records with zero value are flagged as INVALID_PLEDGE_VALUE and excluded from LTV. | `55000000.00` |
| `CollateralPledgeDate` | DATE | No | The date the collateral was formally pledged against the loan. | `2021-04-20` |
| `EffectiveStartDate` | DATE | No | SCD Type 2 start date. | `2021-04-20` |
| `EffectiveEndDate` | DATE | Yes | SCD Type 2 end date. NULL for current record. | `NULL` |
| `IsCurrent` | BOOLEAN | No | TRUE for the active version of the collateral record. | `TRUE` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 8. dim_collections_officer

**Purpose:** Descriptive context about each CSL collections officer. SCD Type 2 tracks Status changes and regional reassignments. Used as the anchor table for Row Level Security in Power BI via the OfficerEmail column matched to USERPRINCIPALNAME().

**Write Strategy:** Delta merge on OfficerSurrogateKey.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `OfficerSurrogateKey` | STRING | No | Primary key. SHA-256 hash of OfficerID concatenated with EffectiveStartDate. | `d7f2b1e8...` |
| `OfficerID` | STRING | No | Natural key from the on-premises SQL Server collections_officer table. | `OFF-0007` |
| `FullName` | STRING | No | The officer's full name. Internal staff data, not subject to PII masking. | `Emeka Okonkwo` |
| `Email` | STRING | No | The officer's work email address. Used as the RLS filter column matched to USERPRINCIPALNAME() in Power BI. | `emeka.okonkwo@collectionsolutionsltd.com` |
| `Region` | STRING | No | The geographic region the officer is currently assigned to cover. | `Lagos` |
| `Status` | STRING | No | The officer's employment status. Active, Inactive, or Suspended. This is the SCD Type 2 tracked column. | `Active` |
| `TeamLeadOfficerID` | STRING | Yes | The OfficerID of this officer's team lead. NULL for team leads themselves. | `OFF-0001` |
| `DateJoined` | DATE | Yes | The date the officer joined CSL. NULL for legacy records migrated without a join date, replaced with sentinel value 1900-01-01 at Silver layer. | `2020-06-01` |
| `EffectiveStartDate` | DATE | No | SCD Type 2 start date. | `2020-06-01` |
| `EffectiveEndDate` | DATE | Yes | SCD Type 2 end date. NULL for current record. | `NULL` |
| `IsCurrent` | BOOLEAN | No | TRUE for the active version of the officer record. | `TRUE` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 9. dim_client_bank

**Purpose:** Reference table for the four client banks whose debtor portfolios CSL manages. A simple lookup dimension with no SCD Type 2 required given the small size and infrequent changes. Full overwrite on each pipeline run.

**Write Strategy:** Full overwrite.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `ClientSurrogateKey` | STRING | No | Primary key. SHA-256 hash of ClientID. | `a1b2c3d4...` |
| `ClientID` | STRING | No | Natural key matching ClientID across all pipeline tables. | `CLT-001` |
| `ClientName` | STRING | No | The full name of the client bank. | `Premier Bank` |
| `ContactEmail` | STRING | No | The designated contact email for data drop coordination. | `data.drops@premierbank.ng` |
| `ContactPhone` | STRING | No | The designated contact phone number. | `+2341234567890` |
| `S3FolderPath` | STRING | Yes | The S3 folder prefix where this client drops their nightly balance update files. Populated after S3 bucket provisioning. | `client-drops/CLT-001/` |
| `OnboardedDate` | DATE | No | The date CSL onboarded this client bank. | `2020-01-15` |
| `IsActive` | BOOLEAN | No | TRUE for active client relationships. | `TRUE` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this row was written to Gold. | `2026-03-26 23:47:12` |

---

## 10. dim_date

**Purpose:** Standard calendar dimension enabling all time intelligence calculations in Power BI. Generated programmatically covering 2018-01-01 to 2030-12-31. Written once with an idempotency check that skips the write entirely if the table is already populated.

**Write Strategy:** Write once with catalog check. Never modified after initial population.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `DateKey` | DATE | No | Primary key. The calendar date. | `2026-03-26` |
| `FullDate` | DATE | No | Duplicate of DateKey retained for explicit date display formatting in Power BI. | `2026-03-26` |
| `DayOfWeek` | STRING | No | The name of the day. | `Thursday` |
| `DayOfWeekNumber` | INTEGER | No | Numeric day of week. 1 = Monday, 7 = Sunday. | `4` |
| `DayOfMonth` | INTEGER | No | The day number within the month. 1 to 31. | `26` |
| `WeekOfYear` | INTEGER | No | The ISO week number within the year. 1 to 53. | `12` |
| `MonthNumber` | INTEGER | No | The numeric month. 1 to 12. | `3` |
| `MonthName` | STRING | No | The full name of the month. | `March` |
| `Quarter` | INTEGER | No | The fiscal quarter. 1 to 4. | `1` |
| `QuarterName` | STRING | No | The quarter label. | `Q1` |
| `Year` | INTEGER | No | The four-digit calendar year. | `2026` |
| `IsWeekend` | BOOLEAN | No | TRUE for Saturday and Sunday. | `FALSE` |
| `IsBusinessDay` | BOOLEAN | No | TRUE for Monday through Friday. FALSE for weekends. | `TRUE` |
| `IsMarketDay` | BOOLEAN | No | TRUE for weekdays. Current implementation treats all weekdays as market days. Public holidays are not yet modelled. A licensed holiday calendar is required for production accuracy. Known limitation documented in Gold Layer Summary Section 7. | `TRUE` |

---

## 11. gold_audit_log

**Purpose:** Records one row per pipeline step per notebook run. Provides a complete forensic audit trail of every execution of `nb_gold_aggregation`. Separated from `gold_pipeline_metadata` which is the Bronze watermark store owned by the Fabric Data Pipeline. Each table has a single clear responsibility.

**Write Strategy:** Append with idempotency guard. Delete on RunID before append ensures a rerun on the same day replaces rather than duplicates audit rows.

| Column | Data Type | Nullable | Description | Example Value |
|---|---|---|---|---|
| `AuditID` | STRING | No | Primary key. SHA-256 hash of RunID and StepName concatenated. | `f8e3d2c1...` |
| `RunID` | STRING | No | UUID generated at notebook start. Ties all audit rows from a single execution together. Links to fact_ltv_daily_snapshot via PipelineRunID. | `d485fb22-cb1c-4d94-bcba-978fb4fbeb73` |
| `StepName` | STRING | No | The name of the pipeline step that generated this audit row. | `dim_debtor` |
| `StepDescription` | STRING | No | A brief description of what the step does. | `SCD Type 2 merge for debtor dimension` |
| `RunTimestamp` | TIMESTAMP | No | The datetime at which this step completed. | `2026-03-26 23:47:12` |
| `RunStatus` | STRING | No | The outcome of this step. SUCCESS or FAILED. | `SUCCESS` |
| `RowsRead` | INTEGER | Yes | Number of rows read from the Silver input table for this step. | `300` |
| `RowsWritten` | INTEGER | Yes | Number of rows written to the Gold output table for this step. | `204` |
| `RowsRejected` | INTEGER | Yes | Number of rows excluded from the output due to data quality or validation failures. | `0` |
| `Notes` | STRING | Yes | Free text field for additional context. Used to record deduplication counts, gap findings, or remediation notes. | `Deduplicated from 300 collateral rows to 204 distinct debtors` |
| `gold_ingestion_timestamp` | TIMESTAMP | No | Datetime this audit row was written. | `2026-03-26 23:47:12` |

---

## 12. Known Gaps

| Gap | Affected Table | Severity | Remediation |
|---|---|---|---|
| BTC-USD weekend price gaps from yfinance | `fact_ltv_daily_snapshot`, `fact_market_prices_daily` | MEDIUM | Source weekend prices from Coinbase or Binance API |
| `Close` column should be renamed `ClosePrice` in `silver_market_prices` | `fact_market_prices_daily` | LOW | Update Silver transformation notebook |
| Public holidays not modelled in `IsMarketDay` | `dim_date` | LOW | Licensed holiday calendar required for production |
| OLS not yet implemented on PII columns in `dim_debtor` | `dim_debtor` | MEDIUM | Implement via Tabular Editor when XMLA endpoint access is available |
| `fact_market_prices_daily` not included in referential integrity validation | `fact_market_prices_daily` | LOW | Add validation in next iteration of nb_gold_aggregation |

---

## 13. Document Version History

| Version | Date | Author | Change Summary |
|---|---|---|---|
| v1.0 | March 2026 | Data Engineering Team | Initial version. Gold layer complete and validated. All 9 tables documented. |

---

*CSL-DE-001 | Confidential | Data Engineering Division | March 2026*
