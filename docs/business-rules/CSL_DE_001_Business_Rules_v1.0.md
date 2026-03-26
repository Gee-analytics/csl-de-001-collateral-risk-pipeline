# CSL-DE-001 Business Rules Document

**Collection Solutions Limited**
Data Engineering Division

| Field | Detail |
|---|---|
| Document Code | CSL-DE-001-BRD |
| Version | v1.0 |
| Status | Approved |
| Prepared By | Data Engineering Team |
| Reviewed By | Head of Risk & Collections |
| Date | March 2026 |

---

## 1. Purpose

This document defines the business rules governing the Collateral Risk Monitoring and Margin Call Automation pipeline (CSL-DE-001). It serves as the authoritative reference for all logic implemented in the Gold aggregation notebook and is the contractual agreement between the Business/Risk team and the Data Engineering team on how LTV calculations, risk flags, and alert criteria are computed.

Any change to the rules in this document requires formal sign-off from the Head of Risk and Collections before implementation in the pipeline.

---

## 2. LTV Formula

### 2.1 Total Collateral Value

```
Total Collateral Value = SUM(QuantityHeld x CurrentMarketPrice)
```

Summed across all **eligible** collateral positions for a given debtor on a given date.

- `QuantityHeld` is the number of units of the asset held at the time of pledge, sourced from the collateral table.
- `CurrentMarketPrice` is the end-of-day closing price for the ticker on the pipeline run date, sourced from the yfinance API via the Silver market prices table.

### 2.2 Total Outstanding Balance

```
Total Outstanding Balance = SUM(CurrentOutstandingBalance)
```

Summed across all active loans for a given debtor, using the authoritative balance source defined in Section 3.

### 2.3 LTV Ratio

```
LTV Ratio = Total Outstanding Balance / Total Collateral Value
```

Expressed as a decimal. For example, an LTV Ratio of 0.85 means the outstanding debt is 85% of the total collateral value.

### 2.4 LTV Percentage

```
LTV Percentage = LTV Ratio x 100
```

Expressed as a percentage for display purposes in the Power BI Risk Command Centre dashboard.

---

## 3. Balance Authority Rule

### 3.1 Primary Source: S3 Bank Balance Update

Where a `CurrentOutstandingBalance` exists in `silver_bank_balance_update` for a given `LoanID` and `ReportingDate`, that value is the **authoritative outstanding balance** for LTV calculation purposes.

### 3.2 Fallback Source: On-Premises SQL Server

Where no bank balance update exists in `silver_bank_balance_update` for a given `LoanID`, the `OutstandingBalance` from `silver_debtor_loan_collateral` sourced from the on-premises SQL Server is used as the fallback balance.

### 3.3 Rationale

Client banks are the system of record for outstanding balances because debtor payments are made directly to the bank, not to CSL. The on-premises SQL Server balance reflects the balance at the point of loan onboarding and does not capture subsequent debtor payment activity. The nightly S3 bank drop therefore supersedes the on-premises balance wherever available.

### 3.4 Implementation Note

This rule is implemented in the Gold aggregation notebook as a LEFT JOIN from `silver_debtor_loan_collateral` to `silver_bank_balance_update` on `LoanID`, with a COALESCE function selecting the S3 balance where available and falling back to the on-premises balance where not.

---

## 4. Risk Flag Thresholds

These thresholds are defined by the Head of Risk and Collections and implemented by the Data Engineering team. Any change to threshold values requires formal approval and a version increment to this document.

| Risk Flag | LTV Ratio Condition | Description | Immediate Action Required |
|---|---|---|---|
| LOW | LTV Ratio < 0.60 | Collateral comfortably covers outstanding debt | FALSE |
| MEDIUM | 0.60 <= LTV Ratio < 0.80 | Collateral adequate but deteriorating. Monitor closely | FALSE |
| HIGH | 0.80 <= LTV Ratio < 1.00 | Collateral margin dangerously thin | TRUE |
| CRITICAL | LTV Ratio >= 1.00 | Collateral value is less than outstanding debt. Account fully unsecured | TRUE |

### 4.1 ImmediateActionRequired Flag

`ImmediateActionRequired` is set to `TRUE` for all accounts where `RiskFlag` is HIGH or CRITICAL. These accounts appear in the High Risk Accounts view on the Power BI dashboard and are prioritised for collection action.

---

## 5. Collateral Coverage Shortfall

### 5.1 Formula

```
Required Collateral Value = Total Outstanding Balance / 0.80
Collateral Coverage Shortfall = Required Collateral Value - Total Collateral Value
```

### 5.2 Interpretation

- Where the result is **zero or negative**: the debtor is within safe limits. This field is set to NULL.
- Where the result is **positive**: it represents the Naira amount by which the debtor's collateral value must increase to bring the account out of HIGH risk status.

### 5.3 Business Purpose

This field gives collection officers a specific, actionable figure rather than just a ratio. Instead of "this account is HIGH risk", the dashboard communicates "this account needs an additional NGN 4,250,000 in collateral value to return to safe status." That is an actionable instruction.

---

## 6. Stale Price Logic

### 6.1 Primary Price Source

The `ClosePrice` from `silver_market_prices` for the current pipeline run date (`PriceDate = today`) is used as `CurrentMarketPrice` for LTV calculation.

### 6.2 Stale Price Fallback

Where no market price exists for a `TickerSymbol` on the current `PriceDate`, the most recent available `ClosePrice` within the **last 5 business days** is used.

- The record is flagged with `DataFreshnessStatus = STALE` to indicate that a fallback price was used.

### 6.3 Missing Price Exclusion

Where no price exists for a `TickerSymbol` within the last 5 business days:

- The collateral position is flagged with `DataFreshnessStatus = MISSING`
- `EligibleForLTV` is set to `FALSE`
- The position is excluded from LTV calculation
- The record remains in the fact table for audit and investigation purposes

### 6.4 Rationale

Using market prices older than 5 business days introduces unacceptable valuation risk in a margin call context. Stale prices in a volatile market can mask genuine collateral deterioration, leading to delayed collection action and increased recovery risk.

---

## 7. Collateral Eligibility Rules

A collateral position is excluded from LTV calculation (`is_eligible_for_ltv = FALSE`) where any of the following conditions apply:

| Condition | Flag | Action |
|---|---|---|
| `CollateralValueAtPledge` is 0.00 | INVALID_PLEDGE_VALUE | Exclude from LTV |
| `QuantityHeld` cannot be resolved to a positive number | QUANTITY_INVALID | Exclude from LTV |
| `LoanMaturityDate` is before `LoanStartDate` on the associated loan | DATE_ERROR | Exclude from LTV |
| `CurrentOutstandingBalance` is NULL in both S3 and on-premises source | MISSING_BALANCE | Exclude from LTV |
| No market price available within 5 business days | MISSING_PRICE | Exclude from LTV |

Excluded records remain in the fact table with `EligibleForLTV = FALSE`. They are not surfaced in the main LTV dashboard view but are accessible for data quality investigation.

---

## 8. Multi-Loan and Multi-Collateral Debtor Handling

### 8.1 Multi-Loan Debtors

Where a debtor has multiple loans, all outstanding balances are summed to produce `Total Outstanding Balance` before LTV is computed.

```
Total Outstanding Balance = SUM(CurrentOutstandingBalance) across all active loans for DebtorID
```

LTV is calculated at **debtor level**, not loan level.

### 8.2 Multi-Collateral Debtors

Where a debtor has multiple collateral assets across multiple loans, all eligible collateral values are summed to produce `Total Collateral Value` before LTV is computed.

```
Total Collateral Value = SUM(QuantityHeld x CurrentMarketPrice) across all eligible collateral positions for DebtorID
```

### 8.3 Rationale

A debtor's true risk exposure is the relationship between their total debt obligation and their total available collateral. Evaluating LTV at loan level or collateral asset level in isolation would produce misleading risk signals for debtors with complex multi-loan positions.

---

## 9. Pipeline Schedule and Data Freshness

| Parameter | Value |
|---|---|
| Pipeline schedule | Daily, after market close |
| Market close reference | 4:00 PM ET for NYSE and NASDAQ |
| LTV calculation basis | End-of-day closing prices |
| Balance update basis | Most recent nightly S3 bank drop |
| Dashboard default view | Most recent PriceDate snapshot |
| Historical data availability | Full daily history since pipeline go-live |

---

## 10. Loan Status Scope

LTV calculations include loans with `LoanStatus` of **Defaulted** and **Active** only.

Loans with `LoanStatus` of **Settled** are excluded from LTV calculation as the debt obligation has been fulfilled.

---

## 11. Known Data Quality Findings

### 11.1 NASDAQ Misclassification

**Finding:** A systematic exchange misclassification was identified during Silver layer transformation. NASDAQ-listed stocks (AAPL, TSLA, GOOGL, MSFT, AMZN, NVDA) were incorrectly labelled as NYSE in the source collateral table on the on-premises SQL Server.

**Resolution Applied:** Corrected at Silver layer transformation using a ticker-to-exchange lookup mapping. All affected records now carry the correct Exchange value in Silver and Gold.

**Source System Action Required:** The on-premises collateral table requires correction at source. This is a pending remediation item for the IT/Systems Administration team.

**Impact on LTV Calculation:** None. The exchange field is a descriptive attribute and does not participate in the LTV formula. Correction is required for data accuracy and audit purposes only.

---

## 12. Assumptions and Constraints

| Assumption | Detail |
|---|---|
| Market data provider | Yahoo Finance via yfinance Python library. Free tier with no API key required. |
| Supported asset types | NYSE equities and BTC-USD cryptocurrency. NGX-listed securities are not currently supported due to inconsistent data coverage on Yahoo Finance. A licensed market data feed would be required for NGX coverage in production. |
| Currency | All financial values are in Nigerian Naira (NGN) unless otherwise stated. |
| Collateral quantity | QuantityHeld reflects the quantity at the time of pledge. It is not updated to reflect subsequent asset sales or transfers unless a new collateral record is created. |
| LTV threshold values | Defined by the Head of Risk and Collections. Subject to change based on market conditions and regulatory guidance. Any change requires a version increment to this document. |

---

## 13. Document Version History

| Version | Date | Author | Change Summary |
|---|---|---|---|
| v1.0 | March 2026 | Data Engineering Team | Initial version. All rules defined and approved for CSL-DE-001 pipeline go-live. |

---

*CSL-DE-001 | ********* | Data Engineering Division | March 2026*
