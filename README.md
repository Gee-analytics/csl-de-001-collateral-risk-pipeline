<h1> csl-de-001-collateral-risk-pipeline </h1>
End-to-end data engineering pipeline on Microsoft Fabric monitoring debtor collateral risk and automating margin call alerts via a real-time LTV Risk Command Centre.

<h2>CSL-DE-001: Collateral Risk Monitoring & Margin Call Automation Pipeline </h2>

<h3>Overview</h3>
<p>Collection Solutions Limited (CSL) is a Nigerian debt recovery and financial assurance agency managing delinquent receivables on behalf of multiple client banks. This project implements an automated, end-to-end data engineering pipeline that monitors the market value of debtor pledged collateral against outstanding loan balances in near real-time, triggering margin call alerts before collateral value falls below the debt threshold. <br>
The pipeline transforms CSL from a reactive collections agency into a proactive, data-driven risk management operation.</p>

<h3>Business Problem</h3>
<p>Debtors who pledge volatile assets such as stocks and cryptocurrency as loan collateral present a significant recovery risk when market values decline. Without automated monitoring, collection actions are triggered reactively after collateral value has already eroded below the outstanding debt threshold, reducing the likelihood of full recovery. <br>
This pipeline solves that problem by computing Loan-to-Value ratios per debtor daily, flagging high-risk accounts automatically, and surfacing actionable insights through a Power BI Risk Command Centre dashboard.</p>

<h3>Architecture</h3>
<img width="7951" height="2972" alt="CSL_DE_001_Architecture_HighLevel_v3 0 drawio" src="https://github.com/user-attachments/assets/b75279d1-2285-4054-93ae-c70c5f692176" />


The pipeline follows a Medallion Lakehouse architecture on Microsoft Fabric, ingesting data from three sources into Bronze, transforming and joining in Silver, computing business logic in Gold, and serving a Direct Lake Power BI dashboard.

| Layer | Responsibility |
| :--- | :--- |
| **Bronze** | Raw Delta tables. Schema on read. No transformations. |
| **Silver** | Cleansed, typed, deduplicated, joined, PII masked. |
| **Gold** | LTV calculations, risk flags, business ready output. |

### Data Sources

| Data Source | Technology | Description |
| :--- | :--- | :--- |
| **On-Prem SQL Server** | MS Data Gateway | CSL internal debtor, loan, collateral, and officer records. |
| **Amazon S3** | Fabric Shortcut | Daily Parquet balance update files (4 client banks). |
| **Yahoo Finance** | Python (yfinance) | Daily closing prices for AAPL, TSLA, GOOGL, MSFT, AMZN, NVDA, BTC-USD. |


### Technical Stack

| Category | Technology |
| :--- | :--- |
| **Platform** | Microsoft Fabric (Lakehouse, Spark, Data Pipeline, Power BI) |
| **Storage** | Delta Parquet, OneLake |
| **Ingestion** | Fabric Data Pipeline, On-Prem Gateway, Fabric Shortcut, PySpark |
| **Transformation** | PySpark Notebooks, Medallion Architecture |
| **Orchestration** | Fabric Data Pipeline (w/ dependency logic & failure alerts) |
| **Presentation** | Power BI Direct Lake mode (w/ Row Level Security) |
| **Version Control** | Git, GitHub (branch-based environment promotion) |
| **Cloud/Local** | AWS S3 (Landing Zone), On-Prem SQL Server |


### Key Engineering Features

* <u>**Hybrid Cloud Connectivity**</u>: <br>
  Secure extraction from an on-premises SQL Server via Microsoft On-Premises Data Gateway without exposing the database to the open internet. Standard enterprise pattern for hybrid architectures.
  
* <u>**Zero-Copy S3 Integration**</u>: <br>
  Client bank balance files in S3 are mounted as native Lakehouse paths via Fabric Shortcuts, eliminating redundant data movement and storage costs.
  
* <u>**Incremental Loading**</u>: <br>
  Watermark-based incremental logic on the SQL Server and API sources ensures only new or changed records are processed on each run, reducing compute cost and pipeline runtime significantly.
  
* <u>**Multi-Asset LTV Aggregation**</u>: <br>
  LTV ratio is computed by aggregating the current market value across all collateral positions per debtor before dividing by outstanding balance. A debtor holding multiple tickers across multiple loans is handled correctly.
  
* <u>**Data Quality Framework**</u>: <br>
  Dirty records are never silently dropped. Every data quality issue is flagged with a specific flag column and an is_eligible_for_ltv boolean controls which records participate in LTV calculation. Bad records are quarantined and visible for investigation - ensuring auditability.
  
* <u>**PII Protection**</u>: <br>
  Debtor contact data (phone, email, address) is SHA-256 hashed at the Silver layer before hitting Gold/Presentation.
  
* <u>**Row-Level Security (RLS)**</u>: <br>
  Power BI RLS rules map each collection officer to their assigned client portfolios and regions via the officer_client_mapping table. Officers see only the   <br> accounts they are authorised to action.
  
* <u>**Audit Trail**</u>: <br>
  Every pipeline run logs start time, end time, row counts, and source system to a metadata table, providing a full forensic audit trail of data movement.


### Data Governance & Security

| Requirement | Implementation | Status |
|---|---|---|
| PII Protection | SHA-256 hashing of NationalID at Silver layer | Complete |
| Row Level Security | Power BI semantic model RLS via USERPRINCIPALNAME() filtering on dim_collections_officer | Complete |
| Object Level Security | Designed for PII columns in dim_debtor via XMLA endpoint and Tabular Editor | Deferred - see note below |
| OneLake Storage Security | Designed for storage level column restriction across all Fabric engines via OneLake data access roles | Deferred - see note below |
| Access Control | Microsoft Fabric workspace roles | Complete |
| Audit Trail | gold_audit_log table capturing every pipeline step per run | Complete |

### Security Implementation Note

Two security features were designed but not implemented due to Fabric free trial environment constraints:

**Object Level Security (OLS):** Implementation requires XMLA endpoint access via Tabular Editor. XMLA endpoint is not available on Fabric free trial accounts. In a production environment OLS would be configured on PhoneNumber, EmailAddress, and ResidentialAddress columns in dim_debtor, restricting visibility to CollectionsOfficer and Admin roles only.

**OneLake Data Access Roles:** Implementation requires Microsoft Entra ID organisational credentials. Personal Microsoft accounts on Fabric free trial do not support this feature. In a production environment OneLake data access roles would enforce column level restrictions at the storage layer, applying across all Fabric engines including Spark, the SQL Analytics Endpoint, and Power BI. This provides a stronger security posture than semantic model level OLS alone because it cannot be bypassed by querying through an alternative engine.

The security architecture is fully designed and documented. RLS is implemented and tested. Both deferred features would be prioritised in a production deployment.


### Pipeline Orchestration

The master Fabric Data Pipeline executes all steps in dependency order daily at market close:

| Order | Action |
| :--- | :--- |
| 1 | Extract from on-premises SQL Server via Gateway to Bronze |
| 2 | Validate S3 Shortcut and register new client bank files to Bronze |
| 3 | Run yfinance Spark Notebook to fetch latest market prices to Bronze |
| 4 | Run Silver transformation notebook |
| 5 | Run Gold aggregation notebook |
| 6 | Trigger Power BI semantic model refresh |

Failed steps trigger an email alert to the Data Engineering team. Each step logs to a pipeline metadata table.


### LTV Calculation Logic

Total Collateral Value = SUM(QuantityHeld x CurrentMarketPrice) for all tickers per debtor
LTV Ratio = OutstandingBalance / Total Collateral Value
High Risk Flag = TRUE where LTV Ratio > 0.80

Edge case handling: where no price exists for a ticker on the current date, the most recent available price within the last 5 business days is used. Records with no price within 5 days are flagged as STALE DATA and excluded from LTV calculation.

### Architecture Decision Notes

**Lakehouse over Fabric Data Warehouse**: <br>
A Lakehouse was chosen over a Fabric Data Warehouse because the pipeline is notebook-driven with PySpark, data arrives raw and unstructured at Bronze requiring schema flexibility, and Power BI Direct Lake mode requires Delta tables in a Lakehouse. A Warehouse would be appropriate for a high concurrency SQL analyst workload, which is not the use case here.

**Spark Notebook over Eventstream for API Ingestion**: <br>
Eventstream is designed for continuous unbounded event streams. Daily end-of-day API calls are a scheduled batch operation. A Spark Notebook provides full control over rate limiting, JSON flattening, incremental watermark logic, and error handling. The tool fits the workload.

**NYSE Tickers over NGX Tickers**: <br>
NGX-listed securities on Yahoo Finance have inconsistent data coverage and frequent gaps. NYSE and NASDAQ tickers provide reliable, consistent daily price data. In a production environment, NGX securities would be handled via a licensed market data feed.


### Project Context

This pipeline was designed and built based on operational challenges observed in the debt recovery and financial assurance industry. All data used in this project is fully synthetic, generated specifically to simulate realistic data quality issues and relational complexity. No real debtor, client, or financial data was used at any stage.

## Known Limitations and Technical Debt

The following items were designed and documented but not fully implemented 
due to Fabric free trial environment constraints or scope decisions. 
Each has a defined remediation path for a production deployment.

| Item | Severity | Reason | Remediation |
|---|---|---|---|
| Stream A watermark implementation | MEDIUM | Deferred due to Fabric trial time constraints. SQL Server ingestion currently uses full load pattern. | Implement Max date watermark logic on Stream A to reduce pipeline runtime and compute cost in production |
| View 4 Collateral Value Trend | LOW | Requires minimum 5 daily LTV snapshots to render a meaningful trend line. Deferred pending data accumulation. | Build once sufficient daily snapshots are available. Pipeline is running daily and accumulating data automatically |
| BTC-USD weekend price gaps | MEDIUM | yfinance returns NULL ClosePrice for BTC-USD on weekend dates. 17 records flagged as MISSING and excluded from LTV. | Source weekend crypto prices from Coinbase or Binance API in production |
| Public holidays not modelled in dim_date | LOW | Licensed holiday calendar required for accurate IsMarketDay classification. | Integrate a licensed public holiday calendar in production |




***(CSL-DE-001 | Data Engineering Division | March 2026)***
