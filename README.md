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


<h3> Key Engineering Features </h3>
<strong>Hybrid Cloud Connectivity</strong>
Secure extraction from an on-premises SQL Server via Microsoft On-Premises Data Gateway without exposing the database to the open internet. Standard enterprise pattern for hybrid architectures. <br>

<strong>Zero-Copy S3 Integration</strong>Zero-Copy S3 Integration
Client bank balance files in Amazon S3 are mounted as a native Lakehouse path via Fabric Shortcut. 
Data stays in S3 with no redundant movement or storage cost. <br>

<strong>Incremental Loading</strong>
Watermark-based incremental logic on the SQL Server and API sources ensures only new or changed records are processed on each run, reducing compute cost and pipeline runtime significantly. <br>

