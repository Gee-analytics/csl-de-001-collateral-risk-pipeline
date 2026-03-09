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




