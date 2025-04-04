# Data Warehouse Project
🚀 Building a modern data warehouse with SQL Server Management Studio (SSMS), including ETL processes and data modeling. 



## 🏗  Data Architecture 


![The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers](docs/data_flow_diagram.png)

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

## 📖 Project Overview
This project involves:

Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
Data Modeling: Developing fact and dimension tables optimized for analytical queries.


### This repository shows my skills in: 

- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Developer
- Data Modeling


## 🎯 Project Requirements

Building the Data Warehouse (Data Engineering) in SSMS

#### Specifications

Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
Data Quality: Cleanse and resolve data quality issues prior to analysis.
Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
Scope: Focus on the latest dataset only; historization of data is not required.
Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.






## Data Integration Model
![Database Schema: Star Schema](docs/Integration_model.png)



