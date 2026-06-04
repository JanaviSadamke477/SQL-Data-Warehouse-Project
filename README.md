# Customer Segmentation & Data Warehouse Analytics Platform

## Overview

This project demonstrates the design and implementation of an end-to-end analytics solution using SQL Server, Python, and Power BI. The solution follows the Medallion Architecture (Bronze, Silver, Gold) to transform raw sales data into a structured analytical warehouse. Customer behavior is analyzed using RFM (Recency, Frequency, Monetary) metrics and K-Means clustering to generate actionable customer segments.

## Objectives

* Build a scalable SQL Server data warehouse.
* Implement Bronze, Silver, and Gold data layers.
* Design a star schema for analytical reporting.
* Perform customer segmentation using RFM analysis.
* Apply K-Means clustering for customer grouping.
* Create interactive Power BI dashboards for business insights.

---

## Architecture

Source Data
→ Bronze Layer (Raw Data Ingestion)
→ Silver Layer (Data Cleansing & Transformation)
→ Gold Layer (Star Schema Modeling)
→ Customer Segmentation (Python & K-Means)
→ Power BI Dashboard

---

## Data Warehouse Design

### Bronze Layer

* Loaded raw source data into SQL Server.
* Preserved original records without business transformations.

### Silver Layer

* Cleaned and standardized data.
* Removed inconsistencies and prepared data for analytics.
* Applied transformation logic and quality checks.

### Gold Layer

* Designed a star schema.
* Created dimension tables:

  * dim_customers
  * dim_products
* Created fact table:

  * fact_sales
* Optimized data structure for reporting and business analysis.

---

## Customer Segmentation

### RFM Analysis

Customer behavior was measured using:

* Recency: Days since last purchase
* Frequency: Number of purchases
* Monetary: Total customer spending

### Data Preparation

* Handled missing values.
* Standardized RFM features using StandardScaler.
* Prepared data for clustering.

### K-Means Clustering

* Applied K-Means clustering.
* Used the Elbow Method to determine the optimal number of clusters.
* Selected k = 5 customer segments.

### Business Segments

The resulting clusters were interpreted as:

* Loyal Customers
* High-Value Occasional Buyers
* Churned High-Value Customers
* At Risk Low-Value Customers
* Low-Value Regular Customers

---

## Dashboard Features

### Executive KPIs

* Total Sales
* Total Customers
* Average Order Value

### Customer Segmentation Analysis

* Customer distribution by segment
* Revenue contribution by segment
* Segment-based filtering

### Interactive Exploration

* Country-level filtering
* Segment-level filtering
* Dynamic KPI updates

---

## Technology Stack

### Database

* SQL Server

### Data Engineering

* SQL
* Stored Procedures
* ETL Pipelines

### Data Science

* Python
* Pandas
* NumPy
* Scikit-learn

### Business Intelligence

* Power BI

---

## Key Learnings

* Data warehouse design using Medallion Architecture.
* Dimensional modeling and star schema design.
* ETL development and data transformation.
* Customer segmentation using machine learning.
* Building business-focused analytical dashboards.
* Integrating SQL, Python, and Power BI into a single analytics workflow.

---

## Future Improvements

* Automate data refresh pipelines.
* Implement incremental loading strategies.
* Add customer lifetime value (CLV) analysis.
* Deploy the solution on a cloud platform.
* Build predictive churn models using supervised machine learning.
