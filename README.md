Municipal Bond Market Database & Analysis

Authors: Mark Crisci, Muhammed Altindal, and Avery Bishop

Course: Databases (Final Project)

Repository: https://github.com/mcrisci24/Final-Project-Databases

Streamlit Deployed Link: https://final-project-databases-89wbbgrpmrftas5apyd8pp.streamlit.app/

Project Overview

This project implements a robust relational database system for analyzing the US Municipal Bond Market. It integrates demographic data, financial trade records, credit ratings, and macroeconomic indicators into a unified PostgreSQL warehouse.
The goal is to enable complex financial analysis—such as calculating yield spreads, assessing credit risk, and identifying liquidity trends—that would be impossible using disjointed spreadsheets.

Key Features

Relational Schema: A normalized database structure (Star Schema inspired) with a central Fact table (bonds) and supporting Dimensions (issuers, bond_purposes).
Python ETL Pipeline: A custom load.py script that cleanses and loads raw CSV data using pandas and SQLAlchemy.
Interactive Dashboard: A Streamlit web application providing live visualizations of yield curves, risk spreads, and volume trends.
Audit System: Database triggers that automatically log all insertions, updates, and deletions for compliance.

Note on Deployment: Converting the Data to MongoDB for the Streamlit App

When running the dashboard locally, the application connects directly to our PostgreSQL database. However, deploying the dashboard publicly through Streamlit Cloud introduced some limitations. Streamlit Cloud does not support hosting or initiating a local Postgres server, and it restricts outbound connections to most external database ports—including those typically used by PostgreSQL. To make the dashboard portable and fully deployable, we converted our cleaned relational data into MongoDB documents. MongoDB Atlas provides a cloud-native, publicly reachable database endpoint, which allows the Streamlit application to fetch the same data—now structured as collections instead of SQL tables—without requiring a local server.

This conversion required:

- Restructuring the SQL tables into document-friendly formats.

- Rewriting SQL joins and aggregations as MongoDB aggregation pipelines.

- Updating the dashboard queries to use PyMongo instead of SQLAlchemy.

- Ensuring that derived metrics (liquidity, spreads, sentiment trends, etc.) produce the same results across both systems.

Functionally, the dashboard behaves the same, but the backend is now fully cloud-compatible. This enables sharing the application through a simple web link rather than relying on localhost.

Database Architecture

The database municipal_bonds is designed to answer questions about risk and return in the public sector.

Schema Diagram

Table Descriptions

Table
Type
Description
issuers
Dimension
Demographic info on the borrowing entity (State, Population, Tax Base).
bonds
Fact
Core instrument data (CUSIP, Coupon Rate, Maturity Date, Face Value).
bond_purposes
Dimension
Categorization of funding use (e.g., 'Education', 'Public Safety').
trades
Transaction
Time-series data of individual bond transactions (Price, Yield, Quantity).
credit_ratings
Dimension
History of credit ratings and outlooks (Moody's, S&P) for each bond.
macro_economic_data
External
State-level economic health indicators (Unemployment, 10Y Treasury Rates).
audit_logs
System
Automatically tracks data changes via triggers.

Repository Contents

Core System Files

municipal_schema.sql: The DDL script. Creates tables, primary keys, foreign keys, indexes, and constraints.
municipal_triggers.sql: Contains the PL/pgSQL functions and triggers for the audit logging system.
load.py: The ETL script. Connects to the database and bulk-loads the CSV files in the correct dependency order.
MuniBonds_dashboard.py: The Streamlit Python application for the interactive dashboard.

Data Files (Raw Source)

issuers.csv, bonds.csv, bond_purposes.csv, trades.csv, credit_ratings.csv, economic_indicators.csv

Analysis Files

municipal_bonds_analysis.sql: A collection of the advanced SQL queries used to power the dashboard visuals.
Setup & Installation
Follow these steps to replicate the environment locally.

Prerequisites

PostgreSQL 
Python 
PgAdmin 4 


Step 1:

Database Creation
Open your terminal or SQL tool (PgAdmin) and create the database:
CREATE DATABASE municipal_bonds;


Step 2:

Create Schema & Triggers
Run the schema script to build the tables, then the trigger script to enable auditing.
# Run schema first
psql -U postgres -d municipal_bonds -f municipal_schema.sql

# Then run triggers
psql -U postgres -d municipal_bonds -f municipal_triggers.sql


(Alternatively, open these files in PgAdmin and execute them manually.)

Step 3: 

Install Python Dependencies
Ensure you have the required libraries for ETL and the Dashboard:
pip install pandas sqlalchemy psycopg2-binary plotly streamlit


Step 4: 

Load Data
Run the Python loader.
Note: Ensure DB_PORT in load.py matches your local Postgres port (default is 5432, this project uses 5433).
python load.py


You should see success messages for connecting and loading each table.

Step 5: 

Launch the Dashboard
Start the interactive application:
streamlit run MuniBonds_dashboard.py


The dashboard will open in your default browser locally.
How to Run the Analysis (SQL)
If you prefer to run raw SQL queries instead of using the dashboard:
Open your SQL client (e.g., PgAdmin, DBeaver).
Connect to the municipal_bonds database.
Open municipal_bonds_analysis.sql.
Highlight and execute individual queries to see the results.
Key Analyses Included:
Yield Spread Calculation: Compares municipal yields against the "risk-free" 10-Year Treasury rate.
Liquidity Analysis: Identifies the most actively traded long-duration bonds.
Credit Risk Trends: Analyzing rating outlook changes (Positive vs. Negative) over time.


