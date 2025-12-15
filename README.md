# Vehicle Crash Analytics  
**End-to-End Database & Analytics System (Dockerized)**

This project implements a complete, production-style database analytics system for **NYC Motor Vehicle Collision data**.  
It covers database design, data ingestion, analytical querying, performance optimization, and an interactive analytics dashboard — all deployed using Docker.

The entire project is fully reproducible and runnable end-to-end via **Docker Compose**.

------------------------------------------------------------

## Project Overview

The system is designed to:

- Store large-scale collision data in a normalized PostgreSQL database
- Enforce relational integrity and normalization (3NF)
- Support analytical SQL queries and performance tuning
- Provide an interactive dashboard for data exploration
- Run automatically with minimal setup using Docker

------------------------------------------------------------

## Repository Structure
vehicle-crash-analytics-oltp/
├── docker-compose.yml
├── Dockerfile
├── schema.sql
├── ingest_data.py
├── security.sql
├── app.py
├── requirements.txt
├── motor_vehicle_collisions_2023_2024.csv
├── README.md
│
├── erd/
│ └── ERD.png
│
├── docs/
│ ├── conceptual_logical_design.docx
│ └── 3NF_justification.pdf
│
├── sql/
│ └── advanced_queries.sql
│
└── screenshots/
├── docker_ps.png
├── psql_tables.png
├── ingestion_success.png
├── security_roles.png
├── q1_output.png
├── q2_output.png
├── q3_output.png
├── explain_before.png
└── explain_after.png


------------------------------------------------------------

## Database Design

The database schema is designed using a **conceptual ER model**, translated into a **fully normalized relational schema (3NF)**.

### Core Tables
- boroughs
- collisions
- vehicle_types
- collision_vehicles
- factors
- collision_factors

### Design Principles
- Elimination of redundancy
- Proper use of primary and foreign keys
- Enforcement of data integrity using constraints
- Separation of lookup tables and fact tables

Design artifacts:
- ER Diagram: `erd/ERD.png`
- Logical design & normalization justification:
  - `docs/conceptual_logical_design.docx`
  - `docs/3NF_justification.pdf`

------------------------------------------------------------

## Physical Schema

All table definitions are implemented in:


The schema includes:
- Primary keys
- Foreign keys
- UNIQUE, NOT NULL, CHECK constraints
- Appropriate numeric, date, and text data types

The schema is **automatically created** when the PostgreSQL container starts.

------------------------------------------------------------

## Data Ingestion (ETL Pipeline)

The data ingestion pipeline is implemented in:


### ETL Responsibilities
- Load raw CSV data using Pandas
- Handle missing and null values
- Convert date and time fields
- Insert records in the correct relational order
- Populate junction tables for vehicles and contributing factors

The pipeline runs automatically as a Docker service after the database becomes healthy.

------------------------------------------------------------

## Security (Role-Based Access Control)

Database security is implemented using PostgreSQL roles defined in:

### Roles
- **readonly** — SELECT privileges only
- **readwrite** — SELECT, INSERT, UPDATE, DELETE privileges

Roles can be applied using:
```bash
docker exec -i collisions_db psql -U postgres collisions < security.sql
## Analytical Queries & Performance Optimization

## Advanced Analytical SQL Queries
Advanced analytical SQL queries are stored in:


## Query Features
The queries include the following advanced SQL features:

- Multi-table joins  
- Aggregations  
- Common Table Expressions (CTEs) and subqueries  
- Window functions  

## Performance Tuning
To ensure optimal query performance, the following steps were taken:

- Execution plans analyzed using `EXPLAIN ANALYZE`  
- Indexes added to improve join and filter performance  
- Before and after execution plans documented in:

CREATE INDEX idx_collision_factors_factor
    ON collision_factors (factor_id);

CREATE INDEX idx_collision_factors_collision
    ON collision_factors (collision_id);

CREATE INDEX idx_collisions_date_borough
    ON collisions (crash_date, borough_id);


# Interactive Analytics Dashboard

An interactive analytics dashboard is built using **Streamlit** and connects directly to the **PostgreSQL** database.

## Dashboard Features

### KPI Metrics
- Total Crashes  
- Total Injured  
- Total Killed  

### Interactive Filters
- Date range selector  
- Borough dropdown  
- Metric selector  
- Top-N slider  

### Visualizations
- Crashes by Borough (bar chart)  
- Trends over Time (line chart)  
- Top Contributing Factors (bar chart)  

### Data Table
- Latest filtered records  
- CSV download support  

The dashboard runs automatically as a **Docker service**.

---

## End-to-End Execution Flow

Using **Docker Compose**, the system runs in the following order:

1. PostgreSQL database container starts  
2. Database schema is created automatically  
3. ETL pipeline ingests collision data  
4. Streamlit dashboard launches and connects to the database  

All services are orchestrated via:


---

## Running the Project

### Prerequisites
- Docker Desktop  
- Git  

### Steps to Run
```bash
git clone https://github.com/Nirwade/vehicle-crash-analytics-oltp.git
cd vehicle-crash-analytics-oltp
docker compose up --build
http://localhost:8501



## demo video link: "https://drive.google.com/file/d/1o741zlMUIoQbE17SOpZGQFdQYMSjkmIv/view?usp=drivesdk"
