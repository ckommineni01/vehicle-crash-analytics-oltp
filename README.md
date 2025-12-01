# Vehicle Crash Analytics (Phase 1 — OLTP Database Foundation)

This project builds a normalized OLTP database for NYC Motor Vehicle Collision data.
It includes: ERD, 3NF schema, PostgreSQL using Docker, Python ETL pipeline, and RBAC security roles.

------------------------------------------------------------

## Repository Structure
database_project/
├── docker-compose.yml
├── schema.sql
├── ingest_data.py
├── security.sql
├── README.md
│
├── erd/
│   └── ERD.png
│
├── docs/
│   ├── conceptual_logical_design.docx
│   └── 3NF_justification.pdf
│
└── screenshots/
    ├── docker_ps.png
    ├── psql_tables.png
    ├── collisions_sample.png
    ├── borough_counts.png
    ├── vehicles_sample.png
    ├── ingestion_success.png
    └── security_roles.png

------------------------------------------------------------

## 1. Database Design (Conceptual & Logical)

### ERD:
Located in `erd/ERD.png`

### Entities:
- boroughs
- collisions
- collision_vehicles
- vehicle_types
- collision_factors
- factors

### Normalization:
Schema is in full **Third Normal Form (3NF)**.
Explanation is in: `docs/conceptual_logical_design.docx`

------------------------------------------------------------

## 2. Physical Schema
All CREATE TABLE statements are in `schema.sql`.

Includes:
- Primary keys
- Foreign keys
- NOT NULL, UNIQUE, CHECK
- Correct data types

Docker automatically runs this file when container starts.

------------------------------------------------------------

## 3. Data Ingestion Pipeline (ETL)

Python script: `ingest_data.py`
It performs:
- CSV loading using Pandas
- Cleaning missing values
- Converting data types
- Loading tables in correct order
- Using SQLAlchemy to insert data into Docker PostgreSQL

ETL success screenshots are in `/screenshots/`.

------------------------------------------------------------

## 4. Security (RBAC)

Roles created in `security.sql`:

### readonly:
- SELECT only

### readwrite:
- SELECT, INSERT, UPDATE, DELETE

Run with:
docker exec -i collisions_db psql -U postgres collisions < security.sql

------------------------------------------------------------

## 5. Docker Deployment

Start database:
docker-compose up -d

Stop database:
docker-compose down -v

------------------------------------------------------------

## 6. Verification Queries

Inside PostgreSQL:
docker exec -it collisions_db psql -U postgres collisions

Commands used:
- \dt
- SELECT * FROM collisions LIMIT 5;
- Borough counts
- Vehicle counts
- Factor counts

Screenshots in `/screenshots/`.

------------------------------------------------------------

## 7. Video Demo
Video demonstrates:
- Docker running
- Schema created
- ETL script running
- SQL queries
- Security roles

------------------------------------------------------------

## Phase 1 Complete
This repository includes all deliverables required for Phase 1 (OLTP).




# Vehicle Crash Analytics (Phase 2 — OLAP Analytical Layer)

This phase extends the OLTP database into an analytical layer.  
It includes: advanced SQL analytical queries, performance tuning using EXPLAIN ANALYZE, indexing strategies, and an optional OLAP star schema with dbt-based ETL modeling.

------------------------------------------------------------

## Repository Additions for Phase 2
database_project/
├── sql/
│   └── advanced_queries.sql
│
├── reports/
│   ├── performance_tuning.md
│   ├── dimensional_model_phase2.md
│   └── star_schema.png
│
└── screenshots/
    ├── q1_output.png
    ├── q2_output.png
    ├── q3_output.png
    ├── explain_before.png
    ├── explain_after.png
    └── star_schema.png

------------------------------------------------------------

## 1. Advanced Analytical Queries

Three complex analytical SQL queries were created to answer meaningful business questions about NYC motor vehicle collisions.

Queries include:
- Multi-table joins  
- Aggregations  
- Subqueries / CTEs  
- Window functions  

All analytical queries are located in:


Screenshots of executed queries are included in `/screenshots/`.

------------------------------------------------------------

## 2. Query Optimization & Performance Tuning

Query 2 — **Contributing Factors Ranked by Fatality Rate** — was selected as the most complex analytical query for performance tuning.

### Before Indexing:
- Execution Time: **100.333 ms**
- PostgreSQL performed full sequential scans on:
  - `collision_factors` (~44k rows)
  - `collisions` (~50k rows)
- Joins had **no supporting indexes**, causing high computational cost

### Indexing Strategy:
Indexes were added to improve join performance and reduce full-table scans:

```sql
CREATE INDEX idx_collision_factors_factor
    ON collision_factors (factor_id);

CREATE INDEX idx_collision_factors_collision
    ON collision_factors (collision_id);

CREATE INDEX idx_collisions_date_borough
    ON collisions (crash_date, borough_id);



