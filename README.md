# Vehicle Crash Analytics  
**EAS 550 – Database Foundations | End-to-End Project**

This project implements a complete database-driven analytics system for NYC Motor Vehicle Collision data.  
It progresses through three phases: OLTP database design, OLAP analytics and optimization, and an interactive application layer.

The entire system is containerized and runnable end-to-end using Docker Compose.

------------------------------------------------------------

# Phase 1 — OLTP Database Foundation

This phase focuses on designing and implementing a normalized OLTP database for NYC Motor Vehicle Collision data using PostgreSQL and Docker.

------------------------------------------------------------

## 1. Database Design (Conceptual & Logical)

### ERD
The entity-relationship diagram is located at:

### Core Entities
- boroughs
- collisions
- vehicle_types
- collision_vehicles
- factors
- collision_factors

### Normalization
The schema is fully normalized to **Third Normal Form (3NF)**.

Detailed explanations are provided in:
- `docs/conceptual_logical_design.docx`
- `docs/3NF_justification.pdf`

------------------------------------------------------------

## 2. Physical Schema

All table definitions are contained in:

Features include:
- Primary keys
- Foreign keys
- UNIQUE, NOT NULL, CHECK constraints
- Appropriate data types

The schema is automatically applied when the PostgreSQL container initializes.

------------------------------------------------------------

## 3. Data Ingestion Pipeline (ETL)

Python script:

ETL responsibilities:
- Load raw CSV data using Pandas
- Handle missing values
- Convert date and time columns
- Insert data in referentially correct order
- Populate junction tables for vehicles and contributing factors

------------------------------------------------------------

## 4. Security (RBAC)

Role-based access control is defined in:

Roles:
- **readonly**: SELECT privileges
- **readwrite**: SELECT, INSERT, UPDATE, DELETE privileges

Execution:
```bash
docker exec -i collisions_db psql -U postgres collisions < security.sql
docker compose up -d db
docker compose up -d db

sql/advanced_queries.sql
CREATE INDEX idx_collision_factors_factor
    ON collision_factors (factor_id);

CREATE INDEX idx_collision_factors_collision
    ON collision_factors (collision_id);

CREATE INDEX idx_collisions_date_borough
    ON collisions (crash_date, borough_id);


