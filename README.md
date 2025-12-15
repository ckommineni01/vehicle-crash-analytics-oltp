# Vehicle Crash Analytics  
**EAS 550 – Database Foundations | End-to-End Project**

This project implements a complete database-driven analytics system for NYC Motor Vehicle Collision data.  
It progresses through three phases: OLTP database design, OLAP analytics and optimization, and an interactive application layer.

The entire system is containerized and runnable end-to-end using Docker Compose.

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
├── reports/
│ ├── performance_tuning.md
│ ├── dimensional_model_phase2.md
│ └── star_schema.png
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
