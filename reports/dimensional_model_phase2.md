# Step 2.3 – Dimensional Modeling & ETL (Optional)

## Star Schema Overview
To support analytical queries, I designed a star schema with a central fact table and several supporting dimension tables.

### Grain
The grain of the fact table is **one row per collision**, identified by `collision_id`.

### Fact Table: fact_collisions
Measures:
- persons_injured  
- persons_killed  
- pedestrians_injured  
- pedestrians_killed  
- cyclists_injured  
- cyclists_killed  
- motorists_injured  
- motorists_killed  
- vehicle_count  
- factor_count  

Foreign keys:
- `date_key` → dim_date  
- `borough_key` → dim_borough  
- `location_key` → dim_location  

### Dimension Tables

#### dim_date
- date_key (PK)  
- full_date  
- year  
- month  
- day  
- day_of_week  
- is_weekend  

#### dim_borough
- borough_key (PK)  
- borough_name  

#### dim_location
- location_key (PK)  
- zip_code  
- on_street_name  
- cross_street_name  
- latitude  
- longitude  

#### dim_vehicle (optional)
- vehicle_key (PK)  
- vehicle_type  

#### dim_factor (optional)
- factor_key (PK)  
- factor_description  

## ETL / dbt Overview
The ETL process loads OLTP data into the dimensional model using dbt.

### ETL Steps:
1. **Staging:** dbt models read the raw OLTP tables (collisions, vehicles, factors).  
2. **Dimension Building:** Build dim_date, dim_borough, dim_location from staged data.  
3. **Fact Table:** Join staged collisions with dimensions and aggregate vehicle & factor counts.

Example dbt model for fact_collisions:

```sql
select
    c.collision_id,
    d.date_key,
    b.borough_key,
    l.location_key,
    count(v.vehicle_id) as vehicle_count,
    count(f.factor_id) as factor_count,
    c.number_of_persons_injured,
    c.number_of_persons_killed
from stg_collisions c
left join dim_date d on d.full_date = c.crash_date
left join dim_borough b on b.borough_name = c.borough
left join dim_location l on l.zip_code = c.zip_code
left join stg_collision_vehicles v on v.collision_id = c.collision_id
left join stg_collision_factors f on f.collision_id = c.collision_id
group by c.collision_id, d.date_key, b.borough_key, l.location_key;
