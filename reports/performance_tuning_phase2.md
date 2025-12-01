## Performance Tuning Summary

**Before Indexing:**  
The execution time for Query 2 was **100.333 ms**. PostgreSQL performed sequential scans on both `collision_factors` (~44k rows) and `collisions` (~50k rows). Because the joins used `factor_id` and `collision_id`—and neither had indexes—the database had no fast lookup path. As a result, the query had to read every row, build large hash tables, and process full-table scans before grouping. This made the query slower.

**After Indexing:**  
After adding indexes on `collision_factors.factor_id`, `collision_factors.collision_id`, and `collisions (crash_date, borough_id)`, the execution time improved to **38.823 ms**. PostgreSQL was able to use index-based lookups instead of scanning full tables. This reduced the number of rows examined during joins and allowed the query to execute much more efficiently.

**Why the Indexes Worked:**  
The chosen indexes directly matched the join conditions and filtering patterns used in the query. Indexing `factor_id` and `collision_id` allowed PostgreSQL to locate matching rows quickly, which dramatically reduced join cost. The composite index on `crash_date, borough_id` improved access paths for analytical workloads. Overall, the indexes reduced unnecessary scanning, improved join performance, and lowered the total execution time by approximately **61%**.
