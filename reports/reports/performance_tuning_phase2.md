# Phase 2.2 – Query Performance Tuning

## Query Selected
**Query 2 – Contributing Factors Ranked by Fatality Rate**

This was the most complex analytical query from Step 2.1 because it:
- Joins three OLTP tables (`collision_factors`, `factors`, `collisions`)
- Uses multiple CTEs
- Performs GROUP BY aggregation
- Computes percentages
- Uses a window function (`RANK()`)

This workload is ideal for demonstrating performance improvements via indexing.

---

## Full Query Used for Tuning

```sql
WITH factor_events AS (
    SELECT
        cf.collision_id,
        f.factor_desc,
        c.number_of_persons_killed
    FROM collision_factors cf
    JOIN factors f
        ON cf.factor_id = f.factor_id
    JOIN collisions c
        ON cf.collision_id = c.collision_id
    WHERE f.factor_desc IS NOT NULL
),
factor_agg AS (
    SELECT
        factor_desc,
        COUNT(*) AS factor_collision_count,
        SUM(CASE WHEN number_of_persons_killed > 0 THEN 1 ELSE 0 END) 
            AS fatal_collisions
    FROM factor_events
    GROUP BY factor_desc
),
factor_rates AS (
    SELECT
        factor_desc,
        factor_collision_count,
        fatal_collisions,
        ROUND(
            fatal_collisions::numeric / NULLIF(factor_collision_count, 0) * 100,
            2
        ) AS fatal_rate_pct
    FROM factor_agg
)
SELECT
    factor_desc,
    factor_collision_count,
    fatal_collisions,
    fatal_rate_pct,
    RANK() OVER (ORDER BY fatal_rate_pct DESC) AS fatality_rank
FROM factor_rates
WHERE factor_collision_count >= 50
ORDER BY fatality_rank;
