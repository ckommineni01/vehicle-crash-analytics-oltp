Q1 – Top 10 most dangerous streets in each borough (2023–2024)
Business question:
Which streets in each borough have the highest total injuries + deaths?

Advanced features: CTEs, join, aggregation, window function.

  WITH street_severity AS (
    SELECT
        b.borough_name,
        c.on_street_name,
        c.crash_date,
        (c.number_of_persons_injured + c.number_of_persons_killed) AS severity_score
    FROM collisions c
    LEFT JOIN boroughs b
        ON c.borough_id = b.borough_id
    WHERE c.crash_date >= DATE '2023-01-01'
      AND c.crash_date <  DATE '2025-01-01'
      AND c.on_street_name IS NOT NULL
),
street_agg AS (
    SELECT
        borough_name,
        on_street_name,
        COUNT(*) AS total_collisions,
        SUM(severity_score) AS total_severity
    FROM street_severity
    GROUP BY borough_name, on_street_name
),
ranked AS (
    SELECT
        borough_name,
        on_street_name,
        total_collisions,
        total_severity,
        RANK() OVER (
            PARTITION BY borough_name
            ORDER BY total_severity DESC, total_collisions DESC
        ) AS rnk
    FROM street_agg
)
SELECT
    borough_name,
    on_street_name,
    total_collisions,
    total_severity
FROM ranked
WHERE rnk <= 10
ORDER BY borough_name, total_severity DESC;


Q2 – Contributing factors with highest fatality rate

Business question:
Which contributing factors are most associated with fatal crashes?

Advanced features: multi-table joins, CTEs, complex aggregation, window function, percentage calc.

  

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
        SUM(CASE WHEN number_of_persons_killed > 0 THEN 1 ELSE 0 END) AS fatal_collisions
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


Q3 – Crash severity by primary vehicle type

Business question:
Which vehicle types are most likely to be involved in fatal or injury crashes?

Advanced features: 3-table join, CASE, aggregation, window function, filter on aggregate.



WITH vehicle_events AS (
    SELECT
        vt.vehicle_type_desc,
        c.number_of_persons_injured,
        c.number_of_persons_killed,
        CASE
            WHEN c.number_of_persons_killed > 0 THEN 'Fatal'
            WHEN c.number_of_persons_injured > 0 THEN 'Injury'
            ELSE 'Property Damage Only'
        END AS severity_category
    FROM collision_vehicles cv
    JOIN vehicle_types vt
        ON cv.vehicle_type_id = vt.vehicle_type_id
    JOIN collisions c
        ON cv.collision_id = c.collision_id
    WHERE cv.vehicle_order = 1              
      AND vt.vehicle_type_desc IS NOT NULL
),
vehicle_agg AS (
    SELECT
        vehicle_type_desc,
        COUNT(*) AS total_collisions,
        SUM(CASE WHEN severity_category = 'Fatal'  THEN 1 ELSE 0 END) AS fatal_collisions,
        SUM(CASE WHEN severity_category = 'Injury' THEN 1 ELSE 0 END) AS injury_collisions
    FROM vehicle_events
    GROUP BY vehicle_type_desc
),
vehicle_rates AS (
    SELECT
        vehicle_type_desc,
        total_collisions,
        fatal_collisions,
        injury_collisions,
        ROUND(fatal_collisions::numeric / NULLIF(total_collisions, 0) * 100, 2)
            AS fatal_rate_pct,
        ROUND(injury_collisions::numeric / NULLIF(total_collisions, 0) * 100, 2)
            AS injury_rate_pct
    FROM vehicle_agg
)
SELECT
    vehicle_type_desc,
    total_collisions,
    fatal_collisions,
    injury_collisions,
    fatal_rate_pct,
    injury_rate_pct,
    RANK() OVER (ORDER BY fatal_rate_pct DESC) AS fatality_rank
FROM vehicle_rates
WHERE total_collisions >= 50               
ORDER BY fatality_rank;
