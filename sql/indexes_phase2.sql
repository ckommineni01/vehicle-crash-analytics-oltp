CREATE INDEX idx_collision_factors_factor
    ON collision_factors (factor_id);

CREATE INDEX idx_collision_factors_collision
    ON collision_factors (collision_id);

CREATE INDEX idx_collisions_date_borough
    ON collisions (crash_date, borough_id);

