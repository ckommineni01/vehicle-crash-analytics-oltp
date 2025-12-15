-- Drop existing tables
DROP TABLE IF EXISTS collision_factors;
DROP TABLE IF EXISTS collision_vehicles;
DROP TABLE IF EXISTS factors;
DROP TABLE IF EXISTS vehicle_types;
DROP TABLE IF EXISTS collisions;
DROP TABLE IF EXISTS boroughs;

-- Borough lookup table
CREATE TABLE boroughs (
    borough_id SERIAL PRIMARY KEY,
    borough_name VARCHAR(50) NOT NULL UNIQUE
);

-- Main collisions table
CREATE TABLE collisions (
    collision_id BIGINT PRIMARY KEY,
    crash_date DATE,
    crash_time TIME,
    borough_id INTEGER REFERENCES boroughs(borough_id),
    zip_code VARCHAR(10),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    location TEXT,
    on_street_name TEXT,
    off_street_name TEXT,
    cross_street_name TEXT,
    number_of_persons_injured INTEGER CHECK (number_of_persons_injured >= 0),
    number_of_persons_killed INTEGER CHECK (number_of_persons_killed >= 0),
    number_of_pedestrians_injured INTEGER CHECK (number_of_pedestrians_injured >= 0),
    number_of_pedestrians_killed INTEGER CHECK (number_of_pedestrians_killed >= 0),
    number_of_cyclist_injured INTEGER CHECK (number_of_cyclist_injured >= 0),
    number_of_cyclist_killed INTEGER CHECK (number_of_cyclist_killed >= 0),
    number_of_motorist_injured INTEGER CHECK (number_of_motorist_injured >= 0),
    number_of_motorist_killed INTEGER CHECK (number_of_motorist_killed >= 0)
);

-- Vehicle types
CREATE TABLE vehicle_types (
    vehicle_type_id SERIAL PRIMARY KEY,
    vehicle_type_desc TEXT NOT NULL UNIQUE
);

-- Vehicles involved in each collision
CREATE TABLE collision_vehicles (
    collision_id BIGINT NOT NULL REFERENCES collisions(collision_id) ON DELETE CASCADE,
    vehicle_order INTEGER NOT NULL CHECK (vehicle_order BETWEEN 1 AND 5),
    vehicle_type_id INTEGER NOT NULL REFERENCES vehicle_types(vehicle_type_id),
    PRIMARY KEY (collision_id, vehicle_order)
);

-- Contributing factors
CREATE TABLE factors (
    factor_id SERIAL PRIMARY KEY,
    factor_desc TEXT NOT NULL UNIQUE
);

-- Factors involved in each collision
CREATE TABLE collision_factors (
    collision_id BIGINT NOT NULL REFERENCES collisions(collision_id) ON DELETE CASCADE,
    factor_order INTEGER NOT NULL CHECK (factor_order BETWEEN 1 AND 5),
    factor_id INTEGER NOT NULL REFERENCES factors(factor_id),
    PRIMARY KEY (collision_id, factor_order)
);
