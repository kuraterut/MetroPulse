CREATE TABLE dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE dim_route (
    route_sk SERIAL PRIMARY KEY,
    route_id INT,
    route_number VARCHAR,
    vehicle_type VARCHAR,
    base_fare DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE TABLE dim_vehicle (
    vehicle_sk SERIAL PRIMARY KEY,
    vehicle_id INT,
    license_plate VARCHAR,
    capacity INT
);

CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT
);
