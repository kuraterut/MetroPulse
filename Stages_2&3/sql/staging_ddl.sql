CREATE TABLE stg_users (
    user_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    created_at TIMESTAMP,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE stg_routes (
    route_id INT,
    route_number VARCHAR,
    vehicle_type VARCHAR,
    base_fare DECIMAL(10,2),
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE stg_vehicles (
    vehicle_id INT,
    route_id INT,
    license_plate VARCHAR,
    capacity INT,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE stg_rides (
    ride_id UUID,
    user_id INT,
    route_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    fare_amount DECIMAL(10,2),
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE stg_payments (
    payment_id UUID,
    ride_id UUID,
    user_id INT,
    amount DECIMAL(10,2),
    payment_method VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE stg_vehicle_positions (
    event_id UUID,
    vehicle_id INT,
    route_number VARCHAR,
    event_time TIMESTAMP,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    speed_kmh DECIMAL(5,2),
    passengers_estimated INT,
    load_ts TIMESTAMP DEFAULT now()
);
