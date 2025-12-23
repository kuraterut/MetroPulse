CREATE TABLE fact_rides (
    ride_id UUID PRIMARY KEY,
    user_sk INT,
    route_sk INT,
    vehicle_sk INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    ride_duration_sec INT,
    fare_amount DECIMAL(10,2),
    date_sk INT
);

CREATE TABLE fact_payments (
    payment_id UUID PRIMARY KEY,
    ride_id UUID,
    user_sk INT,
    amount DECIMAL(10,2),
    payment_method VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    date_sk INT
);

CREATE TABLE fact_vehicle_movement (
    route_sk INT,
    date_sk INT,
    avg_speed DECIMAL(5,2),
    avg_passengers INT,
    events_count INT
);
