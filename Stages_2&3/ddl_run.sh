#!/bin/bash
echo " Инициализация DWH..."

# 1. Создание таблиц измерений
echo " Создание таблиц измерений..."
docker exec -i postgres-dwh psql -U dwh -d metropulse_dwh << 'EOF'
-- dim таблицы
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_route (
    route_sk SERIAL PRIMARY KEY,
    route_id INT,
    route_number VARCHAR,
    vehicle_type VARCHAR,
    base_fare DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_sk SERIAL PRIMARY KEY,
    vehicle_id INT,
    license_plate VARCHAR,
    capacity INT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT
);
EOF

echo " Создание таблиц фактов..."
docker exec -i postgres-dwh psql -U dwh -d metropulse_dwh << 'EOF'
CREATE TABLE IF NOT EXISTS fact_rides (
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

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id UUID PRIMARY KEY,
    ride_id UUID,
    user_sk INT,
    amount DECIMAL(10,2),
    payment_method VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    date_sk INT
);

CREATE TABLE IF NOT EXISTS fact_vehicle_movement (
    route_sk INT,
    date_sk INT,
    avg_speed DECIMAL(5,2),
    avg_passengers INT,
    events_count INT
);
EOF

echo " Создание staging таблиц..."
docker exec -i postgres-dwh psql -U dwh -d metropulse_dwh << 'EOF'
CREATE TABLE IF NOT EXISTS stg_users (
    user_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    created_at TIMESTAMP,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_routes (
    route_id INT,
    route_number VARCHAR,
    vehicle_type VARCHAR,
    base_fare DECIMAL(10,2),
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_vehicles (
    vehicle_id INT,
    route_id INT,
    license_plate VARCHAR,
    capacity INT,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_rides (
    ride_id UUID,
    user_id INT,
    route_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    fare_amount DECIMAL(10,2),
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id UUID,
    ride_id UUID,
    user_id INT,
    amount DECIMAL(10,2),
    payment_method VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    load_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_vehicle_positions (
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
EOF

# 4. Проверка
echo " Проверка созданных таблиц..."
docker exec postgres-dwh psql -U dwh -d metropulse_dwh -c "
SELECT
    table_name,
    CASE
        WHEN table_name LIKE 'dim_%' THEN 'Измерение'
        WHEN table_name LIKE 'fact_%' THEN 'Факт'
        WHEN table_name LIKE 'stg_%' THEN 'Staging'
        ELSE 'Другое'
    END as table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_type, table_name;
"

echo " DWH инициализирован!"