CREATE TABLE dm_user_behavior_hourly
(
    full_date Date,
    hour_of_day UInt8, 
    day_of_week UInt8,

    city LowCardinality(String),
    user_id UInt32,

    route_number LowCardinality(String),
    vehicle_type LowCardinality(String),

    payment_method LowCardinality(String),

    total_rides UInt32,
    total_revenue Decimal(10,2),
    avg_duration_seconds UInt32,
    avg_fare Decimal(10,2),

    total_payments UInt32,
    successful_payments UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(full_date)
ORDER BY (full_date, hour_of_day, city, route_number, payment_method)
SETTINGS index_granularity = 8192;
