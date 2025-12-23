"""
ETL Pipeline для Metropulse
Корректный Airflow + Spark пайплайн
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# -----------------------
# DEFAULT ARGS
# -----------------------
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": ["data-team@metropulse.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="metropulse_etl_pipeline",
    description="Полный ETL пайплайн Metropulse (Airflow + Spark + DWH)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "spark", "dwh"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------
    # 1. INIT DWH
    # -----------------------
    init_dwh = PostgresOperator(
        task_id="initialize_dwh_tables",
        postgres_conn_id="postgres_dwh",
        autocommit=True,
        sql="""
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
        """,
    )

    # -----------------------
    # 2. GENERATE DATA
    # -----------------------
    generate_test_data = SparkSubmitOperator(
        task_id="generate_test_data",
        application="/opt/spark-apps/spark/jobs/generate_data.py",
        conn_id="spark_default",
        name="generate_test_data",
        verbose=True,
    )

    # -----------------------
    # 3. DIMENSIONS (PARALLEL)
    # -----------------------
    load_dim_users = SparkSubmitOperator(
        task_id="process_and_load_dim_users",
        application="/opt/spark-apps/spark/jobs/load_dim_users.py",
        conn_id="spark_default",
        name="load_dim_users",
        verbose=True,
        packages="org.postgresql:postgresql:42.6.0",
    )

    load_dim_routes = SparkSubmitOperator(
        task_id="process_and_load_dim_routes",
        application="/opt/spark-apps/spark/jobs/load_dim_route_scd2.py",
        conn_id="spark_default",
        name="load_dim_routes",
        verbose=True,
        packages="org.postgresql:postgresql:42.6.0",
    )

    load_dim_vehicles = SparkSubmitOperator(
        task_id="process_and_load_dim_vehicles",
        application="/opt/spark-apps/spark/jobs/load_dim_vehicle.py",
        conn_id="spark_default",
        name="load_dim_vehicles",
        verbose=True,
        packages="org.postgresql:postgresql:42.6.0",
    )

    # -----------------------
    # 4. FACTS
    # -----------------------
    load_fact_rides = SparkSubmitOperator(
        task_id="process_and_load_fact_rides",
        application="/opt/spark-apps/spark/jobs/load_fact_rides.py",
        conn_id="spark_default",
        name="load_fact_rides",
        verbose=True,
        packages="org.postgresql:postgresql:42.6.0",
    )

    # -----------------------
    # 5. VALIDATION
    # -----------------------
    validate_dwh = PostgresOperator(
        task_id="validate_dwh_data",
        postgres_conn_id="postgres_dwh",
        do_xcom_push=True,
        sql="""
        SELECT 'dim_user' table_name, COUNT(*) FROM dim_user
        UNION ALL
        SELECT 'dim_route', COUNT(*) FROM dim_route
        UNION ALL
        SELECT 'dim_vehicle', COUNT(*) FROM dim_vehicle
        UNION ALL
        SELECT 'fact_rides', COUNT(*) FROM fact_rides;
        """,
    )

    # -----------------------
    # 6. DATA MART
    # -----------------------
    create_data_mart = PostgresOperator(
        task_id="create_daily_dashboard_mart",
        postgres_conn_id="postgres_dwh",
        autocommit=True,
        sql="""
        DROP TABLE IF EXISTS dashboard_daily_stats;
        CREATE TABLE dashboard_daily_stats AS
        SELECT
            d.full_date,
            r.route_number,
            v.vehicle_type,
            COUNT(f.ride_id) AS total_rides,
            SUM(f.fare_amount) AS total_revenue,
            AVG(f.ride_duration_sec) AS avg_duration_seconds,
            AVG(f.fare_amount) AS avg_fare
        FROM fact_rides f
        JOIN dim_route r ON f.route_sk = r.route_sk
        JOIN dim_vehicle v ON f.vehicle_sk = v.vehicle_sk
        WHERE r.is_current = TRUE
        GROUP BY d.full_date, r.route_number, v.vehicle_type;
        """,
    )

    end = EmptyOperator(task_id="end")

    # -----------------------
    # DEPENDENCIES
    # -----------------------
    start >> init_dwh >> generate_test_data

    generate_test_data >> [
        load_dim_users,
        load_dim_routes,
        load_dim_vehicles,
    ]

    [load_dim_users, load_dim_routes, load_dim_vehicles] >> load_fact_rides

    load_fact_rides >> validate_dwh >> create_data_mart >> end
