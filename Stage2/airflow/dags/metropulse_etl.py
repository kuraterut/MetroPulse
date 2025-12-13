"""
ETL Pipeline для Metropulse
Полный пайплайн от генерации данных до загрузки в DWH
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data-team@metropulse.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'execution_timeout': timedelta(hours=2)
}

with DAG(
        dag_id='metropulse_etl_pipeline',
        default_args=default_args,
        description='Полный ETL пайплайн Metropulse',
        schedule_interval='0 2 * * *',  # Запуск каждый день в 2:00
        catchup=False,
        tags=['etl', 'spark', 'dwh']
) as dag:
    start = DummyOperator(task_id='start')

    # 1. Инициализация DWH (если нужно)
    init_dwh = PostgresOperator(
        task_id='initialize_dwh_tables',
        postgres_conn_id='postgres_dwh',
        sql='''
            -- Проверяем и создаем таблицы если их нет
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
        ''',
        autocommit=True
    )

    # 2. Генерация тестовых данных
    generate_data = BashOperator(
        task_id='generate_test_data',
        bash_command='cd /opt/spark-apps && python3 data_generator/generate_all.py',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    # 3. Обработка и загрузка измерений (можно выполнять параллельно)
    process_dim_users = BashOperator(
        task_id='process_and_load_dim_users',
        bash_command='''
            cd /opt/spark-apps && \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio \
            --conf spark.hadoop.fs.s3a.secret.key=minio123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --packages org.postgresql:postgresql:42.6.0 \
            spark/jobs/load_dim_users.py
        ''',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    process_dim_routes = BashOperator(
        task_id='process_and_load_dim_routes',
        bash_command='''
            cd /opt/spark-apps && \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio \
            --conf spark.hadoop.fs.s3a.secret.key=minio123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --packages org.postgresql:postgresql:42.6.0 \
            spark/jobs/load_dim_route_scd2.py
        ''',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    process_dim_vehicles = BashOperator(
        task_id='process_and_load_dim_vehicles',
        bash_command='''
            cd /opt/spark-apps && \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio \
            --conf spark.hadoop.fs.s3a.secret.key=minio123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --packages org.postgresql:postgresql:42.6.0 \
            spark/jobs/load_dim_vehicle.py
        ''',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    # 4. Обработка фактов (зависит от измерений)
    process_fact_rides = BashOperator(
        task_id='process_and_load_fact_rides',
        bash_command='''
            cd /opt/spark-apps && \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio \
            --conf spark.hadoop.fs.s3a.secret.key=minio123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --packages org.postgresql:postgresql:42.6.0 \
            spark/jobs/load_fact_rides.py
        ''',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    # 5. Валидация данных
    validate_data = PostgresOperator(
        task_id='validate_dwh_data',
        postgres_conn_id='postgres_dwh',
        sql='''
            -- Проверяем, что данные загружены
            SELECT 
                'dim_user' as table_name, 
                COUNT(*) as row_count 
            FROM dim_user
            UNION ALL
            SELECT 
                'dim_route', 
                COUNT(*) 
            FROM dim_route
            UNION ALL
            SELECT 
                'dim_vehicle', 
                COUNT(*) 
            FROM dim_vehicle
            UNION ALL
            SELECT 
                'fact_rides', 
                COUNT(*) 
            FROM fact_rides;
        ''',
        do_xcom_push=True  # Результат будет доступен для следующих тасков
    )

    # 6. Создание витрины данных
    create_data_mart = PostgresOperator(
        task_id='create_daily_dashboard_mart',
        postgres_conn_id='postgres_dwh',
        sql='''
            -- Витрина для дашборда
            DROP TABLE IF EXISTS dashboard_daily_stats;
            CREATE TABLE dashboard_daily_stats AS
            SELECT 
                d.full_date,
                r.route_number,
                v.vehicle_type,
                COUNT(f.ride_id) as total_rides,
                SUM(f.fare_amount) as total_revenue,
                AVG(f.ride_duration_sec) as avg_duration_seconds,
                AVG(f.fare_amount) as avg_fare
            FROM fact_rides f
            JOIN dim_route r ON f.route_sk = r.route_sk
            JOIN dim_vehicle v ON f.vehicle_sk = v.vehicle_sk
            JOIN dim_date d ON f.date_sk = d.date_sk
            WHERE r.is_current = true
            GROUP BY d.full_date, r.route_number, v.vehicle_type
            ORDER BY d.full_date DESC, total_rides DESC;

            -- Витрина для анализа пользователей
            DROP TABLE IF EXISTS user_behavior_mart;
            CREATE TABLE user_behavior_mart AS
            SELECT 
                u.user_id,
                u.city,
                COUNT(f.ride_id) as total_rides,
                SUM(f.fare_amount) as total_spent,
                AVG(f.ride_duration_sec) as avg_ride_duration
            FROM fact_rides f
            JOIN dim_user u ON f.user_sk = u.user_sk
            GROUP BY u.user_id, u.city;
        ''',
        autocommit=True
    )

    end = DummyOperator(task_id='end')

    # Определяем зависимости
    start >> init_dwh >> generate_data

    # Параллельная обработка измерений
    generate_data >> [process_dim_users, process_dim_routes, process_dim_vehicles]

    # Обработка фактов после всех измерений
    [process_dim_users, process_dim_routes, process_dim_vehicles] >> process_fact_rides

    # Валидация и создание витрин
    process_fact_rides >> validate_data >> create_data_mart >> end