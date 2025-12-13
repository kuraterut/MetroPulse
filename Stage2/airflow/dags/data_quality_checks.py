"""
Data Quality Checks для DWH
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'data_quality',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1
}

with DAG(
        dag_id='data_quality_checks',
        default_args=default_args,
        schedule_interval='0 3 * * *',  # После основного ETL
        catchup=False,
        tags=['data-quality']
) as dag:
    check_null_values = PostgresOperator(
        task_id='check_null_values',
        postgres_conn_id='postgres_dwh',
        sql='''
            SELECT 
                'fact_rides' as table_name,
                COUNT(*) as null_count,
                'user_sk is null' as check_type
            FROM fact_rides 
            WHERE user_sk IS NULL
            UNION ALL
            SELECT 
                'fact_rides',
                COUNT(*),
                'route_sk is null'
            FROM fact_rides 
            WHERE route_sk IS NULL
            UNION ALL
            SELECT 
                'dim_user',
                COUNT(*),
                'user_id is null'
            FROM dim_user 
            WHERE user_id IS NULL;
        ''',
        do_xcom_push=True
    )

    check_data_freshness = PostgresOperator(
        task_id='check_data_freshness',
        postgres_conn_id='postgres_dwh',
        sql='''
            SELECT 
                'fact_rides' as table_name,
                MAX(start_time) as latest_record,
                NOW() - MAX(start_time) as data_lag
            FROM fact_rides;
        ''',
        do_xcom_push=True
    )

    check_duplicates = PostgresOperator(
        task_id='check_duplicates',
        postgres_conn_id='postgres_dwh',
        sql='''
            SELECT 
                'dim_user' as table_name,
                COUNT(*) - COUNT(DISTINCT user_id) as duplicate_count
            FROM dim_user
            UNION ALL
            SELECT 
                'fact_rides',
                COUNT(*) - COUNT(DISTINCT ride_id)
            FROM fact_rides;
        ''',
        do_xcom_push=True
    )

    generate_quality_report = PostgresOperator(
        task_id='generate_quality_report',
        postgres_conn_id='postgres_dwh',
        sql='''
            CREATE TABLE IF NOT EXISTS data_quality_log (
                check_date TIMESTAMP DEFAULT NOW(),
                table_name VARCHAR(100),
                check_type VARCHAR(100),
                result_value VARCHAR(100),
                status VARCHAR(20)
            );

            -- Здесь можно добавить логику сохранения результатов
        '''
    )

    send_alert_if_problems = EmailOperator(
        task_id='send_alert_if_problems',
        to='data-team@metropulse.com',
        subject='Data Quality Alert - {{ ds }}',
        html_content='''
            <h3>Data Quality Report for {{ ds }}</h3>
            <p>Please check the data quality checks in Airflow.</p>
            <p>DAG: data_quality_checks</p>
        ''',
        trigger_rule='one_failed'  # Отправляем email если любой таск упал
    )

    [check_null_values, check_data_freshness, check_duplicates] >> generate_quality_report
    generate_quality_report >> send_alert_if_problems