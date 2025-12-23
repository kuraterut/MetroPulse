from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data-team@metropulse.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'execution_timeout': timedelta(hours=1)
}

with DAG(
    dag_id='user_behavior_mart_dag',
    default_args=default_args,
    description='Построение витрины dm_user_behavior_hourly для BI',
    schedule_interval='0 3 * * *', 
    catchup=False,
    tags=['data-mart', 'clickhouse', 'bi']
) as dag:

    start = DummyOperator(task_id='start')

    load_user_behavior_mart = BashOperator(
        task_id='load_user_behavior_mart',
        bash_command='''
            cd /opt/spark-apps &&
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio \
            --conf spark.hadoop.fs.s3a.secret.key=minio123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --packages org.postgresql:postgresql:42.6.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 \
            spark/jobs/load_user_behavior_mart.py
        ''',
        env={
            'AWS_ACCESS_KEY_ID': 'minio',
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
    )

    end = DummyOperator(task_id='end')

    start >> load_user_behavior_mart >> end