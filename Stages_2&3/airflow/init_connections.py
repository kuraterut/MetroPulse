from airflow.models.connection import Connection
from airflow import settings
from airflow.utils.db import create_session
import json


def setup_connections():
    # Правильные параметры из docker-compose.yml
    connections = [
        {
            'conn_id': 'postgres_oltp',
            'conn_type': 'postgres',
            'host': 'postgres-oltp',
            'login': 'metropulse',
            'password': 'metropulse',
            'port': 5432,
            'schema': 'metropulse_db'
        },
        {
            'conn_id': 'postgres_dwh',
            'conn_type': 'postgres',
            'host': 'postgres-dwh',
            'login': 'dwh',
            'password': 'dwh',
            'port': 5432,
            'schema': 'metropulse_dwh'
        },
        {
            'conn_id': 'spark_default',
            'conn_type': 'spark',
            'host': 'spark://spark',
            'port': 7077
        },
        {
            'conn_id': 'minio_default',
            'conn_type': 's3',
            'host': 'minio',
            'port': 9000,
            'login': 'minio',
            'password': 'minio123',
            'extra': json.dumps({'endpoint_url': 'http://minio:9000'})
        }
    ]

    with create_session() as session:
        for conn_config in connections:
            existing = session.query(Connection).filter(
                Connection.conn_id == conn_config['conn_id']
            ).first()

            if existing:
                print(f"Connection {conn_config['conn_id']} уже существует, обновляем...")
                for key, value in conn_config.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
            else:
                print(f"Создаем connection {conn_config['conn_id']}...")
                conn = Connection(**conn_config)
                session.add(conn)

        session.commit()

    print("Все connections успешно настроены!")
    print("Проверьте connections в Airflow UI: Admin -> Connections")


if __name__ == "__main__":
    setup_connections()
