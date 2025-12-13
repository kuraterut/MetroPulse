#!/usr/bin/env python3
import os
from airflow.models import Connection
from airflow.settings import Session

# Создаем connection для DWH
dwh_conn = Connection(
    conn_id='postgres_dwh',
    conn_type='postgres',
    host='postgres-dwh',
    login='dwh',
    password='dwh',
    port=5432,
    schema='metropulse_dwh'
)

# Создаем connection для OLTP
oltp_conn = Connection(
    conn_id='postgres_oltp',
    conn_type='postgres',
    host='postgres-oltp',
    login='metropulse',
    password='metropulse',
    port=5432,
    schema='metropulse_db'
)

# Добавляем connections в базу Airflow
session = Session()
session.add(dwh_conn)
session.add(oltp_conn)
session.commit()