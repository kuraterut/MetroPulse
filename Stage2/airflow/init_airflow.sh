#!/bin/bash
echo " Инициализация Airflow..."

sleep 20

# Инициализируем Airflow базу данных
echo " Инициализируем базу данных Airflow..."
docker exec airflow-webserver airflow db init

# Создаем администратора
echo " Создаем администратора Airflow..."
docker exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@metropulse.com

# Устанавливаем connections
echo " Настраиваем connections..."
docker cp init_connections.py airflow-webserver:/tmp/init_connections.py
docker exec airflow-webserver python3 /tmp/init_connections.py

echo " Airflow инициализирован!"
echo ""
echo " Airflow UI: http://localhost:8080"
echo " Логин: admin"
echo " Пароль: admin"