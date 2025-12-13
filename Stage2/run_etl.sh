#!/bin/bash
set -e

echo "Запуск полного ETL пайплайна с Airflow..."

echo "Поднимаем Docker-контейнеры..."
docker-compose up -d

echo "надо подождать, airflow долго поднимается"
sleep 200

echo "Инициализируем Airflow..."
chmod +x airflow/init_airflow.sh
./airflow/init_airflow.sh

echo "Генерируем тестовые данные..."
docker exec spark python3 /opt/spark-apps/data_generator/generate_all.py

echo "Запускаем ETL пайплайн в Airflow..."
docker exec airflow-webserver airflow dags unpause metropulse_etl_pipeline
docker exec airflow-webserver airflow dags trigger metropulse_etl_pipeline

echo ""
echo "================================================"
echo " Все сервисы запущены!"
echo ""
echo " Airflow UI:    http://localhost:8080"
echo " MinIO Console: http://localhost:9001"
echo " Spark UI:      http://localhost:4040"
echo ""
echo " Для проверки данных в DWH:"
echo "docker exec postgres-dwh psql -U dwh -d metropulse_dwh -c \"SELECT table_name, COUNT(*) FROM information_schema.tables WHERE table_schema='public' GROUP BY table_name;\""
echo "================================================"