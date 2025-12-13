#!/bin/bash

set -e

echo "Запуск полного ETL пайплайна с Airflow..."

echo "Проверяем структуру проекта..."
if [ ! -d "airflow" ] || [ ! -f "airflow/init_connections.py" ]; then
    echo "Ошибка: проверьте папку airflow и файл init_connections.py"
    exit 1
fi

echo "Останавливаем старые контейнеры..."
docker-compose down -v 2>/dev/null || true

echo "Запускаем PostgreSQL и Redis..."
docker-compose up -d postgres-airflow redis

echo "Ждем готовности PostgreSQL..."
for i in {1..12}; do
    if docker-compose exec -T postgres-airflow pg_isready -U airflow 2>/dev/null; then
        echo "PostgreSQL готов!"
        break
    fi
    echo "Ожидание... ($i/12)"
    sleep 5
done

echo "Запускаем Airflow сервисы..."
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

echo "Ждем запуска Airflow..."

if [ -f "airflow/init_airflow.sh" ]; then
    bash airflow/init_airflow.sh
else
    echo "Выполняем базовую инициализацию..."
    docker exec airflow-webserver airflow db init 2>/dev/null || echo "База данных уже инициализирована"

    docker exec airflow-webserver airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@metropulse.com 2>/dev/null || echo "Пользователь уже существует"

    docker cp airflow/init_connections.py airflow-webserver:/tmp/init_connections.py
    docker exec airflow-webserver python /tmp/init_connections.py
fi

echo "Проверяем готовность Airflow UI..."
for i in {1..30}; do
    if curl -s -f http://localhost:8080/health 2>/dev/null | grep -q '"healthy"'; then
        echo "Airflow UI готов!"
        break
    fi

    if [ $i -eq 15 ]; then
        docker-compose logs --tail=10 airflow-webserver
    fi

    echo "Ожидание... ($i/30)"
    sleep 5
done

echo "Запускаем остальные сервисы..."
docker-compose up -d postgres-oltp postgres-dwh minio spark

sleep 15

echo "Генерируем тестовые данные..."

if docker-compose ps | grep -q "spark.*Up"; then
    echo "Проверяем доступность Spark..."

    if docker exec -i spark ls /opt/spark/jobs/generate_data.py 2>/dev/null; then
        echo "Запускаем генерацию данных..."

        if docker exec -i spark which python3 2>/dev/null; then
            docker exec -i spark python3 /opt/spark/jobs/generate_data.py || echo "Ошибка генерации данных с python3"
        elif docker exec -i spark which python 2>/dev/null; then
            docker exec -i spark python /opt/spark/jobs/generate_data.py || echo "Ошибка генерации данных с python"
        else
            echo "Python не найден в Spark контейнере"
        fi
    else
        echo "Файл generate_data.py не найден в Spark контейнере"
    fi
else
    echo "Spark не запущен"
fi

echo "Настраиваем DAG..."
sleep 10

echo "Очищаем предыдущие запуски DAG..."
#docker exec airflow-webserver airflow dags delete metropulse_etl_pipeline --yes 2>/dev/null || echo "DAG не найден или не может быть удален"

docker exec -i airflow-webserver airflow dags list | grep metropulse_etl_pipeline && {
    echo "DAG найден, включаем..."
    docker exec -i airflow-webserver airflow dags unpause metropulse_etl_pipeline
    echo "Запускаем DAG..."
    docker exec -i airflow-webserver airflow dags trigger metropulse_etl_pipeline -e $(date +%Y-%m-%d)
} || {
    echo "DAG не найден в списке"
    docker exec -i airflow-webserver airflow dags list
}

echo ""
echo "=========================================="
echo "Система запущена!"
echo ""
echo "Доступные сервисы:"
echo "Airflow UI:    http://localhost:8080"
echo "Логин: admin, Пароль: admin"
echo ""
echo "MinIO Console: http://localhost:9000"
echo "Логин: minioadmin, Пароль: minioadmin"
echo ""
echo "PostgreSQL OLTP: localhost:5432"
echo "PostgreSQL DWH:  localhost:5433"
echo "=========================================="