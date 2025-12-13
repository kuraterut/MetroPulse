ETL-пайплайн для сбора, обработки и анализа данных. Система имитирует работу общественного транспорта с генерацией тестовых данных, их обработкой в хранилище 
данных и визуализацией через Apache Superset.

![img.png](img.png)

![img_1.png](img_1.png)

![img_2.png](img_2.png)

![img_3.png](img_3.png)

### Архитектура проекта

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│    ETL Layer    │───▶│   Data Warehouse│
│                 │    │                 │    │                 │
│  • Тестовые     │    │  • Apache Spark │    │  • PostgreSQL   │
│    данные       │    │  • Airflow DAGs │    │    DWH          │
│  • MinIO (S3)   │    │  • Python jobs  │    │  • Dimensional  │
│                 │    │                 │    │    models       │
└─────────────────┘    └─────────────────┘    └────────┬────────┘
                                                       │
                                                ┌──────▼────────┐
                                                │  BI & Analytics│
                                                │               │
                                                │  • Superset   │
                                                │  • Dashboards │
                                                └───────────────┘
```

**весь пайплайн запускается одной командой:**
```bash
chmod +x run_etl.sh
./run_etl.sh
```


Скрипт автоматически:
-  Поднимет все Docker-контейнеры
-  Подождет инициализации сервисов
-  Настроит Airflow и соединения
-  Сгенерирует тестовые данные
-  Запустит ETL-пайплайн

Предварительно один раз нужно создать таблицы в базе данных с помощью

```bash
chmod +x ddl_run.sh
./ddl_run.sh
```
**интерфейсы:**
   - **Airflow UI**: http://localhost:8080 (admin/admin)
   - **MinIO Console**: http://localhost:9000 (minio/minio123)

## Как работает ETL-пайплайн

### 1. Генерация данных

### 2. Bronze → Silver (сырые → очищенные данные)

### 3. Silver → Gold (очищенные → аналитические данные)

### 4. DAG Airflow

## Ручной запуск компонентов

### Запуск только Docker-контейнеров:
```bash
docker-compose up -d
```

### Просмотр логов конкретного сервиса:
```bash
docker-compose logs -f airflow-webserver
docker-compose logs -f spark
docker-compose logs -f postgres-dwh
```

### Запуск отдельных Spark job:
```bash
docker exec -it spark python /opt/spark/jobs/generate_data.py
docker exec -it spark python /opt/spark/jobs/bronze_to_silver.py
```

### Ручное управление Airflow:
```bash
# Включить DAG
docker exec -it airflow-webserver airflow dags unpause metro_pulse_etl

# Запустить DAG
docker exec -it airflow-webserver airflow dags trigger metro_pulse_etl

# Просмотр статуса задач
docker exec -it airflow-webserver airflow tasks list metro_pulse_etl
```
