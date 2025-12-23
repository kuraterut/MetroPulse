# utils/spark_session.py
import os
import pyarrow.parquet as pq
import s3fs
from pyspark.sql import SparkSession

# ------------------------------
# SparkSession
# ------------------------------
def get_spark(app_name: str = "SparkApp") -> SparkSession:
    """
    Создает SparkSession с настройками для работы с MinIO через S3A.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Добавляем настройки для legacy Parquet
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        # Добавляем JDBC драйвер
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
    )
    return spark

# ------------------------------
# Чтение из S3/MinIO через PyArrow
# ------------------------------
def read_from_s3(spark, bucket_name: str, path: str, file_format: str = "parquet"):
    """
    Читает данные из MinIO через PyArrow (возвращает Pandas DataFrame).
    """
    # Устанавливаем креды для s3fs / boto
    os.environ["AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    fs = s3fs.S3FileSystem(
        key="minio",
        secret="minio123",
        client_kwargs={"endpoint_url": "http://minio:9000"}
    )

    s3_path = f"{bucket_name}/{path}"

    if file_format == "parquet":
        dataset = pq.ParquetDataset(s3_path, filesystem=fs)
        table = dataset.read()
        return table.to_pandas()

    raise ValueError(f"Unsupported format: {file_format}")

# ------------------------------
# Запись в PostgreSQL
# ------------------------------
def write_to_postgres(df, table_name: str, mode: str = "append", config_type: str = "dwh"):
    """
    Записывает Pandas DataFrame или Spark DataFrame в PostgreSQL.
    config_type: "dwh" или "oltp"
    """
    import psycopg2
    from sqlalchemy import create_engine

    if config_type == "dwh":
        user, password, host, port, db = "dwh", "dwh", "postgres-dwh", 5432, "metropulse_dwh"
    elif config_type == "oltp":
        user, password, host, port, db = "metropulse", "metropulse", "postgres-oltp", 5432, "metropulse_db"
    else:
        raise ValueError(f"Unknown config_type: {config_type}")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    if hasattr(df, "to_sql"):
        # Pandas DataFrame
        df.to_sql(table_name, engine, if_exists=mode, index=False)
    else:
        # Spark DataFrame
        df.write \
          .format("jdbc") \
          .option("url", f"jdbc:postgresql://{host}:{port}/{db}") \
          .option("dbtable", table_name) \
          .option("user", user) \
          .option("password", password) \
          .mode(mode) \
          .save()
