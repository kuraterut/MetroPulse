from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import os

# Конфигурация
jdbc_url = "jdbc:postgresql://postgres-dwh:5432/metropulse_dwh"
properties = {
    "user": "dwh",
    "password": "dwh",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Load Dim Users") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Загружаем обработанные данные из MinIO
users_df = spark.read.parquet("s3a://processed/dim_users/")

# Выбираем нужные колонки (соответствие с таблицей dim_user)
dim_users = users_df.select(
    col("user_id"),
    col("name"),
    col("email"),
    col("city"),
    col("created_at")
)

# Записываем в DWH
dim_users.write \
    .jdbc(url=jdbc_url, table="dim_user", mode="overwrite", properties=properties)

print("✅ Dim Users загружены в DWH")
spark.stop()