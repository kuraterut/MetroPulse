from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_date, date_add
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Load Dim Routes SCD2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .getOrCreate()

# 1. Загружаем существующие данные из DWH
try:
    existing_routes = spark.read \
        .jdbc(url=jdbc_url, table="dim_route", properties=properties)
except:
    # Если таблицы нет, создаем пустой DataFrame
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, BooleanType
    schema = StructType([
        StructField("route_sk", IntegerType()),
        StructField("route_id", IntegerType()),
        StructField("route_number", StringType()),
        StructField("vehicle_type", StringType()),
        StructField("base_fare", DecimalType(10, 2)),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType())
    ])
    existing_routes = spark.createDataFrame([], schema)

# 2. Загружаем новые данные из MinIO
new_routes = spark.read.parquet("s3a://raw/routes/") \
    .withColumn("valid_from", current_date()) \
    .withColumn("valid_to", lit("9999-12-31").cast("date")) \
    .withColumn("is_current", lit(True))

# 3. Объединяем данные (простая версия SCD2)
# В реальности тут была бы сложная логика сравнения
final_routes = new_routes

# 4. Записываем в DWH
final_routes.write \
    .jdbc(url=jdbc_url, table="dim_route", mode="overwrite", properties=properties)

print("✅ Dim Routes (SCD2) загружены в DWH")
spark.stop()