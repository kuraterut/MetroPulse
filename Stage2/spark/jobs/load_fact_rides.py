from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, expr, date_format

spark = SparkSession.builder \
    .appName("Load Fact Rides") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .getOrCreate()

# Загружаем данные
rides_df = spark.read.parquet("s3a://raw/rides/")

# Загружаем dimension tables для получения surrogate keys
dim_users = spark.read.jdbc(url=jdbc_url, table="dim_user", properties=properties)
dim_routes = spark.read.jdbc(url=jdbc_url, table="dim_route", properties=properties) \
    .filter(col("is_current") == True)
dim_vehicles = spark.read.jdbc(url=jdbc_url, table="dim_vehicle", properties=properties)

# Вычисляем длительность поездки
rides_with_duration = rides_df.withColumn(
    "ride_duration_sec",
    unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))
)

# Подготавливаем dim_date (упрощенная версия)
# В реальности была бы таблица dim_date
rides_with_date = rides_with_duration.withColumn(
    "date_sk",
    date_format(col("start_time"), "yyyyMMdd").cast("int")
)

# JOIN с dimension tables для получения surrogate keys
fact = rides_with_date \
    .join(dim_users, rides_with_date.user_id == dim_users.user_id, "left") \
    .join(dim_routes, rides_with_date.route_id == dim_routes.route_id, "left") \
    .join(dim_vehicles, rides_with_date.vehicle_id == dim_vehicles.vehicle_id, "left") \
    .select(
        col("ride_id"),
        col("user_sk"),
        col("route_sk"),
        col("vehicle_sk"),
        col("start_time"),
        col("end_time"),
        col("ride_duration_sec"),
        col("fare_amount"),
        col("date_sk")
    )

# Записываем в DWH
fact.write \
    .jdbc(url=jdbc_url, table="fact_rides", mode="overwrite", properties=properties)

print(f"✅ Fact Rides загружены: {fact.count()} записей")
spark.stop()