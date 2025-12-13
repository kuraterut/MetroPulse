from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Конфигурация (такая же как выше)

spark = SparkSession.builder \
    .appName("Load Dim Vehicles") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Загружаем сырые данные
vehicles_df = spark.read.parquet("s3a://raw/vehicles/")

# Преобразуем в dim_vehicle (убираем route_id, т.к. это будет в факте)
dim_vehicles = vehicles_df.select(
    col("vehicle_id"),
    col("license_plate"),
    col("capacity")
)

# Записываем в DWH
dim_vehicles.write \
    .jdbc(url=jdbc_url, table="dim_vehicle", mode="overwrite", properties=properties)

print("✅ Dim Vehicles загружены в DWH")
spark.stop()