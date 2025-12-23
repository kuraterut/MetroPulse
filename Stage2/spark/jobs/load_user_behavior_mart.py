from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, sum, avg, countDistinct,
    date_format, dayofweek, hour
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

DWH_JDBC_URL = "jdbc:postgresql://postgres-dwh:5432/metropulse_dwh"
DWH_PROPERTIES = {
    "user": "dwh",
    "password": "dwh",
    "driver": "org.postgresql.Driver"
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_PROPERTIES = {
    "user": "default",
    "password": "",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver"
}

def main():
    spark = SparkSession.builder \
        .appName("Load User Behavior Data Mart") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
        .getOrCreate()

    try:
        # Читаем данные из Core DWH
        dim_user = spark.read \
            .jdbc(url=DWH_JDBC_URL, table="dim_user", properties=DWH_PROPERTIES)

        dim_route = spark.read \
            .jdbc(url=DWH_JDBC_URL, table="dim_route", properties=DWH_PROPERTIES) \
            .filter(col("is_current") == True)

        dim_date = spark.read \
            .jdbc(url=DWH_JDBC_URL, table="dim_date", properties=DWH_PROPERTIES)

        fact_rides = spark.read \
            .jdbc(url=DWH_JDBC_URL, table="fact_rides", properties=DWH_PROPERTIES)

        fact_payments = spark.read \
            .jdbc(url=DWH_JDBC_URL, table="fact_payments", properties=DWH_PROPERTIES)

        rides_with_dims = fact_rides \
            .join(dim_user, fact_rides.user_sk == dim_user.user_sk, "inner") \
            .join(dim_route, fact_rides.route_sk == dim_route.route_sk, "inner") \
            .join(dim_date, fact_rides.date_sk == dim_date.date_sk, "inner") \
            .select(
                dim_date.full_date,
                hour(fact_rides.start_time).alias("hour_of_day"),
                dayofweek(fact_rides.start_time).alias("day_of_week"),
                dim_user.city,
                dim_user.user_id,
                dim_route.route_number,
                dim_route.vehicle_type,
                fact_rides.ride_id,
                fact_rides.ride_duration_sec,
                fact_rides.fare_amount
            )

        rides_agg = rides_with_dims \
            .groupBy(
                "full_date", "hour_of_day", "day_of_week",
                "city", "user_id", "route_number", "vehicle_type"
            ) \
            .agg(
                count("ride_id").alias("total_rides"),
                sum("fare_amount").alias("total_revenue"),
                avg("ride_duration_sec").cast("int").alias("avg_duration_seconds"),
                avg("fare_amount").alias("avg_fare")
            )

        payments_agg = fact_payments \
            .join(dim_user, fact_payments.user_sk == dim_user.user_sk, "inner") \
            .join(dim_route, fact_payments.user_sk == dim_user.user_sk, "inner") \
            .join(dim_date, fact_payments.date_sk == dim_date.date_sk, "inner") \
            .select(
                dim_date.full_date,
                hour(fact_payments.created_at).alias("hour_of_day"),
                dayofweek(fact_payments.created_at).alias("day_of_week"),
                dim_user.city,
                dim_user.user_id,
                dim_route.route_number,
                dim_route.vehicle_type,
                fact_payments.payment_method,
                fact_payments.status
            ) \
            .groupBy(
                "full_date", "hour_of_day", "day_of_week",
                "city", "user_id", "route_number", "vehicle_type", "payment_method"
            ) \
            .agg(
                count("*").alias("total_payments"),
                sum(when(col("status") == "success", 1).otherwise(0)).alias("successful_payments")
            )

        final_agg = rides_agg \
            .join(
                payments_agg,
                on=[
                    "full_date", "hour_of_day", "day_of_week",
                    "city", "user_id", "route_number", "vehicle_type"
                ],
                how="outer"
            ) \
            .fillna(0) \
            .select(
                col("full_date"),
                col("hour_of_day"),
                col("day_of_week"),
                col("city"),
                col("user_id"),
                col("route_number"),
                col("vehicle_type"),
                col("payment_method"),
                col("total_rides"),
                col("total_revenue"),
                col("avg_duration_seconds"),
                col("avg_fare"),
                col("total_payments"),
                col("successful_payments")
            )

        final_agg.write \
            .format("jdbc") \
            .option("url", CLICKHOUSE_URL) \
            .option("dbtable", "dm_user_behavior_hourly") \
            .option("user", CLICKHOUSE_PROPERTIES["user"]) \
            .option("password", CLICKHOUSE_PROPERTIES["password"]) \
            .option("driver", CLICKHOUSE_PROPERTIES["driver"]) \
            .mode("overwrite") \
            .save()

        print("Витрина dm_user_behavior_hourly успешно загружена в ClickHouse")

    except Exception as e:
        print(f"Ошибка при загрузке витрины: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()