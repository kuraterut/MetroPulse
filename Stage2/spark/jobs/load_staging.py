from utils.spark_session import get_spark

DWH_URL = "jdbc:postgresql://postgres-dwh:5432/metropulse_dwh"
PROPS = {"user": "dwh", "password": "dwh", "driver": "org.postgresql.Driver"}

spark = get_spark()

TABLES = ["users", "routes", "vehicles", "rides", "payments"]

for t in TABLES:
    df = spark.read.parquet(f"s3a://raw/{t}/")
    df.write.jdbc(DWH_URL, f"stg_{t}", mode="append", properties=PROPS)