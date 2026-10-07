from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session
spark = (
    SparkSession.builder
    .appName("GoldDimUser")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.user"
TARGET_TABLE = "lakehouse.gold.dim_user"

print("=== Reading Silver user table ===")
df = spark.table(SOURCE_TABLE)

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold dim_user (Deduplicating & Selecting) ===")

# Deduplicate: Lấy bản ghi mới nhất cho từng user_id dựa vào updated_at/created_at
user_window = Window.partitionBy("user_id").orderBy(col("updated_at").desc(), col("created_at").desc())

gold_df = (
    df.withColumn("rn", row_number().over(user_window))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        col("user_id"),
        col("name"),
        col("gender"),
        col("auth_provider"),
        col("role_id"),
        col("created_at"),
        col("updated_at"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_user ===")
# Tạo bảng nếu chưa tồn tại hoặc ghi đè với cấu hình Apache Iceberg V2
gold_df.writeTo(TARGET_TABLE) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .createOrReplace()

print("=== Validating Gold dim_user ===")
result = spark.table(TARGET_TABLE)
result.show(5, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

# Kiếm tra trùng lặp user_id trên bảng Gold
duplicate_count = (
    result.groupBy("user_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate user_id values in Gold!")

print("✅ User uniqueness validation passed")
print("✅ Gold dim_user ETL completed successfully")

spark.stop()
