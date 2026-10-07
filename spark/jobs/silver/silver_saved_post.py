from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverSavedPost")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.savedpost"
TARGET_TABLE = "lakehouse.silver.saved_post"

print("=== Reading Bronze saved post table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming SavedPost → Silver ===")

silver_df = (
    df.select(
        col("id").alias("saved_post_id"),
        col("userId").alias("user_id"),
        col("postId").alias("post_id"),
        col("createdAt").alias("created_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver saved post table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver saved post table ===")

result = spark.table(TARGET_TABLE)
result.show(truncate=False)

silver_count = result.count()
print(f"Silver row count: {silver_count}")

print("=== Validation ===")

if bronze_count != silver_count:
    raise RuntimeError(
        f"Row count mismatch! Bronze={bronze_count}, Silver={silver_count}"
    )

print("✅ Row count validation passed")
print("=== Silver SavedPost transformation complete ===")

spark.stop()
