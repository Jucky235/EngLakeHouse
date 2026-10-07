from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverChatChannel")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.chatchannel"
TARGET_TABLE = "lakehouse.silver.chat_channel"

print("=== Reading Bronze chat channel table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming ChatChannel → Silver ===")

silver_df = (
    df.select(
        col("id").alias("channel_id"),
        col("name"),
        col("description"),
        col("icon"),
        col("status"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver chat channel table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver chat channel table ===")

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
print("=== Silver ChatChannel transformation complete ===")

spark.stop()
