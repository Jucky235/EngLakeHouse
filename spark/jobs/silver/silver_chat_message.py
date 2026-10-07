from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverChatMessage")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.chatmessage"
TARGET_TABLE = "lakehouse.silver.chat_message"

print("=== Reading Bronze chat message table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming ChatMessage → Silver ===")

silver_df = (
    df.select(
        col("id").alias("message_id"),
        col("channelId").alias("channel_id"),
        col("senderId").alias("sender_id"),
        col("content"),
        col("attachments"),
        col("parentId").alias("parent_id"),
        col("isEdited").alias("is_edited"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver chat message table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver chat message table ===")

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
print("=== Silver ChatMessage transformation complete ===")

spark.stop()
