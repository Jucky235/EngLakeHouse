from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverDeck")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.deck"
TARGET_TABLE = "lakehouse.silver.deck"

print("=== Reading Bronze deck table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming Deck → Silver ===")

silver_df = (
    df.select(
        col("id").alias("deck_id"),
        col("name"),
        col("description"),
        col("category"),
        col("visibility"),
        col("creatorId").alias("creator_id"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
        col("status"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver deck table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver deck table ===")

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
print("=== Silver Deck transformation complete ===")

spark.stop()
