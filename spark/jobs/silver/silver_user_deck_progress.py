from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverUserDeckProgress")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.userdeckprogress"
TARGET_TABLE = "lakehouse.silver.user_deck_progress"

print("=== Reading Bronze user deck progress table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming UserDeckProgress → Silver ===")

silver_df = (
    df.select(
        col("id").alias("progress_id"),
        col("userId").alias("user_id"),
        col("deckId").alias("deck_id"),
        col("totalCardsViewed").alias("total_cards_viewed"),
        col("masteredCards").alias("mastered_cards"),
        col("lastStudiedAt").alias("last_studied_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver user deck progress table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver user deck progress table ===")

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
print("=== Silver UserDeckProgress transformation complete ===")

spark.stop()
