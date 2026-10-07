from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverFlashcardProgress")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.flashcardprogress"
TARGET_TABLE = "lakehouse.silver.flashcard_progress"

print("=== Reading Bronze flashcard progress table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming FlashcardProgress → Silver ===")

silver_df = (
    df.select(
        col("id").alias("progress_id"),
        col("userId").alias("user_id"),
        col("flashcardId").alias("flashcard_id"),
        col("box"),
        col("intervalDays").alias("interval_days"),
        col("easeFactor").alias("ease_factor"),
        col("repetitions"),
        col("nextReviewAt").alias("next_review_at"),
        col("lastReviewedAt").alias("last_reviewed_at"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver flashcard progress table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver flashcard progress table ===")

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
print("=== Silver FlashcardProgress transformation complete ===")

spark.stop()
