from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverFlashcard")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.flashcard"
TARGET_TABLE = "lakehouse.silver.flashcard"

print("=== Reading Bronze flashcard table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming Flashcard → Silver ===")

silver_df = (
    df.select(
        col("id").alias("flashcard_id"),
        col("deckId").alias("deck_id"),
        col("frontContent").alias("front_content"),
        col("backContent").alias("back_content"),
        col("explanation"),
        col("imagePath").alias("image_path"),
        col("audioPath").alias("audio_path"),
        col("partNumber").alias("part_number"),
        col("creatorId").alias("creator_id"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
        col("status"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver flashcard table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver flashcard table ===")

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
print("=== Silver Flashcard transformation complete ===")

spark.stop()
