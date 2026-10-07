from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverQuestion")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.question"
TARGET_TABLE = "lakehouse.silver.question"

print("=== Reading Bronze question table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming Question → Silver ===")

silver_df = (
    df.select(
        col("id").alias("question_id"),
        col("content"),
        col("options"),
        col("category"),
        col("explanation"),
        col("status"),
        col("topicNumber").alias("topic_number"),
        col("lastEditedById").alias("last_edited_by_id"),
        col("lastEditedBy").alias("last_edited_by"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
        col("right_answer"),
        col("audioPath").alias("audio_path"),
        col("imagePath").alias("image_path"),
        col("partNumber").alias("part_number"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver question table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver question table ===")

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
print("=== Silver Question transformation complete ===")

spark.stop()
