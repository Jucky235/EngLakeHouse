from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverExamPart")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.exampart"
TARGET_TABLE = "lakehouse.silver.exam_part"


print("=== Reading Bronze exam part table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()
print(bronze_count)


print("=== Transforming ExamPart → Silver ===")

silver_df = (
    df.select(
        col("id").alias("exam_part_id"),
        col("partNumber").alias("part_number"),
        col("name"),
        col("instructions"),
        col("audioPath").alias("audio_path"),
        col("sortOrder").alias("sort_order"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
        col("examId").alias("exam_id"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()


print("=== Writing Silver exam part table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver exam part table ===")

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

print("=== Silver ExamPart transformation complete ===")


spark.stop()
