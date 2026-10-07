from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverExamHistory")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.examhistory"
TARGET_TABLE = "lakehouse.silver.exam_history"


print("=== Reading Bronze exam history table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()
print(bronze_count)


print("=== Transforming ExamHistory → Silver ===")

silver_df = (
    df.select(
        col("id").alias("exam_history_id"),
        col("userId").alias("user_id"),
        col("examId").alias("exam_id"),
        col("score"),
        col("totalQuestions").alias("total_questions"),
        col("correctQuestions").alias("correct_questions"),
        col("isPassed").alias("is_passed"),
        col("answers"),
        col("startedAt").alias("started_at"),
        col("submittedAt").alias("submitted_at"),
        col("timeTakenSeconds").alias("time_taken_seconds"),
        col("createdAt").alias("created_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()


print("=== Writing Silver exam history table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver exam history table ===")

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

print("=== Silver ExamHistory transformation complete ===")


spark.stop()
