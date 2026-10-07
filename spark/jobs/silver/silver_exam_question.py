from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverExamQuestion")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.examquestion"
TARGET_TABLE = "lakehouse.silver.exam_question"


print("=== Reading Bronze exam question table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()
print(bronze_count)


print("=== Transforming ExamQuestion → Silver ===")

silver_df = (
    df.select(
        col("sortOrder").alias("sort_order"),
        col("partId").alias("exam_part_id"),
        col("questionId").alias("question_id"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()


print("=== Writing Silver exam question table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver exam question table ===")

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

print("=== Silver ExamQuestion transformation complete ===")


spark.stop()
