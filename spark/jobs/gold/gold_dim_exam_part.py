from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder
    .appName("GoldDimExamPart")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.exam_part"
TARGET_TABLE = "lakehouse.gold.dim_exam_part"

print("=== Reading Silver exam_part table ===")

df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver row count: {silver_count}")

print("=== Building Gold dim_exam_part ===")

gold_df = (
    df.select(
        col("exam_part_id"),
        col("exam_id"),
        col("part_number"),
        col("name"),
        col("instructions"),
        col("audio_path"),
        col("sort_order"),
        col("created_at"),
        col("updated_at"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
""")

print("=== Writing Gold dim_exam_part ===")

(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Gold dim_exam_part ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

gold_count = result.count()
print(f"Gold row count: {gold_count}")

print("=== Validation ===")

if silver_count != gold_count:
    raise RuntimeError(
        f"Row count mismatch! Silver={silver_count}, Gold={gold_count}"
    )

duplicate_count = (
    result.groupBy("exam_part_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(
        f"Found {duplicate_count} duplicate exam_part_id values!"
    )

null_id_count = (
    result
    .filter(col("exam_part_id").isNull())
    .count()
)

if null_id_count != 0:
    raise RuntimeError(
        f"Found {null_id_count} rows with NULL exam_part_id!"
    )

print("✅ Row count validation passed")
print("✅ Exam part uniqueness validation passed")
print("✅ Exam part ID null validation passed")
print("=== Gold dim_exam_part complete ===")

spark.stop()
