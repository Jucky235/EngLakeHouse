from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverQuestionSkill")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.question_skill"
TARGET_TABLE = "lakehouse.silver.question_skill"


print("=== Reading Bronze question_skill table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()
print(bronze_count)


# ============================================================
# Transform
# ============================================================

print("=== Transforming QuestionSkill → Silver ===")

silver_df = (
    df.select(
        col("questionId").alias("question_id"),
        col("skillId").alias("skill_id"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()


# ============================================================
# Write
# ============================================================

print("=== Writing Silver question_skill table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


# ============================================================
# Validate
# ============================================================

print("=== Reading Silver question_skill table ===")

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

print("=== Silver QuestionSkill transformation complete ===")

spark.stop()
