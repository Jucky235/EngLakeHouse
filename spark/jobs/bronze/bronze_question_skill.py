from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("SilverQuestionSkill")
    .getOrCreate()
)


# ============================================================
# Configuration
# ============================================================

SOURCE_TABLE = "lakehouse.bronze.question_skill"
TARGET_TABLE = "lakehouse.silver.question_skill"


# ============================================================
# Read Bronze
# ============================================================

print("=== Reading Bronze question_skill table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()

print(bronze_count)


# ============================================================
# Transform Bronze → Silver
# ============================================================

print("=== Transforming QuestionSkill → Silver ===")

silver_df = (
    df.select(
        col("questionId").alias("question_id"),
        col("skillId").alias("skill_id"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


# ============================================================
# Silver schema
# ============================================================

print("=== Silver schema ===")

silver_df.printSchema()


# ============================================================
# Create Silver namespace
# ============================================================

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")


# ============================================================
# Write Silver
# ============================================================

print("=== Writing Silver question_skill table ===")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


# ============================================================
# Read Silver
# ============================================================

print("=== Reading Silver question_skill table ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

silver_count = result.count()

print(f"Silver row count: {silver_count}")


# ============================================================
# Validation
# ============================================================

print("=== Validation ===")

if bronze_count != silver_count:
    raise RuntimeError(
        f"Row count mismatch! Bronze={bronze_count}, Silver={silver_count}"
    )

print("✅ Row count validation passed")
print("=== Silver QuestionSkill transformation complete ===")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()
