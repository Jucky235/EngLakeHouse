from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


SOURCE_TABLE = "lakehouse.analytics.user_skill_performance"
TARGET_TABLE = "lakehouse.analytics.user_roadmap_skill_performance"


spark = (
    SparkSession.builder
    .appName("AnalyticsUserRoadmapSkillPerformance")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=== Reading user skill performance ===")

performance = spark.table(SOURCE_TABLE)


print("=== Filtering leaf skills ===")

roadmap_performance = (
    performance
    .filter(col("parent_skill_id").isNotNull())
    .select(
        "user_id",
        "skill_id",
        "skill_name",
        "skill_slug",
        "category",
        "parent_skill_id",
        "parent_skill_name",
        "attempted_questions",
        "answered_questions",
        "correct_questions",
        "incorrect_questions",
        "accuracy",
        "confidence",
    )
    .withColumn(
        "_analytics_processed_at",
        current_timestamp(),
    )
)


print("=== Creating Analytics namespace ===")

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.analytics"
)


print(f"=== Writing {TARGET_TABLE} ===")

(
    roadmap_performance.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "zstd")
    .createOrReplace()
)


print("=== Validation ===")

result = spark.table(TARGET_TABLE)

row_count = result.count()

print(f"Roadmap skill performance rows: {row_count}")


null_users = (
    result
    .filter(col("user_id").isNull())
    .count()
)

if null_users > 0:
    raise RuntimeError(
        f"Validation Failed: NULL user_id detected: {null_users}"
    )


null_skills = (
    result
    .filter(col("skill_id").isNull())
    .count()
)

if null_skills > 0:
    raise RuntimeError(
        f"Validation Failed: NULL skill_id detected: {null_skills}"
    )


parent_skills = (
    result
    .filter(col("parent_skill_id").isNull())
    .count()
)

if parent_skills > 0:
    raise RuntimeError(
        f"Validation Failed: Found {parent_skills} parent skill rows!"
    )


invalid_accuracy = (
    result
    .filter(
        (col("accuracy") < 0)
        | (col("accuracy") > 1)
    )
    .count()
)

if invalid_accuracy > 0:
    raise RuntimeError(
        f"Validation Failed: Found {invalid_accuracy} rows with invalid accuracy!"
    )


duplicates = (
    result
    .groupBy(
        "user_id",
        "skill_id",
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicates > 0:
    raise RuntimeError(
        f"Validation Failed: Found {duplicates} duplicate user-skill rows!"
    )


invalid_counts = (
    result
    .filter(
        (col("correct_questions") + col("incorrect_questions"))
        != col("attempted_questions")
    )
    .count()
)

if invalid_counts > 0:
    raise RuntimeError(
        f"Validation Failed: Found {invalid_counts} rows with inconsistent question counts!"
    )


print("User ID validation PASSED")
print("Skill ID validation PASSED")
print("Leaf skill validation PASSED")
print("Accuracy validation PASSED")
print("Duplicate validation PASSED")
print("Question count validation PASSED")


print("\nWeakest roadmap skills:")

(
    result
    .filter(col("accuracy") < 0.70)
    .select(
        "user_id",
        "skill_id",
        "skill_name",
        "parent_skill_name",
        "category",
        "attempted_questions",
        "correct_questions",
        "incorrect_questions",
        "accuracy",
        "confidence",
    )
    .orderBy(
        col("user_id"),
        col("accuracy").asc(),
    )
    .show(20, truncate=False)
)


print(
    "=== User roadmap skill performance analytics complete successfully ==="
)


spark.stop()
