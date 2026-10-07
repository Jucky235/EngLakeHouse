from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    round as spark_round,
)


# ============================================================
# CONFIG
# ============================================================

SOURCE_TABLE = (
    "lakehouse.analytics.user_skill_performance"
)

TARGET_TABLE = (
    "lakehouse.analytics.user_skill_summary"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("AnalyticsUserSkillSummary")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ DETAIL TABLE
# ============================================================

print("Reading user skill performance...")


performance = spark.table(
    SOURCE_TABLE
)


# ============================================================
# FILTER PARENT SKILLS ONLY
# ============================================================

print("Creating parent skill summary...")


summary = (
    performance
    .filter(
        col("parent_skill_id").isNull()
    )
    .select(
        "user_id",

        col("skill_id")
        .alias("skill_id"),

        col("skill_name")
        .alias("skill_name"),

        col("category"),

        col("attempted_questions"),

        col("correct_questions"),

        col("incorrect_questions"),

        col("accuracy"),

        col("confidence"),
    )
    .withColumn(
        "_analytics_processed_at",
        current_timestamp(),
    )
)


# ============================================================
# CREATE NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS lakehouse.analytics
""")


# ============================================================
# WRITE ICEBERG
# ============================================================

print(
    f"Writing {TARGET_TABLE}"
)


(
    summary.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty(
        "format-version",
        "2",
    )
    .tableProperty(
        "write.parquet.compression-codec",
        "zstd",
    )
    .createOrReplace()
)


# ============================================================
# VALIDATION
# ============================================================

result = spark.table(
    TARGET_TABLE
)


print(
    f"Summary rows: {result.count()}"
)


# No child skills

child_skill_count = (
    result
    .filter(
        col("skill_id").isNull()
    )
    .count()
)


if child_skill_count:
    raise RuntimeError(
        f"NULL skills detected: {child_skill_count}"
    )


print(
    "Skill validation PASSED"
)


# Duplicate user skill

duplicate_count = (
    result
    .groupBy(
        "user_id",
        "skill_id",
    )
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)


if duplicate_count:
    raise RuntimeError(
        f"Duplicate summary rows: {duplicate_count}"
    )


print(
    "Duplicate validation PASSED"
)


# Accuracy validation

invalid_accuracy = (
    result
    .filter(
        (col("accuracy") < 0)
        |
        (col("accuracy") > 1)
    )
    .count()
)


if invalid_accuracy:
    raise RuntimeError(
        f"Invalid accuracy values: {invalid_accuracy}"
    )


print(
    "Accuracy validation PASSED"
)


print(
    "User skill summary analytics complete"
)


spark.stop()
