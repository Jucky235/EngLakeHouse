from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
    when,
)

# ============================================================
# CONFIG
# ============================================================

SOURCE_TABLE = "lakehouse.analytics.user_skill_performance"
TARGET_TABLE = "lakehouse.analytics.community_skill_performance"

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AnalyticsCommunitySkillPerformance")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ USER SKILL PERFORMANCE
# ============================================================

print("=== Reading user skill performance ===")

performance = spark.table(SOURCE_TABLE)

# ============================================================
# AGGREGATE ALL USERS
# ============================================================

print("=== Calculating community skill performance ===")

community = (
    performance
    .groupBy(
        "skill_id",
        "skill_name",
        "skill_slug",
        "category",
        "parent_skill_id",
        "parent_skill_name",
    )
    .agg(
        # Number of users who have attempted this skill
        countDistinct("user_id").alias("users_attempted"),

        # Total questions across all users
        spark_sum("attempted_questions").alias("total_attempted_questions"),

        # Total answered questions
        spark_sum("answered_questions").alias("total_answered_questions"),

        # Total correct answers
        spark_sum("correct_questions").alias("total_correct_questions"),

        # Total incorrect answers
        spark_sum("incorrect_questions").alias("total_incorrect_questions"),
    )
)

# ============================================================
# CALCULATE COMMUNITY ACCURACY & ERROR RATE (SAFE DIVISION)
# ============================================================

print("=== Calculating community accuracy & error rate ===")

community = (
    community
    .withColumn(
        "community_accuracy",
        when(
            col("total_attempted_questions") > 0,
            spark_round(col("total_correct_questions") / col("total_attempted_questions"), 4)
        ).otherwise(0.0)
    )
    .withColumn(
        "community_error_rate",
        when(
            col("total_attempted_questions") > 0,
            spark_round(col("total_incorrect_questions") / col("total_attempted_questions"), 4)
        ).otherwise(0.0)
    )
)

# ============================================================
# COMMUNITY CONFIDENCE
# ============================================================

community = (
    community
    .withColumn(
        "community_confidence",
        when(col("total_attempted_questions") < 100, "LOW")
        .when(col("total_attempted_questions") < 500, "MEDIUM")
        .otherwise("HIGH")
    )
    .withColumn("_analytics_processed_at", current_timestamp())
)

# ============================================================
# WRITE ICEBERG ANALYTICS TABLE
# ============================================================

print("=== Creating Analytics namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.analytics")

print(f"=== Writing {TARGET_TABLE} ===")
(
    community.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

# ============================================================
# VALIDATION
# ============================================================

print("=== Validation ===")
result = spark.table(TARGET_TABLE)
row_count = result.count()
print(f"Community skill rows: {row_count}")

# 1. Null skill validation
null_skills = result.filter(col("skill_id").isNull()).count()
if null_skills > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_skills} rows with NULL skill_id!")

# 2. User count validation
invalid_users = result.filter(col("users_attempted") <= 0).count()
if invalid_users > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_users} rows with invalid users_attempted <= 0!")

# 3. Accuracy validation
invalid_accuracy = result.filter((col("community_accuracy") < 0) | (col("community_accuracy") > 1)).count()
if invalid_accuracy > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_accuracy} rows with invalid community_accuracy!")

# 4. Error rate validation
invalid_error_rate = result.filter((col("community_error_rate") < 0) | (col("community_error_rate") > 1)).count()
if invalid_error_rate > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_error_rate} rows with invalid community_error_rate!")

# 5. Question count validation
invalid_counts = result.filter(
    (col("total_correct_questions") + col("total_incorrect_questions")) > col("total_attempted_questions")
).count()
if invalid_counts > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_counts} rows where correct + incorrect exceeds total attempted questions!")

# 6. Duplicate validation
duplicates = (
    result.groupBy("skill_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if duplicates > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicates} duplicate skill_id rows!")

print("✅ Skill ID null validation passed")
print("✅ User count validation passed")
print("✅ Accuracy range validation passed")
print("✅ Error rate range validation passed")
print("✅ Question count consistency validation passed")
print("✅ Primary Key uniqueness validation passed")

# ============================================================
# PREVIEW
# ============================================================

print("\nTop 10 weakest community skills:")
(
    result
    .select(
        "skill_id",
        "skill_name",
        "category",
        "users_attempted",
        "total_attempted_questions",
        "total_answered_questions",
        "total_correct_questions",
        "total_incorrect_questions",
        "community_accuracy",
        "community_error_rate",
        "community_confidence",
    )
    .orderBy(col("community_accuracy").asc())
    .show(10, truncate=False)
)

print("=== Community skill performance analytics complete successfully ===")

spark.stop()
