from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    explode,
    from_json,
    lit,
    when,
    count,
    sum as spark_sum,
    round as spark_round,
)
from pyspark.sql.types import MapType, StringType

# ============================================================
# CONFIG
# ============================================================

ATTEMPT_TABLE = "lakehouse.gold.fact_exam_attempt"
QUESTION_TABLE = "lakehouse.gold.dim_question"
QUESTION_SKILL_TABLE = "lakehouse.gold.fact_question_skill"
SKILL_TABLE = "lakehouse.gold.dim_skill"

TARGET_TABLE = "lakehouse.analytics.user_skill_performance"

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AnalyticsUserSkillPerformance")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ GOLD TABLES
# ============================================================

print("=== Reading Gold tables ===")

attempts = spark.table(ATTEMPT_TABLE)

questions = (
    spark.table(QUESTION_TABLE)
    .select(
        "question_id",
        "right_answer",
    )
)

question_skills = (
    spark.table(QUESTION_SKILL_TABLE)
    .select(
        "question_id",
        "skill_id",
    )
)

skills = (
    spark.table(SKILL_TABLE)
    .select(
        "skill_id",
        "skill_name",
        col("slug").alias("skill_slug"),
        "category",
        "parent_skill_id",
    )
)

# Self-join để lấy parent_skill_name
parent_skills = (
    spark.table(SKILL_TABLE)
    .select(
        col("skill_id").alias("parent_skill_id"),
        col("skill_name").alias("parent_skill_name"),
    )
)

skills = (
    skills
    .join(
        parent_skills,
        on="parent_skill_id",
        how="left",
    )
)

# ============================================================
# PARSE ANSWERS JSON
# ============================================================

print("=== Parsing answers JSON ===")

answers_schema = MapType(StringType(), StringType())

answers = (
    attempts
    .select(
        "exam_history_id",
        "user_id",
        "exam_id",
        "started_at",
        "submitted_at",
        "answers",
    )
    .filter(col("answers").isNotNull())
    .withColumn("answers_map", from_json(col("answers"), answers_schema))
)

# ============================================================
# EXPLODE QUESTIONS & CHECK ANSWERS
# ============================================================

print("=== Exploding answers and checking correctness ===")

question_answers = (
    answers
    .filter(col("answers_map").isNotNull())
    .select(
        "user_id",
        "exam_history_id",
        "submitted_at",
        explode(col("answers_map")).alias("question_id", "selected_answer"),
    )
    .join(questions, on="question_id", how="inner")
    .withColumn(
        "is_correct",
        when(
            col("selected_answer").isNotNull() & (col("selected_answer") == col("right_answer")),
            1
        ).otherwise(0)
    )
)

# ============================================================
# CONNECT QUESTION -> SKILL
# ============================================================

print("=== Mapping Question to Skill ===")

skill_answers = (
    question_answers
    .join(question_skills, on="question_id", how="inner")
    .join(skills, on="skill_id", how="inner")
)

# ============================================================
# AGGREGATE USER SKILL PERFORMANCE
# ============================================================

print("=== Calculating skill performance metrics ===")

performance = (
    skill_answers
    .groupBy(
        "user_id",
        "skill_id",
        "skill_name",
        "skill_slug",
        "category",
        "parent_skill_id",
        "parent_skill_name",
    )
    .agg(
        count("*").alias("attempted_questions"),
        spark_sum("is_correct").alias("correct_questions"),
        spark_sum(
            when(col("selected_answer").isNotNull(), 1).otherwise(0)
        ).alias("answered_questions"),
    )
)

# Thêm chỉ số phụ & độ tin cậy (Confidence)
performance = (
    performance
    .withColumn("incorrect_questions", col("attempted_questions") - col("correct_questions"))
    .withColumn(
        "accuracy",
        when(col("attempted_questions") > 0, spark_round(col("correct_questions") / col("attempted_questions"), 4))
        .otherwise(0.0)
    )
    .withColumn(
        "confidence",
        when(col("attempted_questions") < 50, "LOW")
        .when(col("attempted_questions") < 200, "MEDIUM")
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
    performance.writeTo(TARGET_TABLE)
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
print(f"Skill performance rows: {result.count()}")

# 1. Validation Accuracy
invalid_accuracy = result.filter((col("accuracy") < 0) | (col("accuracy") > 1)).count()
if invalid_accuracy > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_accuracy} rows with invalid accuracy!")

# 2. Validation NULL user_id
null_users = result.filter(col("user_id").isNull()).count()
if null_users > 0:
    raise RuntimeError(f"❌ Validation Failed: NULL user_id detected: {null_users}")

# 3. Validation NULL skill_id
null_skills = result.filter(col("skill_id").isNull()).count()
if null_skills > 0:
    raise RuntimeError(f"❌ Validation Failed: NULL skill_id detected: {null_skills}")

# 4. Validation Duplicate Primary Key (user_id, skill_id)
duplicates = (
    result.groupBy("user_id", "skill_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if duplicates > 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicates} duplicate user-skill rows!")

print("✅ Accuracy range validation passed")
print("✅ User ID null validation passed")
print("✅ Skill ID null validation passed")
print("✅ Primary Key uniqueness validation passed")
print("=== User skill performance analytics complete successfully ===")

spark.stop()
