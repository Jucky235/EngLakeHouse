from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    round as spark_round,
    row_number,
    when,
)
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với extension Iceberg
spark = (
    SparkSession.builder
    .appName("GoldFactExamAttempt")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.exam_history"
TARGET_TABLE = "lakehouse.gold.fact_exam_attempt"

print("=== Reading Silver exam_history table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold fact_exam_attempt (Deduplicating & Mapping Date Keys) ===")

# Deduplicate: Lấy bản ghi mới nhất cho mỗi exam_history_id
fact_window = Window.partitionBy("exam_history_id").orderBy(col("created_at").desc())

gold_df = (
    df.withColumn("rn", row_number().over(fact_window))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        col("exam_history_id"),
        col("user_id"),
        col("exam_id"),
        # Foreign Keys trỏ tới dim_date (YYYYMMDD)
        date_format(col("started_at"), "yyyyMMdd").cast("int").alias("started_date_key"),
        date_format(col("submitted_at"), "yyyyMMdd").cast("int").alias("submitted_date_key"),
        date_format(col("created_at"), "yyyyMMdd").cast("int").alias("created_date_key"),
        col("score"),
        col("total_questions"),
        col("correct_questions"),
        col("is_passed"),
        col("answers"),
        col("started_at"),
        col("submitted_at"),
        col("time_taken_seconds"),
        col("created_at"),
    )
    # Calculated Metrics
    .withColumn(
        "accuracy",
        when(
            col("total_questions") > 0,
            spark_round(
                col("correct_questions") / col("total_questions"),
                4,
            ),
        ).otherwise(None),
    )
    .withColumn(
        "duration_minutes",
        when(
            col("time_taken_seconds").isNotNull(),
            spark_round(
                col("time_taken_seconds") / 60.0,
                2,
            ),
        ).otherwise(None),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold fact_exam_attempt ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold fact_exam_attempt ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Kiểm tra Primary Key duy nhất
duplicate_count = (
    result.groupBy("exam_history_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {duplicate_count} duplicate exam_history_id values!"
    )

# 2. Kiểm tra NULL Primary Key
null_id_count = (
    result
    .filter(col("exam_history_id").isNull())
    .count()
)

if null_id_count != 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {null_id_count} rows with NULL exam_history_id!"
    )

# 3. Kiểm tra tính hợp lệ của số lượng câu hỏi
invalid_question_counts = (
    result
    .filter(
        (col("total_questions") < 0)
        | (col("correct_questions") < 0)
        | (col("correct_questions") > col("total_questions"))
    )
    .count()
)

if invalid_question_counts != 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {invalid_question_counts} rows with invalid question counts!"
    )

print("✅ Exam attempt uniqueness validation passed")
print("✅ Exam history ID null validation passed")
print("✅ Question count validation passed")
print("=== Gold fact_exam_attempt ETL completed successfully ===")

spark.stop()
