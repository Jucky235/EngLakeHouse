from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, coalesce, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với Iceberg extension
spark = (
    SparkSession.builder
    .appName("GoldFactFlashcardReview")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.flashcard_progress"
TARGET_TABLE = "lakehouse.gold.fact_flashcard_review"

print("=== Reading Silver flashcard_progress table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold fact_flashcard_review (Deduplicating & Mapping Date Key) ===")

# Deduplicate dựa trên progress_id (lấy bản ghi mới nhất theo updated_at / last_reviewed_at)
order_cols = []
if "updated_at" in df.columns:
    order_cols.append(col("updated_at").desc())
if "last_reviewed_at" in df.columns:
    order_cols.append(col("last_reviewed_at").desc())

if order_cols:
    progress_window = Window.partitionBy("progress_id").orderBy(*order_cols)
    dedup_df = df.withColumn("rn", row_number().over(progress_window)).filter(col("rn") == 1).drop("rn")
else:
    dedup_df = df.dropDuplicates(["progress_id"])

# Tạo review_date_key để map với dim_date (ưu tiên last_reviewed_at > updated_at > created_at)
review_date_source = coalesce(col("last_reviewed_at"), col("updated_at"), col("created_at"))

gold_df = (
    dedup_df.select(
        col("progress_id"),
        col("user_id"),
        col("flashcard_id"),
        col("box"),
        col("interval_days"),
        col("ease_factor"),
        col("repetitions"),
        col("next_review_at"),
        col("last_reviewed_at"),
        col("created_at"),
        col("updated_at"),
        date_format(review_date_source, "yyyyMMdd").cast("int").alias("review_date_key")
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold fact_flashcard_review ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold fact_flashcard_review ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Cảnh báo biến động số lượng dòng do Deduplication
if silver_count != gold_count:
    print(f"⚠️ Warning: Row count changed! Silver={silver_count}, Gold={gold_count} (Deduplication applied)")

# 2. Kiểm tra Primary Key duy nhất
duplicate_count = (
    result.groupBy("progress_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate progress_id values!")

# 3. Kiểm tra NULL Primary Key
null_id_count = result.filter(col("progress_id").isNull()).count()

if null_id_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_id_count} rows with NULL progress_id!")

# 4. Kiểm tra dữ liệu hợp lệ (interval_days, repetitions)
invalid_values = (
    result
    .filter(
        (col("interval_days") < 0)
        | (col("repetitions") < 0)
    )
    .count()
)

if invalid_values != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {invalid_values} rows with invalid interval/repetition values!")

print("✅ Progress uniqueness validation passed")
print("✅ Progress ID null validation passed")
print("✅ Interval/repetition validation passed")
print("=== Gold fact_flashcard_review ETL completed successfully ===")

spark.stop()
