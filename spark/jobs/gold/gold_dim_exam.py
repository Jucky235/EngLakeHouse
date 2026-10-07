from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với extension Iceberg
spark = (
    SparkSession.builder
    .appName("GoldDimExam")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.exam"
TARGET_TABLE = "lakehouse.gold.dim_exam"

print("=== Reading Silver exam table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold dim_exam (Deduplicating & Mapping Date Key) ===")

# Deduplicate: Lấy bản ghi mới nhất cho từng exam_id dựa vào updated_at/created_at
exam_window = Window.partitionBy("exam_id").orderBy(col("updated_at").desc(), col("created_at").desc())

gold_df = (
    df.withColumn("rn", row_number().over(exam_window))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        col("exam_id"),
        col("name"),
        col("category"),
        col("duration_minutes"),
        col("status"),
        col("created_at"),
        # Foreign Key dạng INT (YYYYMMDD) trỏ tới dim_date
        date_format(col("created_at"), "yyyyMMdd").cast("int").alias("created_date_key"),
        col("updated_at"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_exam ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold dim_exam ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Kiểm tra duy nhất (Uniqueness)
duplicate_count = (
    result.groupBy("exam_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate exam_id values!")

# 2. Kiểm tra NULL ID
null_id_count = result.filter(col("exam_id").isNull()).count()

if null_id_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_id_count} rows with NULL exam_id!")

print("✅ Exam uniqueness validation passed")
print("✅ Exam ID null validation passed")
print("=== Gold dim_exam ETL completed successfully ===")

spark.stop()
