from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với Iceberg extension
spark = (
    SparkSession.builder
    .appName("GoldDimQuestion")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.question"
TARGET_TABLE = "lakehouse.gold.dim_question"

print("=== Reading Silver question table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold dim_question (Deduplicating) ===")

# Deduplicate: Lấy bản ghi mới nhất dựa trên question_id
# Hỗ trợ cả trường hợp có hoặc không có các cột timestamp trong Silver
order_cols = []
if "updated_at" in df.columns:
    order_cols.append(col("updated_at").desc())
if "created_at" in df.columns:
    order_cols.append(col("created_at").desc())

if order_cols:
    question_window = Window.partitionBy("question_id").orderBy(*order_cols)
    dedup_df = df.withColumn("rn", row_number().over(question_window)).filter(col("rn") == 1).drop("rn")
else:
    dedup_df = df.dropDuplicates(["question_id"])

# Chọn các cột theo đúng schema thực tế của bạn
select_cols = [
    col("question_id"),
    col("content"),
    col("options"),
    col("category"),
    col("explanation"),
    col("status"),
    col("topic_number"),
    col("right_answer"),
    col("audio_path"),
    col("image_path"),
    col("part_number"),
]

# Thêm created_date_key nếu có created_at trong Silver
if "created_at" in df.columns:
    select_cols.append(col("created_at"))
    select_cols.append(date_format(col("created_at"), "yyyyMMdd").cast("int").alias("created_date_key"))

gold_df = dedup_df.select(*select_cols).withColumn("_gold_processed_at", current_timestamp())

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_question ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold dim_question ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Kiểm tra Primary Key duy nhất
duplicate_count = (
    result.groupBy("question_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate question_id values!")

# 2. Kiểm tra NULL ID
null_id_count = result.filter(col("question_id").isNull()).count()

if null_id_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_id_count} rows with NULL question_id!")

print("✅ Question uniqueness validation passed")
print("✅ Question ID null validation passed")
print("=== Gold dim_question ETL completed successfully ===")

spark.stop()
