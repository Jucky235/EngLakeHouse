from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với Iceberg extension
spark = (
    SparkSession.builder
    .appName("GoldFactQuestionSkill")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.question_skill"
QUESTION_TABLE = "lakehouse.gold.dim_question"
SKILL_TABLE = "lakehouse.gold.dim_skill"
TARGET_TABLE = "lakehouse.gold.fact_question_skill"

print("=== Reading Silver question_skill table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Reading Gold dimension tables ===")
question_df = spark.table(QUESTION_TABLE).select(col("question_id"))
skill_df = spark.table(SKILL_TABLE).select(col("skill_id"))

print("=== Building Gold fact_question_skill ===")

# Deduplicate dựa trên Cặp Khóa (question_id, skill_id)
dedup_df = df.dropDuplicates(["question_id", "skill_id"])

# Enforce Referential Integrity thông qua INNER JOIN với các bảng Dimension
gold_df = (
    dedup_df.select(
        col("question_id"),
        col("skill_id")
    )
    .join(question_df, on="question_id", how="inner")
    .join(skill_df, on="skill_id", how="inner")
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold fact_question_skill ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold fact_question_skill ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Kiểm tra biến động số lượng dòng (Warning nếu giảm do Filter / Invalid Keys)
if silver_count != gold_count:
    print(f"⚠️ Warning: Row count changed! Silver={silver_count}, Gold={gold_count} (Filter/Deduplication applied)")

# 2. Kiểm tra Composite Primary Key duy nhất
duplicate_count = (
    result
    .groupBy("question_id", "skill_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate question-skill relationships!")

# 3. Kiểm tra NULL ID
null_question_count = result.filter(col("question_id").isNull()).count()
if null_question_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_question_count} rows with NULL question_id!")

null_skill_count = result.filter(col("skill_id").isNull()).count()
if null_skill_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_skill_count} rows with NULL skill_id!")

print("✅ Question-skill uniqueness validation passed")
print("✅ Foreign key non-null validation passed")
print("=== Gold fact_question_skill ETL completed successfully ===")

spark.stop()
