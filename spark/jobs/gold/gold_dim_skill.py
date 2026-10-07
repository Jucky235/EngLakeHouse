from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với Iceberg extension
spark = (
    SparkSession.builder
    .appName("GoldDimSkill")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.skill"
TARGET_TABLE = "lakehouse.gold.dim_skill"

print("=== Reading Silver skill table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold dim_skill (Deduplicating & Mapping Date Key) ===")

# Deduplicate dựa trên skill_id (lấy bản ghi mới nhất theo updated_at/created_at)
order_cols = []
if "updated_at" in df.columns:
    order_cols.append(col("updated_at").desc())
if "created_at" in df.columns:
    order_cols.append(col("created_at").desc())

if order_cols:
    skill_window = Window.partitionBy("skill_id").orderBy(*order_cols)
    dedup_df = df.withColumn("rn", row_number().over(skill_window)).filter(col("rn") == 1).drop("rn")
else:
    dedup_df = df.dropDuplicates(["skill_id"])

# Select các cột thuộc tính của Skill
select_cols = [
    col("skill_id"),
    col("skill_name"),
    col("slug"),
    col("description"),
    col("category"),
    col("parent_skill_id"),
    col("created_at"),
    col("updated_at"),
]

# Thêm created_date_key nếu có created_at
if "created_at" in df.columns:
    select_cols.append(
        date_format(col("created_at"), "yyyyMMdd").cast("int").alias("created_date_key")
    )

gold_df = dedup_df.select(*select_cols).withColumn("_gold_processed_at", current_timestamp())

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_skill ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold dim_skill ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Kiểm tra Primary Key duy nhất
duplicate_count = (
    result
    .groupBy("skill_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate skill_id values!")

# 2. Kiểm tra NULL ID
null_id_count = (
    result
    .filter(col("skill_id").isNull())
    .count()
)

if null_id_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_id_count} rows with NULL skill_id!")

print("✅ Skill uniqueness validation passed")
print("✅ Skill ID null validation passed")
print("=== Gold dim_skill ETL completed successfully ===")

spark.stop()
