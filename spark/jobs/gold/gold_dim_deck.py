from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, row_number
from pyspark.sql.window import Window

# 1. Khởi tạo Spark Session với Iceberg extension
spark = (
    SparkSession.builder
    .appName("GoldDimDeck")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.deck"
TARGET_TABLE = "lakehouse.gold.dim_deck"

print("=== Reading Silver deck table ===")
df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver raw row count: {silver_count}")

print("=== Building Gold dim_deck (Deduplicating & Mapping Date Key) ===")

# Deduplicate dựa trên deck_id (lấy bản ghi mới nhất theo updated_at / created_at)
order_cols = []
if "updated_at" in df.columns:
    order_cols.append(col("updated_at").desc())
if "created_at" in df.columns:
    order_cols.append(col("created_at").desc())

if order_cols:
    deck_window = Window.partitionBy("deck_id").orderBy(*order_cols)
    dedup_df = df.withColumn("rn", row_number().over(deck_window)).filter(col("rn") == 1).drop("rn")
else:
    dedup_df = df.dropDuplicates(["deck_id"])

# Select các cột thuộc tính của Deck
select_cols = [
    col("deck_id"),
    col("name"),
    col("description"),
    col("category"),
    col("visibility"),
    col("creator_id"),
    col("status"),
    col("created_at"),
    col("updated_at"),
]

# Thêm created_date_key nếu có created_at trong Silver
if "created_at" in df.columns:
    select_cols.append(
        date_format(col("created_at"), "yyyyMMdd").cast("int").alias("created_date_key")
    )

gold_df = dedup_df.select(*select_cols).withColumn("_gold_processed_at", current_timestamp())

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_deck ===")
(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print("=== Reading Gold dim_deck ===")
result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold final row count: {gold_count}")

print("=== Validation ===")

# 1. Cảnh báo biến động dòng nếu có Deduplication
if silver_count != gold_count:
    print(f"⚠️ Warning: Row count changed! Silver={silver_count}, Gold={gold_count} (Deduplication applied)")

# 2. Kiểm tra Primary Key duy nhất
duplicate_count = (
    result
    .groupBy("deck_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {duplicate_count} duplicate deck_id values!")

# 3. Kiểm tra NULL Primary Key
null_id_count = result.filter(col("deck_id").isNull()).count()

if null_id_count != 0:
    raise RuntimeError(f"❌ Validation Failed: Found {null_id_count} rows with NULL deck_id!")

print("✅ Deck uniqueness validation passed")
print("✅ Deck ID null validation passed")
print("=== Gold dim_deck ETL completed successfully ===")

spark.stop()
