from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverNews")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.news"
TARGET_TABLE = "lakehouse.silver.news"

print("=== Reading Bronze news table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming News → Silver ===")

silver_df = (
    df.select(
        col("id").alias("news_id"),
        col("title"),
        col("slug"),
        col("summary"),
        col("content"),
        col("thumbnail"),
        col("category"),
        col("status"),
        col("viewsCount").alias("views_count"),
        col("authorName").alias("author_name"),
        col("publishedAt").alias("published_at"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver news table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver news table ===")

result = spark.table(TARGET_TABLE)
result.show(truncate=False)

silver_count = result.count()
print(f"Silver row count: {silver_count}")

print("=== Validation ===")

if bronze_count != silver_count:
    raise RuntimeError(
        f"Row count mismatch! Bronze={bronze_count}, Silver={silver_count}"
    )

print("✅ Row count validation passed")
print("=== Silver News transformation complete ===")

spark.stop()
