from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverNewsTag")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.newstag"
TARGET_TABLE = "lakehouse.silver.news_tag"

print("=== Reading Bronze news tag table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming NewsTag → Silver ===")

silver_df = (
    df.select(
        col("id").alias("tag_id"),
        col("name"),
        col("slug"),
        col("createdAt").alias("created_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver news_tag table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver news_tag table ===")

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
print("=== Silver NewsTag transformation complete ===")

spark.stop()
