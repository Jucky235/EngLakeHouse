from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverRole")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.role"
TARGET_TABLE = "lakehouse.silver.role"


print("=== Reading Bronze role table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
print(df.count())


print("=== Transforming Role → Silver ===")

silver_df = (
    df.select(
        col("id").alias("role_id"),
        col("name"),
        col("description"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver role table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver role table ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

print(f"Silver row count: {result.count()}")

print("=== Silver Role transformation complete ===")

spark.stop()
