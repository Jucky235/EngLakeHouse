from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverPermission")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.permission"
TARGET_TABLE = "lakehouse.silver.permission"


print("=== Reading Bronze permission table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
print(df.count())


print("=== Transforming Permission → Silver ===")

silver_df = (
    df.select(
        col("id").alias("permission_id"),
        col("roleId").alias("role_id"),
        col("resource"),
        col("scope"),
        col("permission"),
        col("allowed"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver permission table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver permission table ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

print(f"Silver row count: {result.count()}")

print("=== Silver Permission transformation complete ===")

spark.stop()
