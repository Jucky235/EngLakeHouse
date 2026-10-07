from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverUser")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.user"
TARGET_TABLE = "lakehouse.silver.user"


print("=== Reading Bronze user table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
print(df.count())


print("=== Transforming User → Silver ===")

silver_df = (
    df.select(
        col("id").alias("user_id"),
        col("email"),
        col("name"),
        col("phoneNumber").alias("phone_number"),
        col("gender"),
        col("provider").alias("auth_provider"),
        col("roleId").alias("role_id"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver user table ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver user table ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

print(f"Silver row count: {result.count()}")

print("=== Silver User transformation complete ===")

spark.stop()
