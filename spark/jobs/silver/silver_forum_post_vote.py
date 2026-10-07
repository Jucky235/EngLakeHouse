from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverForumPostVote")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.forumpostvote"
TARGET_TABLE = "lakehouse.silver.forum_post_vote"

print("=== Reading Bronze forum post vote table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming ForumPostVote → Silver ===")

silver_df = (
    df.select(
        col("id").alias("vote_id"),
        col("type").alias("vote_type"),
        col("userId").alias("user_id"),
        col("postId").alias("post_id"),
        col("createdAt").alias("created_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver forum post vote table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver forum post vote table ===")

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
print("=== Silver ForumPostVote transformation complete ===")

spark.stop()
