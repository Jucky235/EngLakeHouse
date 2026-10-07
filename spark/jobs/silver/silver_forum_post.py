from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


spark = (
    SparkSession.builder
    .appName("SilverForumPost")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.forumpost"
TARGET_TABLE = "lakehouse.silver.forum_post"


print("=== Reading Bronze forum post table ===")

df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")

bronze_count = df.count()
print(bronze_count)


print("=== Transforming ForumPost → Silver ===")

silver_df = (
    df.select(
        col("id").alias("post_id"),
        col("title"),
        col("slug"),
        col("content"),
        col("attachments"),
        col("status"),
        col("isPinned").alias("is_pinned"),
        col("isLocked").alias("is_locked"),
        col("viewsCount").alias("views_count"),
        col("upvotesCount").alias("upvotes_count"),
        col("downvotesCount").alias("downvotes_count"),
        col("categoryId").alias("category_id"),
        col("authorId").alias("author_id"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)


print("=== Silver schema ===")
silver_df.printSchema()


print("=== Writing Silver forum post table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)


print("=== Reading Silver forum post table ===")

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

print("=== Silver ForumPost transformation complete ===")


spark.stop()
