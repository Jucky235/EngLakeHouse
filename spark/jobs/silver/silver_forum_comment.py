from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

spark = (
    SparkSession.builder
    .appName("SilverForumComment")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.bronze.forumcomment"
TARGET_TABLE = "lakehouse.silver.forum_comment"

print("=== Reading Bronze forum comment table ===")
df = spark.table(SOURCE_TABLE)

print("=== Bronze schema ===")
df.printSchema()

print("=== Bronze row count ===")
bronze_count = df.count()
print(bronze_count)

print("=== Transforming ForumComment → Silver ===")

silver_df = (
    df.select(
        col("id").alias("comment_id"),
        col("content"),
        col("isEdited").alias("is_edited"),
        col("upvotesCount").alias("upvotes_count"),
        col("downvotesCount").alias("downvotes_count"),
        col("postId").alias("post_id"),
        col("authorId").alias("author_id"),
        col("parentId").alias("parent_id"),
        col("createdAt").alias("created_at"),
        col("updatedAt").alias("updated_at"),
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

print("=== Silver schema ===")
silver_df.printSchema()

print("=== Writing Silver forum comment table ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

(
    silver_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Silver forum comment table ===")

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
print("=== Silver ForumComment transformation complete ===")

spark.stop()
