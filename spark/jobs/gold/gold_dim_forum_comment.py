from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


SOURCE_TABLE = "lakehouse.silver.forum_comment"
TARGET_TABLE = "lakehouse.gold.dim_forum_comment"


spark = (
    SparkSession.builder
    .appName("GoldDimForumComment")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Reading {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)

gold_df = df.select(
    "comment_id",
    "content",
    "is_edited",
    "upvotes_count",
    "downvotes_count",
    "post_id",
    "author_id",
    "parent_id",
    "created_at",
    "updated_at",
).withColumn(
    "_gold_processed_at",
    current_timestamp(),
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print(f"Writing {TARGET_TABLE}")

(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

silver_count = df.count()
gold_count = spark.table(TARGET_TABLE).count()

print(f"Silver count: {silver_count}")
print(f"Gold count:   {gold_count}")

if silver_count != gold_count:
    raise RuntimeError(
        f"Count mismatch: Silver={silver_count}, Gold={gold_count}"
    )

duplicate_ids = (
    spark.table(TARGET_TABLE)
    .groupBy("comment_id")
    .count()
    .filter("count > 1")
    .count()
)

null_ids = (
    spark.table(TARGET_TABLE)
    .filter("comment_id IS NULL")
    .count()
)

print(f"Duplicate comment IDs: {duplicate_ids}")
print(f"Null comment IDs:      {null_ids}")

if duplicate_ids != 0:
    raise RuntimeError("Duplicate comment_id values detected")

if null_ids != 0:
    raise RuntimeError("NULL comment_id values detected")

print("Gold dim_forum_comment validation PASSED")

spark.stop()
