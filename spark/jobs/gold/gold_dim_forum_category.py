from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


SOURCE_TABLE = "lakehouse.silver.forum_category"
TARGET_TABLE = "lakehouse.gold.dim_forum_category"


spark = (
    SparkSession.builder
    .appName("GoldDimForumCategory")
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Reading {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)

gold_df = df.select(
    "category_id",
    "name",
    "slug",
    "description",
    "icon",
    "is_private",
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
    .groupBy("category_id")
    .count()
    .filter("count > 1")
    .count()
)

null_ids = (
    spark.table(TARGET_TABLE)
    .filter("category_id IS NULL")
    .count()
)

print(f"Duplicate category IDs: {duplicate_ids}")
print(f"Null category IDs:      {null_ids}")

if duplicate_ids != 0:
    raise RuntimeError("Duplicate category_id values detected")

if null_ids != 0:
    raise RuntimeError("NULL category_id values detected")

print("Gold dim_forum_category validation PASSED")

spark.stop()
