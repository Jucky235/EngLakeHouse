from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


SOURCE_TABLE = "lakehouse.silver.chat_channel"
TARGET_TABLE = "lakehouse.gold.dim_chat_channel"


spark = (
    SparkSession.builder
    .appName("GoldDimChatChannel")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Reading {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)

gold_df = df.select(
    "channel_id",
    "name",
    "description",
    "icon",
    "status",
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
    .groupBy("channel_id")
    .count()
    .filter("count > 1")
    .count()
)

null_ids = (
    spark.table(TARGET_TABLE)
    .filter("channel_id IS NULL")
    .count()
)

print(f"Duplicate channel IDs: {duplicate_ids}")
print(f"Null channel IDs:      {null_ids}")

if duplicate_ids != 0:
    raise RuntimeError("Duplicate channel_id values detected")

if null_ids != 0:
    raise RuntimeError("NULL channel_id values detected")

print("Gold dim_chat_channel validation PASSED")

spark.stop()
