from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder
    .appName("GoldFactDeckProgress")
    .getOrCreate()
)

SOURCE_TABLE = "lakehouse.silver.user_deck_progress"
TARGET_TABLE = "lakehouse.gold.fact_deck_progress"

print("=== Reading Silver user_deck_progress table ===")

df = spark.table(SOURCE_TABLE)

print("=== Silver schema ===")
df.printSchema()

silver_count = df.count()
print(f"Silver row count: {silver_count}")

print("=== Building Gold fact_deck_progress ===")

gold_df = (
    df.select(
        col("progress_id"),
        col("user_id"),
        col("deck_id"),
        col("total_cards_viewed"),
        col("mastered_cards"),
        col("last_studied_at"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace ===")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
""")

print("=== Writing Gold fact_deck_progress ===")

(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Gold fact_deck_progress ===")

result = spark.table(TARGET_TABLE)

result.show(truncate=False)

gold_count = result.count()
print(f"Gold row count: {gold_count}")

print("=== Validation ===")

if silver_count != gold_count:
    raise RuntimeError(
        f"Row count mismatch! Silver={silver_count}, Gold={gold_count}"
    )

duplicate_count = (
    result.groupBy("progress_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(
        f"Found {duplicate_count} duplicate progress_id values!"
    )

null_id_count = (
    result
    .filter(col("progress_id").isNull())
    .count()
)

if null_id_count != 0:
    raise RuntimeError(
        f"Found {null_id_count} rows with NULL progress_id!"
    )

invalid_values = (
    result
    .filter(
        (col("total_cards_viewed") < 0)
        | (col("mastered_cards") < 0)
        | (col("mastered_cards") > col("total_cards_viewed"))
    )
    .count()
)

if invalid_values != 0:
    raise RuntimeError(
        f"Found {invalid_values} rows with invalid progress values!"
    )

print("✅ Row count validation passed")
print("✅ Progress uniqueness validation passed")
print("✅ Progress ID null validation passed")
print("✅ Progress value validation passed")
print("=== Gold fact_deck_progress complete ===")

spark.stop()
