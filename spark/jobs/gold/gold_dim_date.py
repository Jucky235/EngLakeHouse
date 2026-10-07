from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    explode,
    month,
    quarter,
    sequence,
    to_date,
    weekofyear,
    when,
    year,
)

spark = SparkSession.builder.appName("GoldDimDate").getOrCreate()

TARGET_TABLE = "lakehouse.gold.dim_date"

# Định nghĩa khoảng thời gian sinh lịch (tùy chỉnh nếu hệ thống cần dữ liệu xa hơn)
START_DATE = "2020-01-01"
END_DATE = "2030-12-31"

print(f"=== Generating Date Range from {START_DATE} to {END_DATE} ===")

# 1. Tạo chuỗi danh sách ngày liên tục
date_df = spark.sql(
    f"SELECT sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day) as date_seq"
).select(explode("date_seq").alias("full_date"))

print("=== Building Gold dim_date ===")

gold_df = (
    date_df.select(
        # Surrogate Key dạng INTEGER (VD: 20260917) - Chuẩn Kimball
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        date_format("full_date", "MMM").alias("month_name_short"),
        dayofmonth("full_date").alias("day_of_month"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        date_format("full_date", "EEE").alias("day_name_short"),
        # Cột bổ trợ logic kinh doanh
        when(dayofweek("full_date").isin([1, 7]), True)
        .otherwise(False)
        .alias("is_weekend"),
        weekofyear("full_date").alias("week_of_year"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

print("=== Gold schema ===")
gold_df.printSchema()

print("=== Creating Gold namespace ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

print("=== Writing Gold dim_date ===")

(
    gold_df.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("=== Reading Gold dim_date ===")

result = spark.table(TARGET_TABLE)
result.show(10, truncate=False)

gold_count = result.count()
print(f"Gold dim_date row count: {gold_count}")

print("=== Validation ===")

# Validate tính duy nhất của date_key
duplicate_count = (
    result.groupBy("date_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(
        f"Found {duplicate_count} duplicate date_key values!"
    )

print("✅ Date range generation complete")
print("✅ Date key uniqueness validation passed")
print("=== Gold dim_date complete ===")

spark.stop()
