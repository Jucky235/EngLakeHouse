import os
from urllib.parse import urlparse, unquote

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")


POSTGRES_TABLE = 'public."ForumCommentVote"'

CATALOG = "lakehouse"
NAMESPACE = "bronze"
TABLE = "forumcommentvote"

ICEBERG_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"


# ============================================================
# PostgreSQL connection
# ============================================================

parsed = urlparse(DATABASE_URL)

db_user = unquote(parsed.username)
db_password = unquote(parsed.password)

jdbc_url = (
    f"jdbc:postgresql://"
    f"{parsed.hostname}"
    f"{':' + str(parsed.port) if parsed.port else ''}"
    f"{parsed.path}"
    f"?sslmode=require"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BronzeForumCommentVote")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read PostgreSQL
# ============================================================

print("\n=== Read PostgreSQL ForumCommentVote table ===")

votes_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)


print("\n=== PostgreSQL schema ===")
votes_df.printSchema()


print("\n=== PostgreSQL data ===")
votes_df.show(truncate=False)


# ============================================================
# Bronze metadata
# ============================================================

print("\n=== Adding Bronze metadata ===")

bronze_df = votes_df.withColumn(
    "_bronze_ingested_at",
    current_timestamp()
)


# ============================================================
# Create namespace
# ============================================================

print(f"\n=== Creating namespace: {CATALOG}.{NAMESPACE} ===")

spark.sql(f"""
    CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}
""")


# ============================================================
# Write Iceberg Bronze table
# ============================================================

print(f"\n=== Writing Bronze table: {ICEBERG_TABLE} ===")

(
    bronze_df.writeTo(ICEBERG_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "zstd")
    .createOrReplace()
)


# ============================================================
# Validate
# ============================================================

print(f"\n=== Reading Bronze table: {ICEBERG_TABLE} ===")

result_df = spark.table(ICEBERG_TABLE)

result_df.show(truncate=False)

row_count = result_df.count()

print(f"\nBronze row count: {row_count}")

print("\n=== Bronze ForumCommentVote ingestion complete ===")


spark.stop()
