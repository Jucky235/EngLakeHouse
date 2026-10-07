import os
from urllib.parse import urlparse, unquote

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# ============================================================
# Configuration
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")


POSTGRES_TABLE = 'public."Role"'

CATALOG = "lakehouse"
NAMESPACE = "bronze"
TABLE = "role"

ICEBERG_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"


# ============================================================
# PostgreSQL Connection
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

# Iceberg / REST Catalog / MinIO configuration is provided
# globally through /opt/spark/conf/spark-defaults.conf.

spark = (
    SparkSession.builder
    .appName("BronzeRole")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read PostgreSQL
# ============================================================

print("\n=== Read PostgreSQL Role table ===")

roles_df = (
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
roles_df.printSchema()


print("\n=== PostgreSQL data ===")
roles_df.show(truncate=False)


# ============================================================
# Add Bronze Metadata
# ============================================================

print("\n=== Adding Bronze metadata ===")

bronze_df = roles_df.withColumn(
    "_bronze_ingested_at",
    current_timestamp()
)


# ============================================================
# Create Bronze Namespace
# ============================================================

print(f"\n=== Creating namespace: {CATALOG}.{NAMESPACE} ===")

spark.sql(f"""
    CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}
""")


# ============================================================
# Write Bronze Iceberg Table
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

print("\n=== Bronze Role ingestion complete ===")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()
