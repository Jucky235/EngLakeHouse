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

POSTGRES_TABLE = 'public."Permission"'

CATALOG = "lakehouse"
NAMESPACE = "bronze"
TABLE = "permission"

ICEBERG_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"

# ============================================================
# PostgreSQL Connection
# ============================================================

parsed = urlparse(DATABASE_URL)

jdbc_url = (
    f"jdbc:postgresql://{parsed.hostname}"
    f"{':' + str(parsed.port) if parsed.port else ''}"
    f"{parsed.path}?sslmode=require"
)

# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BronzePermission")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Read PostgreSQL
# ============================================================

permission_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", unquote(parsed.username))
    .option("password", unquote(parsed.password))
    .option("driver", "org.postgresql.Driver")
    .load()
)

# ============================================================
# Bronze Metadata
# ============================================================

bronze_df = permission_df.withColumn(
    "_bronze_ingested_at",
    current_timestamp()
)

# ============================================================
# Namespace
# ============================================================

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

# ============================================================
# Write Iceberg
# ============================================================

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

spark.table(ICEBERG_TABLE).show(truncate=False)

print("Rows:", spark.table(ICEBERG_TABLE).count())

spark.stop()
