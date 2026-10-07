from pyspark.sql import SparkSession


# ============================================================
# Configuration
# ============================================================

CATALOG = "lakehouse"
NAMESPACE = "bronze"
TABLE = "users"

TABLE_IDENTIFIER = f"{CATALOG}.{NAMESPACE}.{TABLE}"

REST_CATALOG_URI = "http://iceberg-rest:8181"
MINIO_ENDPOINT = "http://minio:9000"


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ReadBronzeIceberg")

    # Iceberg Spark extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # REST Catalog
    .config(
        f"spark.sql.catalog.{CATALOG}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.type",
        "rest",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.uri",
        REST_CATALOG_URI,
    )

    # Iceberg S3 FileIO
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    # MinIO
    .config(
        f"spark.sql.catalog.{CATALOG}.s3.endpoint",
        MINIO_ENDPOINT,
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.s3.path-style-access",
        "true",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.s3.access-key-id",
        "minioadmin",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.s3.secret-access-key",
        "minioadmin123",
    )

    .getOrCreate()
)


# ============================================================
# Spark Configuration
# ============================================================

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.iceberg.vectorization.enabled",
    "false",
)


# ============================================================
# Read Bronze Table
# ============================================================

print("\n=== Reading Bronze Iceberg table ===")
print(f"Table: {TABLE_IDENTIFIER}")

df = spark.table(TABLE_IDENTIFIER)

df.orderBy("id").show(truncate=False)


# ============================================================
# Row Count
# ============================================================

print("\n=== Row count ===")

row_count = df.count()

print(f"Rows: {row_count}")


# ============================================================
# Schema
# ============================================================

print("\n=== Schema ===")

df.printSchema()


# ============================================================
# Iceberg Snapshots
# ============================================================

print("\n=== Iceberg snapshots ===")

spark.sql(f"""
    SELECT
        committed_at,
        snapshot_id,
        parent_id,
        operation,
        summary
    FROM {TABLE_IDENTIFIER}.snapshots
    ORDER BY committed_at
""").show(truncate=False)


# ============================================================
# Iceberg History
# ============================================================

print("\n=== Iceberg history ===")

spark.sql(f"""
    SELECT
        made_current_at,
        snapshot_id,
        parent_id,
        is_current_ancestor
    FROM {TABLE_IDENTIFIER}.history
    ORDER BY made_current_at
""").show(truncate=False)


# ============================================================
# Data Files
# ============================================================

print("\n=== Iceberg data files ===")

spark.sql(f"""
    SELECT
        file_path,
        file_format,
        record_count,
        file_size_in_bytes
    FROM {TABLE_IDENTIFIER}.files
""").show(truncate=False)


# ============================================================
# Validation
# ============================================================

print("\n=== Validation ===")

if row_count > 0:
    print("✓ Table contains data")
else:
    print("✗ Table is empty")


print("\n=== Read test successful ===")
print(f"Catalog:      {CATALOG}")
print(f"Namespace:    {NAMESPACE}")
print(f"Table:        {TABLE_IDENTIFIER}")
print(f"REST Catalog: {REST_CATALOG_URI}")
print(f"MinIO:        {MINIO_ENDPOINT}")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()
