from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "lakehouse"
NAMESPACE = "bronze"
TABLE = "users"
TABLE_IDENTIFIER = f"{CATALOG}.{NAMESPACE}.{TABLE}"

REST_CATALOG_URI = "http://iceberg-rest:8181"
MINIO_ENDPOINT = "http://minio:9000"
WAREHOUSE = "s3://warehouse/"


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("IcebergRestCatalogTest")

    # --------------------------------------------------------
    # Iceberg Spark extensions
    # --------------------------------------------------------
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # --------------------------------------------------------
    # Iceberg REST Catalog
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # Iceberg S3 FileIO
    # --------------------------------------------------------
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    # --------------------------------------------------------
    # MinIO
    # --------------------------------------------------------
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

spark.conf.set(
    "spark.sql.iceberg.vectorization.enabled",
    "false",
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Test Data
# ============================================================

print("\n=== Creating test data ===")

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
])

initial_data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35),
]

df_initial = spark.createDataFrame(
    initial_data,
    schema,
)

df_initial.show()


# ============================================================
# Create Namespace
# ============================================================

print("\n=== Creating Bronze namespace ===")

spark.sql(f"""
    CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}
""")


# ============================================================
# Reset Test Table
# ============================================================

print("\n=== Resetting Bronze Iceberg table ===")

spark.sql(f"""
    DROP TABLE IF EXISTS {TABLE_IDENTIFIER}
""")


# ============================================================
# Create Iceberg Table
# ============================================================

print("\n=== Creating Bronze Iceberg table ===")

spark.sql(f"""
    CREATE TABLE {TABLE_IDENTIFIER} (
        id INT,
        name STRING,
        age INT
    )
    USING iceberg
""")


# ============================================================
# Write Initial Data
# ============================================================

print("\n=== Writing initial data to Bronze ===")

df_initial.writeTo(
    TABLE_IDENTIFIER
).append()


# ============================================================
# Read Initial Data
# ============================================================

print("\n=== Reading Bronze Iceberg table ===")

spark.sql(f"""
    SELECT *
    FROM {TABLE_IDENTIFIER}
    ORDER BY id
""").show()


# ============================================================
# Second Test Write
# ============================================================

print("\n=== Writing second batch ===")

second_data = [
    (4, "David", 40),
    (5, "Emma", 28),
]

df_second = spark.createDataFrame(
    second_data,
    schema,
)

df_second.show()

df_second.writeTo(
    TABLE_IDENTIFIER
).append()


# ============================================================
# Read Final Data
# ============================================================

print("\n=== Reading final Bronze Iceberg table ===")

spark.sql(f"""
    SELECT *
    FROM {TABLE_IDENTIFIER}
    ORDER BY id
""").show()


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
# Iceberg Files
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
# Table Information
# ============================================================

print("\n=== Iceberg table information ===")

spark.sql(f"""
    DESCRIBE TABLE EXTENDED {TABLE_IDENTIFIER}
""").show(truncate=False)


# ============================================================
# Final Validation
# ============================================================

print("\n=== Final validation ===")

row_count = spark.sql(f"""
    SELECT COUNT(*) AS count
    FROM {TABLE_IDENTIFIER}
""").collect()[0]["count"]

print(f"Row count: {row_count}")

if row_count == 5:
    print("✓ Row count validation passed")
else:
    print(f"✗ Row count validation failed: expected 5, got {row_count}")


print("\n=== Iceberg REST Catalog -> MinIO test successful ===")

print(f"Catalog:      {CATALOG}")
print(f"Namespace:    {NAMESPACE}")
print(f"Table:        {TABLE_IDENTIFIER}")
print(f"REST Catalog: {REST_CATALOG_URI}")
print(f"Warehouse:    {WAREHOUSE}")
print(f"MinIO:        {MINIO_ENDPOINT}")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()
