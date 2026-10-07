from urllib.parse import urlparse, parse_qs

from pyspark.sql import SparkSession


# ============================================================
# Configuration
# ============================================================

DATABASE_URL = "DATABASE_URL"

# Set this to the table you want to test.
# Example: "users", "Role", "Question", etc.
TARGET_TABLE = "User"


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("NeonPostgreSQLRead")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read DATABASE_URL
# ============================================================

import os

database_url = os.getenv(DATABASE_URL)

if not database_url:
    raise RuntimeError(
        "DATABASE_URL is not set inside the Spark container."
    )


# ============================================================
# Parse Neon PostgreSQL URL
# ============================================================

parsed = urlparse(database_url)

if parsed.scheme not in ("postgres", "postgresql"):
    raise RuntimeError(
        f"Unsupported database URL scheme: {parsed.scheme}"
    )

host = parsed.hostname
port = parsed.port or 5432
database = parsed.path.lstrip("/")

username = parsed.username
password = parsed.password

query_params = parse_qs(parsed.query)

sslmode = query_params.get("sslmode", ["require"])[0]


if not all([host, database, username, password]):
    raise RuntimeError(
        "DATABASE_URL is missing host, database, username, or password."
    )


jdbc_url = (
    f"jdbc:postgresql://{host}:{port}/{database}"
    f"?sslmode={sslmode}"
)


print("\n=== Neon PostgreSQL Configuration ===")
print(f"Host:     {host}")
print(f"Port:     {port}")
print(f"Database: {database}")
print(f"User:     {username}")
print(f"SSL Mode: {sslmode}")


# ============================================================
# JDBC Properties
# ============================================================

jdbc_properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver",
}


# ============================================================
# Test Connection
# ============================================================

print("\n=== Testing PostgreSQL connection ===")

connection_test = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT 1 AS connected) AS connection_test",
    properties=jdbc_properties,
)

connection_test.show()

print("✓ Neon PostgreSQL connection successful")


# ============================================================
# Discover PostgreSQL Tables
# ============================================================

print("\n=== Discovering PostgreSQL tables ===")

tables_query = """
(
    SELECT
        table_schema,
        table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN (
          'pg_catalog',
          'information_schema'
      )
    ORDER BY table_schema, table_name
) AS tables
"""

tables_df = spark.read.jdbc(
    url=jdbc_url,
    table=tables_query,
    properties=jdbc_properties,
)

tables_df.show(
    truncate=False,
)


# ============================================================
# Read Target Table
# ============================================================

print(f"\n=== Reading table: {TARGET_TABLE} ===")

target_table_df = spark.read.jdbc(
    url=jdbc_url,
    table=f'"{TARGET_TABLE}"',
    properties=jdbc_properties,
)


# ============================================================
# Table Schema
# ============================================================

print("\n=== Table Schema ===")

target_table_df.printSchema()


# ============================================================
# Table Data
# ============================================================

print("\n=== Table Data ===")

target_table_df.show(
    20,
    truncate=False,
)


# ============================================================
# Row Count
# ============================================================

print("\n=== Row Count ===")

row_count = target_table_df.count()

print(f"Rows fetched: {row_count}")


# ============================================================
# Complete
# ============================================================

print("\n=== Neon PostgreSQL read successful ===")
print(f"Source:    {host}")
print(f"Table:     {TARGET_TABLE}")
print(f"Rows:      {row_count}")


spark.stop()
