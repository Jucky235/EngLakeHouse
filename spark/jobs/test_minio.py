from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("MinIOConnectionTest")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35),
]

df = spark.createDataFrame(
    data,
    ["id", "name", "age"],
)

print("=== Data ===")
df.show()

output_path = "s3a://warehouse/test"

print(f"Writing to {output_path}")

df.write.mode("overwrite").parquet(output_path)

print("Reading back from MinIO...")

result = spark.read.parquet(output_path)

result.show()

print("=== Spark -> MinIO test successful ===")

spark.stop()
