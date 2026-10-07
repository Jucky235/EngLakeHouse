from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    lit,
    row_number,
)
from pyspark.sql.window import Window

# ============================================================
# CONFIG
# ============================================================

TARGET_TABLE = "lakehouse.analytics.user_social_ranking"

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AnalyticsUserSocialRanking")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ GOLD TABLES
# ============================================================

print("=== Reading Gold tables ===")

users = spark.table("lakehouse.gold.dim_user")
chat_messages = spark.table("lakehouse.gold.fact_chat_message")
forum_posts = spark.table("lakehouse.gold.dim_forum_post")
forum_comments = spark.table("lakehouse.gold.dim_forum_comment")
post_votes = spark.table("lakehouse.gold.fact_post_vote")
comment_votes = spark.table("lakehouse.gold.fact_comment_vote")
saved_posts = spark.table("lakehouse.gold.fact_saved_post")

# ============================================================
# 1. CHAT ACTIVITY
# ============================================================

print("=== Aggregating Chat Activity ===")

chat_activity = (
    chat_messages
    .groupBy("sender_id")
    .agg(
        count("*").alias("chat_message_count")
    )
    .withColumnRenamed("sender_id", "user_id")
)

# ============================================================
# 2. FORUM POSTS
# ============================================================

print("=== Aggregating Forum Posts ===")

post_activity = (
    forum_posts
    .groupBy("author_id")
    .agg(
        count("*").alias("forum_post_count")
    )
    .withColumnRenamed("author_id", "user_id")
)

# ============================================================
# 3. FORUM COMMENTS
# ============================================================

print("=== Aggregating Forum Comments ===")

comment_activity = (
    forum_comments
    .groupBy("author_id")
    .agg(
        count("*").alias("forum_comment_count")
    )
    .withColumnRenamed("author_id", "user_id")
)

# ============================================================
# 4. POST UPVOTES RECEIVED
# ============================================================

print("=== Aggregating Post Upvotes Received ===")

post_upvotes_received = (
    post_votes
    .filter(col("vote_type") == "upvote")
    .join(
        forum_posts.select(
            "post_id",
            col("author_id").alias("post_author_id"),
        ),
        on="post_id",
        how="inner",
    )
    .filter(col("user_id") != col("post_author_id"))
    .groupBy("post_author_id")
    .agg(
        count("*").alias("post_upvotes_received")
    )
    .withColumnRenamed("post_author_id", "user_id")
)

# ============================================================
# 5. COMMENT UPVOTES RECEIVED
# ============================================================

print("=== Aggregating Comment Upvotes Received ===")

comment_upvotes_received = (
    comment_votes
    .filter(col("vote_type") == "upvote")
    .join(
        forum_comments.select(
            "comment_id",
            col("author_id").alias("comment_author_id"),
        ),
        on="comment_id",
        how="inner",
    )
    .filter(col("user_id") != col("comment_author_id"))
    .groupBy("comment_author_id")
    .agg(
        count("*").alias("comment_upvotes_received")
    )
    .withColumnRenamed("comment_author_id", "user_id")
)

# ============================================================
# 6. POST VOTES GIVEN
# ============================================================

print("=== Aggregating Post Votes Given ===")

post_votes_given = (
    post_votes
    .groupBy("user_id")
    .agg(
        count("*").alias("post_votes_given")
    )
)

# ============================================================
# 7. COMMENT VOTES GIVEN
# ============================================================

print("=== Aggregating Comment Votes Given ===")

comment_votes_given = (
    comment_votes
    .groupBy("user_id")
    .agg(
        count("*").alias("comment_votes_given")
    )
)

# ============================================================
# 8. POSTS SAVED
# ============================================================

print("=== Aggregating Posts Saved ===")

posts_saved = (
    saved_posts
    .groupBy("user_id")
    .agg(
        count("*").alias("posts_saved")
    )
)

# ============================================================
# 9. COMBINE ALL USER ACTIVITY
# ============================================================

print("=== Combining User Activity Metrics ===")

activity = (
    users
    .select(
        "user_id",
        "name",
    )
    .join(chat_activity, "user_id", "left")
    .join(post_activity, "user_id", "left")
    .join(comment_activity, "user_id", "left")
    .join(post_upvotes_received, "user_id", "left")
    .join(comment_upvotes_received, "user_id", "left")
    .join(post_votes_given, "user_id", "left")
    .join(comment_votes_given, "user_id", "left")
    .join(posts_saved, "user_id", "left")
)

# ============================================================
# 10. NULL → 0
# ============================================================

metric_columns = [
    "chat_message_count",
    "forum_post_count",
    "forum_comment_count",
    "post_upvotes_received",
    "comment_upvotes_received",
    "post_votes_given",
    "comment_votes_given",
    "posts_saved",
]

for metric in metric_columns:
    activity = activity.withColumn(
        metric,
        coalesce(col(metric), lit(0)),
    )

# ============================================================
# 11. CALCULATE SOCIAL SCORE
# ============================================================

print("=== Calculating Social Score ===")

activity = activity.withColumn(
    "social_score",
    (
        col("chat_message_count") * 1
        + col("forum_post_count") * 5
        + col("forum_comment_count") * 2
        + col("post_upvotes_received") * 3
        + col("comment_upvotes_received") * 2
        + col("post_votes_given") * 1
        + col("comment_votes_given") * 1
        + col("posts_saved") * 1
    ),
)

# ============================================================
# 12. RANK USERS
# ============================================================

print("=== Ranking Users ===")

ranking_window = Window.orderBy(
    col("social_score").desc(),
    col("user_id").asc(),
)

ranking = (
    activity
    .withColumn(
        "social_rank",
        row_number().over(ranking_window),
    )
    .withColumn("_analytics_processed_at", current_timestamp())
    .select(
        "social_rank",
        "user_id",
        "name",
        "social_score",
        *metric_columns,
        "_analytics_processed_at",
    )
)

# ============================================================
# 13. WRITE ICEBERG TABLE
# ============================================================

print("=== Creating Analytics namespace if not exists ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.analytics")

print(f"=== Writing {TARGET_TABLE} ===")

(
    ranking.writeTo(TARGET_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

# ============================================================
# 14. VALIDATION
# ============================================================

print("=== Validation ===")

result = spark.table(TARGET_TABLE)

user_count = users.count()
ranking_count = result.count()

print(f"User count:    {user_count}")
print(f"Ranking count: {ranking_count}")

# 1. Total count validation
if user_count != ranking_count:
    raise RuntimeError(
        f"❌ Validation Failed: Count mismatch! Users={user_count}, Ranking={ranking_count}"
    )

# 2. Null user_id validation
null_user_ids = result.filter(col("user_id").isNull()).count()
if null_user_ids > 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {null_user_ids} rows with NULL user_id!"
    )

# 3. Duplicate user_id validation
duplicate_user_ids = (
    result.groupBy("user_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if duplicate_user_ids > 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {duplicate_user_ids} duplicate user_id rows!"
    )

# 4. Social score non-negative validation
negative_scores = result.filter(col("social_score") < 0).count()
if negative_scores > 0:
    raise RuntimeError(
        f"❌ Validation Failed: Found {negative_scores} rows with negative social_score!"
    )

print("✅ User count validation passed")
print("✅ Primary Key (user_id) null validation passed")
print("✅ Primary Key (user_id) uniqueness validation passed")
print("✅ Social score non-negative validation passed")

# ============================================================
# PREVIEW
# ============================================================

print("\nTop 10 Socially Active Users:")
(
    result
    .select(
        "social_rank",
        "user_id",
        "name",
        "social_score",
        "forum_post_count",
        "forum_comment_count",
        "post_upvotes_received",
    )
    .orderBy(col("social_rank").asc())
    .show(10, truncate=False)
)

print("=== User social ranking analytics complete successfully ===")

spark.stop()
