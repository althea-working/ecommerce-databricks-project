from src.common.logger import get_logger
from src.common.utils import load_config
from pyspark.sql import SparkSession, functions as F

logger = get_logger(__name__)
config = load_config()


def build_user_preference_summary(spark: SparkSession):

    """
    Build user preference summary table (one row per user)
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    detail_table = f"{ecommerce_schema}.gold_user_preference_detail"
    target_table = f"{ecommerce_schema}.gold_user_preference_summary"

    logger.info("Building user preference summary...")

    df = spark.table(detail_table)

    # =========================
    # Top1 category (view)
    # =========================
    top_view_category = df.filter(
        (F.col("preference_type") == "category") &
        (F.col("rank_type") == "view") &
        (F.col("rank") == 1)
    ).select(
        "user_id",
        F.col("preference_value").alias("top_view_category"),
        F.col("view_cnt").alias("top_view_category_cnt")
    )

    # =========================
    # Top1 category (purchase)
    # =========================
    top_purchase_category = df.filter(
        (F.col("preference_type") == "category") &
        (F.col("rank_type") == "purchase") &
        (F.col("rank") == 1)
    ).select(
        "user_id",
        F.col("preference_value").alias("top_purchase_category"),
        F.col("purchase_cnt").alias("top_purchase_category_cnt")
    )

    # =========================
    # Top1 brand (view)
    # =========================
    top_view_brand = df.filter(
        (F.col("preference_type") == "brand") &
        (F.col("rank_type") == "view") &
        (F.col("rank") == 1)
    ).select(
        "user_id",
        F.col("preference_value").alias("top_view_brand"),
        F.col("view_cnt").alias("top_view_brand_cnt")
    )

    # =========================
    # Top1 brand (purchase)
    # =========================
    top_purchase_brand = df.filter(
        (F.col("preference_type") == "brand") &
        (F.col("rank_type") == "purchase") &
        (F.col("rank") == 1)
    ).select(
        "user_id",
        F.col("preference_value").alias("top_purchase_brand"),
        F.col("purchase_cnt").alias("top_purchase_brand_cnt")
    )

    # =========================
    # Top3 categories (view)
    # =========================
    top3_view_categories = df.filter(
        (F.col("preference_type") == "category") &
        (F.col("rank_type") == "view") &
        (F.col("rank") <= 3)
    ).groupBy("user_id").agg(
        F.collect_list("preference_value").alias("top3_view_categories")
    )

    # =========================
    # Top3 categories (purchase)
    # =========================
    top3_purchase_categories = df.filter(
        (F.col("preference_type") == "category") &
        (F.col("rank_type") == "purchase") &
        (F.col("rank") <= 3)
    ).groupBy("user_id").agg(
        F.collect_list("preference_value").alias("top3_purchase_categories")
    )

    # =========================
    # Combine all
    # =========================
    result_df = top_view_category \
        .join(top_purchase_category, "user_id", "full_outer") \
        .join(top_view_brand, "user_id", "full_outer") \
        .join(top_purchase_brand, "user_id", "full_outer") \
        .join(top3_view_categories, "user_id", "left") \
        .join(top3_purchase_categories, "user_id", "left")

    # =========================
    # Write table
    # =========================
    result_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    logger.info("User preference summary built successfully")
    