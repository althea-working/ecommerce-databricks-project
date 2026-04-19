from pyspark.sql import SparkSession, functions as F

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_funnel_daily(spark: SparkSession, run_date_str: str):
    """
    Build daily user funnel metrics
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    events_table = f"{ecommerce_schema}.silver_events"
    target_table = f"{ecommerce_schema}.gold_user_funnel_daily"

    logger.info(f"Building user funnel daily, run_date = {run_date_str}")

    # =========================
    # Load daily events
    # =========================
    df = spark.table(events_table) \
        .where(f"event_date = '{run_date_str}'")

    # =========================
    # Compute user-level flags
    # =========================
    user_stage_df = df.groupBy("user_id").agg(
        F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_flag"),
        F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_flag"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_flag")
    )

    # =========================
    # Aggregate funnel
    # =========================
    agg_df = user_stage_df.agg(
        F.sum("view_flag").alias("view_users"),
        F.sum("cart_flag").alias("add_to_cart_users"),
        F.sum("purchase_flag").alias("purchase_users")
    )

    # =========================
    # Compute conversion rates
    # =========================
    result = agg_df \
        .withColumn("view_to_cart_rate",
            F.when(F.col("view_users") > 0,
                   F.round(F.col("add_to_cart_users") / F.col("view_users"), 2))
            .otherwise(0.0)
        ) \
        .withColumn("cart_to_purchase_rate",
            F.when(F.col("add_to_cart_users") > 0,
                   F.round(F.col("purchase_users") / F.col("add_to_cart_users"), 2))
            .otherwise(0.0)
        ) \
        .withColumn("view_to_purchase_rate",
            F.when(F.col("view_users") > 0,
                   F.round(F.col("purchase_users") / F.col("view_users"), 2))
            .otherwise(0.0)
        ) \
        .withColumn("dt", F.lit(run_date_str))

    # =========================
    # Write table
    # =========================
    result.select(
        "dt",
        "view_users",
        "add_to_cart_users",
        "purchase_users",
        "view_to_cart_rate",
        "cart_to_purchase_rate",
        "view_to_purchase_rate"
    ).write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(target_table)

    logger.info("Gold user funnel daily built successfully")