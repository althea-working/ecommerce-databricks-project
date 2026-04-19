from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def init_user_profile(spark: SparkSession, run_date_str: str):
    """
    Full initialization of user profile snapshot table (partitioned by dt)
    and materialize current table for serving layer.
    """

    logger.info(f"RUN_DATE = {run_date_str}")

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    events_table = f"{ecommerce_schema}.silver_events"
    products_table = f"{ecommerce_schema}.silver_products"
    upi_table = f"{ecommerce_schema}.gold_user_product_interaction"

    # snapshot table (partitioned)
    snapshot_table = f"{ecommerce_schema}.gold_user_profile_snapshot"

    # serving table (latest state)
    current_table = f"{ecommerce_schema}.gold_user_profile_current"

    logger.info("Initializing gold user profile snapshot")

    # =========================
    # Load data (FULL history)
    # =========================
    upi_df = spark.table(upi_table)

    events_df = spark.table(events_table) \
        .where(f"event_date <= '{run_date_str}'")

    products_df = spark.table(products_table) \
        .select("product_id", "price")

    # ==================================================
    # Behavior metrics (from user_product_interaction)
    # ==================================================
    agg_upi = upi_df.groupBy("user_id").agg(
        F.sum("view_cnt").alias("total_view_cnt"),
        F.sum("add_to_cart_cnt").alias("total_add_to_cart_cnt"),
        F.sum("purchase_cnt").alias("total_purchase_cnt"),

        F.countDistinct(
            F.when(F.col("view_cnt") > 0, F.col("product_id"))
        ).alias("distinct_products_viewed"),

        F.countDistinct(
            F.when(F.col("purchase_cnt") > 0, F.col("product_id"))
        ).alias("distinct_products_purchased"),

        F.max("last_event_time").alias("last_active_time")
    )

    # ==================================================
    #  RFM metrics (based on purchase events)
    # ==================================================
    purchase_df = events_df.filter(
        (F.col("event_type") == "purchase") &
        (F.col("product_id").isNotNull())
    ).join(products_df, "product_id", "left")

    rfm_df = purchase_df.groupBy("user_id").agg(
        F.max("event_time").alias("last_purchase_time"),
        F.count("*").alias("purchase_frequency"),
        F.coalesce(
            F.sum(F.col("price").cast(DecimalType(28, 2))).cast(DecimalType(28, 2)),
            F.lit(0).cast(DecimalType(28, 2))
        ).alias("monetary_value")
    )

    # ==================================================
    #  Activity metrics
    # ==================================================
    activity_df = events_df.groupBy("user_id").agg(
        F.countDistinct("event_date").alias("active_days"),
        F.min("event_time").alias("first_seen_time"),
        F.max("event_time").alias("last_seen_time"),
        F.count("*").alias("event_frequency")
    )

    # ==================================================
    #  Join all metrics
    # ==================================================
    df = agg_upi \
        .join(activity_df, "user_id", "left") \
        .join(rfm_df, "user_id", "left")

    df = df.fillna({
        "purchase_frequency": 0,
        "monetary_value": 0.0
    })

    # ==================================================
    #  Derived metrics
    # ==================================================
    df = df.withColumn(
        "recency_days",
        F.when(
            F.col("last_purchase_time").isNotNull(),
            F.datediff(F.to_date(F.lit(run_date_str)), F.to_date("last_purchase_time"))
        ).otherwise(F.lit(9999))
    ).withColumn(
        "lifetime_days",
        F.datediff("last_seen_time", "first_seen_time") + 1
    ).withColumn(
        "conversion_rate",
        F.when(
            F.col("total_view_cnt") > 0,
            F.round(F.col("total_purchase_cnt") / F.col("total_view_cnt"), 4)
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "is_high_value_user",
        F.when(F.col("total_purchase_cnt") >= 5, 1).otherwise(0)
    )

    # ==================================================
    #  Add partition column (dt)
    # ==================================================
    df = df.withColumn("dt", F.lit(run_date_str))

    # ==================================================
    #  Write snapshot table (partition overwrite)
    # ==================================================
    df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(snapshot_table)

    logger.info("Snapshot table written successfully")

    # ==================================================
    #  Materialize current table (latest snapshot)
    # ==================================================
    current_df = df.drop("dt")

    current_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(current_table)

    logger.info("Current table updated successfully")