from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta

from src.common.logger import get_logger
from src.common.utils import load_config
from src.gold.user_profile_validator import UserProfileValidator

logger = get_logger(__name__)
config = load_config()


def ingest_user_profile_snapshot(spark: SparkSession, run_date_str: str, webhook_url: str):
    """
    Hybrid incremental snapshot:
    t-1 snapshot = t-2 snapshot + incremental events (additive metrics)
                  + recompute non-additive metrics

    Idempotent + scalable
    """

    logger.info(f"RUN_DATE = {run_date_str}")

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    events_table = f"{ecommerce_schema}.silver_events"
    products_table = f"{ecommerce_schema}.silver_products"
    upi_table = f"{ecommerce_schema}.gold_user_product_interaction"

    snapshot_table = f"{ecommerce_schema}.gold_user_profile_snapshot"
    current_table = f"{ecommerce_schema}.gold_user_profile_current"

    # =========================
    # Compute previous date
    # =========================
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d")
    prev_date_str = (run_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # =========================
    # Load previous snapshot (t-2)
    # =========================
    prev_df = spark.table(snapshot_table) \
        .where(f"dt = '{prev_date_str}'")

    # =========================
    # Load incremental events (t-1)
    # =========================
    events_df = spark.table(events_table) \
        .where(f"event_date = '{run_date_str}'")

    products_df = spark.table(products_table) \
        .select("product_id", "price")

    # =========================
    #  Incremental additive metrics
    # =========================
    inc_events = events_df.groupBy("user_id").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("inc_view_cnt"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("inc_add_to_cart_cnt"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("inc_purchase_cnt"),
        F.max("event_time").alias("inc_last_active_time"),
        F.max("event_time").alias("inc_last_seen_time"),
        F.count("*").alias("inc_event_frequency"),
        F.when(F.count("*") > 0, 1).otherwise(0).alias("inc_active_days"),
        F.min("event_time").alias("inc_first_seen_time")  # for new users
    )

    # =========================
    #  Incremental RFM
    # =========================
    purchase_df = events_df.filter(
        (F.col("event_type") == "purchase") &
        (F.col("product_id").isNotNull())
    ).join(products_df, "product_id", "left")

    inc_rfm = purchase_df.groupBy("user_id").agg(
        F.max("event_time").alias("inc_last_purchase_time"),
        F.count("*").alias("inc_purchase_frequency"),
        F.coalesce(
            F.sum(F.col("price").cast(DecimalType(28, 2))).cast(DecimalType(28, 2)),
            F.lit(0).cast(DecimalType(28, 2))
        ).alias("inc_monetary_value")
    )

    # =========================
    #  Distinct metrics (FULL recompute from UPI)
    # =========================
    upi_df = spark.table(upi_table)

    distinct_df = upi_df.groupBy("user_id").agg(
        F.countDistinct(
            F.when(F.col("view_cnt") > 0, F.col("product_id"))
        ).alias("distinct_products_viewed"),
        F.countDistinct(
            F.when(F.col("purchase_cnt") > 0, F.col("product_id"))
        ).alias("distinct_products_purchased")
    )

    # =========================
    #  Combine incremental data
    # =========================
    inc_df = inc_events \
        .join(inc_rfm, "user_id", "left")

    # =========================
    #  Merge logic (FULL OUTER JOIN)
    # =========================
    df = prev_df.alias("t").join(
        inc_df.alias("s"),
        "user_id",
        "full_outer"
    ).join(
        distinct_df.alias("d"),
        "user_id",
        "left"
    )

    # =========================
    #  Build new snapshot
    # =========================
    df = df.select(
        F.coalesce("t.user_id", "s.user_id").alias("user_id"),

        # ===== cumulative =====
        (F.coalesce("t.total_view_cnt", F.lit(0)) + F.coalesce("s.inc_view_cnt", F.lit(0))).alias("total_view_cnt"),
        (F.coalesce("t.total_add_to_cart_cnt", F.lit(0)) + F.coalesce("s.inc_add_to_cart_cnt", F.lit(0))).alias("total_add_to_cart_cnt"),
        (F.coalesce("t.total_purchase_cnt", F.lit(0)) + F.coalesce("s.inc_purchase_cnt", F.lit(0))).alias("total_purchase_cnt"),

        # ===== distinct (recomputed) =====
        F.col("d.distinct_products_viewed"),
        F.col("d.distinct_products_purchased"),

        # ===== activity =====
        F.greatest("t.last_active_time", "s.inc_last_active_time").alias("last_active_time"),
        F.coalesce("t.first_seen_time", "s.inc_first_seen_time").alias("first_seen_time"),
        F.greatest("t.last_seen_time", "s.inc_last_seen_time").alias("last_seen_time"),
        (F.coalesce("t.event_frequency", F.lit(0)) + F.coalesce("s.inc_event_frequency", F.lit(0))).alias("event_frequency"),
        (F.coalesce("t.active_days", F.lit(0)) + F.coalesce("s.inc_active_days", F.lit(0))).alias("active_days"),

        # ===== RFM =====
        F.greatest("t.last_purchase_time", "s.inc_last_purchase_time").alias("last_purchase_time"),
        (F.coalesce("t.purchase_frequency", F.lit(0)) + F.coalesce("s.inc_purchase_frequency", F.lit(0))).alias("purchase_frequency"),
        (F.coalesce("t.monetary_value", F.lit(0).cast(DecimalType(28, 2))).cast(DecimalType(28, 2))
         + F.coalesce("s.inc_monetary_value", F.lit(0).cast(DecimalType(28, 2)))).cast(DecimalType(28, 2))
        .cast(DecimalType(28, 2)).alias("monetary_value")
    )

    # =========================
    #  Recompute NON-additive metrics
    # =========================
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

    # =========================
    #  Add partition
    # =========================
    df = df.withColumn("dt", F.lit(run_date_str))

    # =========================
    #  Write snapshot (overwrite partition)
    # =========================
    df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(snapshot_table)

    logger.info("User profile snapshot updated")

    # =========================
    #  Update current table
    # =========================
    df.drop("dt") \
        .write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(current_table)

    logger.info("User profile current table updated")

    # =========================
    #  data validation
    # =========================
    logger.info("Validating data...")

    validator = UserProfileValidator(
        spark,
        table_name=current_table,
        webhook_url=webhook_url
    )

    validator.run()

