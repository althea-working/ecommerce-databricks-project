from pyspark.sql import SparkSession, functions as F
from datetime import datetime

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_activity_daily(spark: SparkSession, run_date_str: str):
    """
    Build user_activity_daily (incremental)
    - One row per user per day
    """

    logger.info(f"RUN_DATE = {run_date_str}")

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    events_table = f"{ecommerce_schema}.silver_events"
    target_table = f"{ecommerce_schema}.silver_user_activity_daily"

    # =========================
    # Load incremental events
    # =========================
    df = spark.table(events_table) \
        .where(F.col("event_date") == run_date_str)

    logger.info(f"Loaded events for {run_date_str}: {df.count()}")

    # =========================
    # Build daily activity
    # =========================
    activity_df = df.select(
        "user_id",
        F.col("event_date").alias("dt")
    ).distinct()

    logger.info(f"User activity rows: {activity_df.count()}")

    # =========================
    # Write (overwrite partition)
    # =========================
    activity_df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(target_table)

    logger.info("Silver user activity daily updated successfully")