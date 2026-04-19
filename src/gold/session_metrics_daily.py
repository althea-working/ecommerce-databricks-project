from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()

def build_session_metrics_daily(spark: SparkSession, run_date_str: str):
    """
    Build session-level metrics
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]
    events_table = f"{ecommerce_schema}.silver_events"
    target_table = f"{ecommerce_schema}.gold_session_metrics_daily"

    logger.info(f"Building session level metrics, run_date = {run_date_str}")

    # ======================================
    # Load data from silver events
    # ======================================
    events_df = spark.table(events_table) \
                     .where(f"event_date = '{run_date_str}'") \
                     .select(
                        "session_id",
                        "user_id",
                        "event_time",
                        "event_type"
                     ) \
                     .filter(F.col("session_id").isNotNull())

    # ======================================
    # Aggregate data by session_id, user_id
    # ======================================
    session_metrics = events_df.groupBy("session_id","user_id").agg(
        F.min(F.col("event_time")).alias("session_start_time"),
        F.max(F.col("event_time")).alias("session_end_time"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("page_view_count"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
    )

    # ==============================
    # Derived metrics
    # ==============================
    session_metrics = session_metrics \
        .withColumn(
            "session_duration", 
            F.when(
                F.col("session_end_time") >= F.col("session_start_time"),
                F.col("session_end_time").cast("long") - F.col("session_start_time").cast("long")
            ).otherwise(0)
        ).withColumn(
            "is_bounce", 
            F.when(F.col("event_count") == 1, 1).otherwise(0)
        ).withColumn(
            "has_purchase", 
            F.when(F.col("purchase_count") > 0, 1).otherwise(0)
        ).withColumn(
            "dt", 
            F.lit(run_date_str)
        ).drop("purchase_count")

    # =========================
    # Write table
    # =========================
    session_metrics.select(
        "dt",
        "session_id",
        "user_id",
        "session_start_time",
        "session_end_time",
        "event_count",
        "page_view_count",
        "session_duration",
        "is_bounce",
        "has_purchase"
    ).write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(target_table)

    logger.info("Gold session metrics daily built successfully")
