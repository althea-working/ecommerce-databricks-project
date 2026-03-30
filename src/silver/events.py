from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


# =========================
# transform
# =========================
def transform_events(df):
    """
    Silver layer cleaning
    """
    valid_event_types = ["view", "add_to_cart", "purchase", "login"]

    return (
        df
        # Key columns not null
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        # Time resonable
        .filter(F.col("event_time") <= F.current_timestamp())
        # Normal event type
        .filter(F.col("event_type").isin(valid_event_types))
        # Standardize device
        .withColumn("device", F.lower(F.col("device")))
    )


# =========================
# main
# =========================
def ingest_events(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    silver_schema = config["paths"]["silver_schema"]

    events_bronze_path = f"{bronze_base}/events"
    target_table = f"{silver_schema}.silver_events"

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Reading from {events_bronze_path}")
    logger.info(f"Writing to {target_table}")

    try:
        # Load from bronze (only run_date partition)
        df = spark.read.format("delta") \
            .load(events_bronze_path) \
            .where(f"event_date = '{run_date_str}'")

        logger.info(f"Input rows = {df.count()}")

        # Cleaning
        df_clean = transform_events(df)

        logger.info(f"Output rows = {df_clean.count()}")

        # Writing to silver table
        df_clean.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"event_date = '{run_date_str}'") \
            .partitionBy("event_date") \
            .saveAsTable(target_table)

        logger.info("Silver events ingestion completed")

    except Exception as e:
        logger.error(f"Events ingestion failed: {e}", exc_info=True)
        raise
    