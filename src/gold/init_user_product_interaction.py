from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def ingest_user_product_interaction(spark: SparkSession, run_date_str: str):
    """
    Iint user-product-interaction data from the source system and write to the target system.
    """
    ecommerce_schema = config["paths"]["ecommerce_schema"]
    events_table = f"{ecommerce_schema}.silver_events"
    target_table = f"{ecommerce_schema}.gold_user_product_interaction"

    try:
        
        logger.info(f"RUN_DATE = {run_date_str}")

        # Load events data from the source system
        df = spark.table(events_table) \
                  .where(F.col("event_date") <= run_date_str) \
                  .filter(F.col("product_id").isNotNull())
        logger.info("Iint user-product-interaction data, loading full events")

        # Aggregation
        agg_df = (
            df.groupBy("user_id", "product_id").agg(
                F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_cnt"),
                F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_cnt"),
                F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_cnt"),
                F.max("event_time").alias("last_event_time")
            )
        ).withColumn(
            "has_purchased",
            F.when(F.col("purchase_cnt") > 0, 1).otherwise(0)
        )

        # Write to the target table
        agg_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)

        logger.info("User product interaction completed")
    except Exception as e:
        logger.error(f"Init user_product_interaction failed: {e}", exc_info=True)
        raise