from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from delta.tables import DeltaTable

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def ingest_user_product_interaction(spark: SparkSession, run_date_str: str):
    
    ecommerce_schema = config["paths"]["ecommerce_schema"]
    events_table = f"{ecommerce_schema}.silver_events"
    target_table = f"{ecommerce_schema}.gold_user_product_interaction"

    logger.info(f"RUN_DATE = {run_date_str}")
    try:
        # Load silver events table by run_date
        logger.info(f"Gold user product interaction starting, loading silver_events")

        events_df = spark.table(events_table) \
                         .where(f"event_date = '{run_date_str}'") \
                         .filter(F.col("product_id").isNotNull())
        
        # Aggregate events by user_id and product_id incrementally
        agg_df = events_df.groupBy("user_id","product_id").agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_cnt"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_cnt"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_cnt"),
            F.max("event_time").alias("last_event_time")
        )
        logger.info("Aggregated incremental data")

        # first run 
        if not spark.catalog.tableExists(target_table):
            agg_df.withColumn(
                "has_purchased",
                F.when(F.col("purchase_cnt") > 0, 1).otherwise(0)
            ).write.format("delta").saveAsTable(target_table)
            return

        # Merge incremental data with existing data
        target = DeltaTable.forName(spark, target_table)
        target.alias("t").merge(
            agg_df.alias("s"),
            "t.user_id = s.user_id AND t.product_id = s.product_id"
        ).whenMatchedUpdate(set = {
            "view_cnt": "t.view_cnt + s.view_cnt",
            "add_to_cart_cnt": "t.add_to_cart_cnt + s.add_to_cart_cnt",
            "purchase_cnt": "t.purchase_cnt + s.purchase_cnt",
            "last_event_time": "GREATEST(t.last_event_time, s.last_event_time)",
            "has_purchased": "CASE WHEN (t.purchase_cnt + s.purchase_cnt) > 0 THEN 1 ELSE 0 END"
        }).whenNotMatchedInsert(values = {
            "user_id": "s.user_id",
            "product_id": "s.product_id",
            "view_cnt": "s.view_cnt",
            "add_to_cart_cnt": "s.add_to_cart_cnt",
            "purchase_cnt": "s.purchase_cnt",
            "last_event_time": "s.last_event_time",
            "has_purchased": "CASE WHEN s.purchase_cnt > 0 THEN 1 ELSE 0 END"
        }).execute()

        logger.info("Gold user product interaction incremental merge completed")

    except Exception as e:
        logger.error(f"Gold user product interaction failed: {e}", exc_info=True)
        raise

