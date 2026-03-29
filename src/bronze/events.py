from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()

# =========================
# main entry (incremental only)
# =========================
def ingest_events(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    users_path = f"{bronze_base}/users"
    products_path = f"{bronze_base}/products"
    events_path = f"{bronze_base}/events"

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Target path = {events_path}")

    try:
        run_date = F.to_date(F.lit(run_date_str))
        # Load source data
        users_df = spark.read.format("delta").load(users_path).select("user_id", "signup_time")
        products_df = spark.read.format("delta").load(products_path).select("product_id", "created_at")

        # Active users (~7%)
        active_users = users_df.withColumn(
            "hash_val", F.abs(F.hash(F.col("user_id"), run_date)) % 100
        ).filter(F.col("hash_val") < 7).select("user_id", "signup_time")

        # Yesterday new users
        new_users = users_df.filter(F.to_date("signup_time") == run_date).select("user_id", "signup_time")

        users_all = active_users.union(new_users).dropDuplicates(["user_id"])

        # Generate 1~2 events per user
        events_df = (
            users_all
            .withColumn("event_count", (F.rand() * 2 + 1).cast("int"))
            .withColumn("event_array", F.sequence(F.lit(1), F.col("event_count")))
            .withColumn("tmp", F.explode("event_array"))
            .drop("event_array", "event_count", "tmp")
        )

        # Assign product_id
        product_ids = [r["product_id"] for r in products_df.collect()]
        events_df = events_df.withColumn(
            "product_id",
            F.array(*[F.lit(p) for p in product_ids])[(F.rand() * len(product_ids)).cast("int")]
        )

        # Join product created_at
        events_df = events_df.join(F.broadcast(products_df), on="product_id", how="left")

        # Event time
        events_df = events_df.withColumn(
            "event_time",
            F.from_unixtime(F.unix_timestamp(run_date) + (F.rand() * 86400).cast("int")).cast("timestamp")
        )

        # Fix constraints
        events_df = (
            events_df
            .withColumn("event_time", F.when(F.col("event_time") < F.col("signup_time"), F.col("signup_time")).otherwise(F.col("event_time")))
            .withColumn("event_time", F.when(F.col("event_time") < F.col("created_at"), F.col("created_at")).otherwise(F.col("event_time")))
            .drop("created_at")
        )

        # Attributes
        events_df = (
            events_df
            .withColumn("event_type", F.when(F.rand() < 0.75, "view")
                        .when(F.rand() < 0.88, "add_to_cart")
                        .when(F.rand() < 0.97, "purchase")
                        .otherwise("login"))
            .withColumn("device", F.when(F.rand() < 0.7, "mobile").otherwise("web"))
            .withColumn("event_date", F.to_date("event_time"))
            .filter(F.to_date("event_time") == run_date)
        )

        # Write partitioned by event_date
        events_df.select("user_id", "product_id", "event_time", "event_date", "event_type", "device") \
            .repartition(1, "event_date") \
            .write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"event_date = '{run_date_str}'") \
            .partitionBy("event_date") \
            .save(events_path)

        logger.info("Events incremental ingestion completed")

    except Exception as e:
        logger.error(f"Events ingestion failed: {e}", exc_info=True)
        raise