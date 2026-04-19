from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


# =========================
# transform
# =========================
def transform_events(df: DataFrame, spark: SparkSession):
    """
    Silver layer cleaning & standardization
    """
    valid_event_types = [
        "login",
        "impression",
        "view",
        "add_to_cart",
        "purchase",
        "logout"
    ]

    bronze_base = config["paths"]["bronze_base"]

    users_df = spark.read.format("delta").load(f"{bronze_base}/users") \
        .select("user_id", "signup_time")

    products_df = spark.read.format("delta").load(f"{bronze_base}/products") \
        .select("product_id", "created_at")

    df = (
        df
        # basic validation
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("event_time").isNotNull())

        # event_type
        .filter(F.col("event_type").isin(valid_event_types))

        # product rule
        .filter(
            (F.col("event_type").isin("login", "logout") & F.col("product_id").isNull()) |
            (~F.col("event_type").isin("login", "logout") & F.col("product_id").isNotNull())
        )

        # device
        .filter(F.col("device").isin("mobile", "web"))

        # time sanity
        .filter(F.col("event_time") <= F.current_timestamp())

        # dedup
        .dropDuplicates(["event_id"])
    )

    # user constraint
    df = df.join(users_df, "user_id", "left") \
           .filter(F.col("event_time") >= F.col("signup_time")) \
           .drop("signup_time")

    # product constraint
    df = df.join(products_df, "product_id", "left") \
           .filter(
               (F.col("product_id").isNull()) |
               (F.col("event_time") >= F.col("created_at"))
           ) \
           .drop("created_at")

    return df

# =========================
# main
# =========================
def ingest_events(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    ecommerce_schema = config["paths"]["ecommerce_schema"]

    source_path = f"{bronze_base}/events"
    target_table = f"{ecommerce_schema}.silver_events"

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Source = {source_path}")
    logger.info(f"Target = {target_table}")

    try:
        # -------------------------
        # 1. read only one partition
        # -------------------------
        df = (
            spark.read.format("delta")
            .load(source_path)
            .where(f"event_date = '{run_date_str}'")
        )

        input_cnt = df.count()
        logger.info(f"Input rows = {input_cnt}")

        # -------------------------
        # 2. transform
        # -------------------------
        df_clean = transform_events(df, spark)

        output_cnt = df_clean.count()
        logger.info(f"Output rows = {output_cnt}")

        # -------------------------
        # 3. write (idempotent)
        # -------------------------
        (
            df_clean
            .repartition(1, "event_date")
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"event_date = '{run_date_str}'")
            .partitionBy("event_date")
            .saveAsTable(target_table)
        )

        logger.info("✅ Silver events ingestion completed")

    except Exception as e:
        logger.error(f"❌ Silver events ingestion failed: {e}", exc_info=True)
        raise