from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_activity_metrics(spark: SparkSession, run_date_str: str):
    """
    Build DAU / WAU / MAU metrics
    """

    logger.info(f"RUN_DATE = {run_date_str}")

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    activity_table = f"{ecommerce_schema}.silver_user_activity_daily"
    target_table = f"{ecommerce_schema}.gold_user_activity_metrics"

    run_date = datetime.strptime(run_date_str, "%Y-%m-%d")

    start_7 = (run_date - timedelta(days=6)).strftime("%Y-%m-%d")
    start_30 = (run_date - timedelta(days=29)).strftime("%Y-%m-%d")

    # =========================
    # Load data window
    # =========================
    df = spark.table(activity_table) \
        .where(F.col("dt") <= run_date_str) \
        .where(F.col("dt") >= start_30)

    logger.info(f"Loaded user activity records in recent 30 days: {df.count()}")

    # =========================
    # DAU
    # =========================
    dau = df.filter(F.col("dt") == run_date_str) \
        .agg(F.countDistinct("user_id").alias("dau"))

    # =========================
    # WAU
    # =========================
    wau = df.filter(F.col("dt") >= start_7) \
        .agg(F.countDistinct("user_id").alias("wau"))

    # =========================
    # MAU
    # =========================
    mau = df.agg(F.countDistinct("user_id").alias("mau"))

    # =========================
    # Combine
    # =========================
    result = dau.crossJoin(wau).crossJoin(mau) \
        .withColumn("dt", F.lit(run_date_str)) \
        .select("dt", "dau", "wau", "mau")

    logger.info(f"Metrics computed: {result.collect()}")

    # =========================
    # Write
    # =========================
    result.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"dt = '{run_date_str}'") \
        .partitionBy("dt") \
        .saveAsTable(target_table)

    logger.info("Gold user activity metrics updated successfully")