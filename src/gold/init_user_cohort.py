from pyspark.sql import SparkSession, functions as F

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def init_user_cohort(spark: SparkSession, run_date_str: str):
    """
    Initialize user_cohort table
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    activity_table = f"{ecommerce_schema}.silver_user_activity_daily"
    target_table = f"{ecommerce_schema}.gold_user_cohort"

    logger.info(f"Initializing gold_user_cohort, run_date = {run_date_str}")

    df = spark.table(activity_table) \
              .where(F.col("dt") <= run_date_str)

    cohort_df = df.groupBy("user_id") \
        .agg(F.min("dt").alias("cohort_date"))

    cohort_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    logger.info("Gold user cohort initialized successfully")

