from pyspark.sql import SparkSession, functions as F

from delta.tables import DeltaTable

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_cohort(spark: SparkSession, run_date_str: str):
    """
    Incremental build for user_cohort
    Only insert new users
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    activity_table = f"{ecommerce_schema}.silver_user_activity_daily"
    target_table = f"{ecommerce_schema}.gold_user_cohort"

    # load daily data
    daily_df = spark.table(activity_table) \
        .where(F.col("dt") == run_date_str) \
        .select("user_id", "dt") \
        .dropDuplicates(["user_id"]) \
        .withColumnRenamed("dt", "cohort_date")


    # merge to target table
    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        daily_df.alias("s"),
        "t.user_id = s.user_id"
    ).whenNotMatchedInsert(
        # only insert new users, ignore old users
        values={
            "user_id": "s.user_id",
            "cohort_date": "s.cohort_date"
        }
    ).execute()

    logger.info("Gold user cohort incrementally updated successfully")