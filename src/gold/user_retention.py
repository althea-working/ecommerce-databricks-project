from pyspark.sql import SparkSession, functions as F

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_retention(spark: SparkSession):
    """
    Build user retention table (full refresh)
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    activity_table = f"{ecommerce_schema}.silver_user_activity_daily"
    cohort_table = f"{ecommerce_schema}.gold_user_cohort"
    target_table = f"{ecommerce_schema}.gold_user_retention"

    logger.info("Building user retention, loading tables...")

    activity_df = spark.table(activity_table)
    cohort_df = spark.table(cohort_table)

    # =========================
    # Join activity with cohort
    # =========================
    df = activity_df.join(cohort_df, "user_id", "inner")

    # =========================
    # Calculate day_n
    # =========================
    df = df.withColumn(
        "day_n",
        F.datediff(F.col("dt"), F.col("cohort_date"))
    ).filter(
       (F.col("day_n") >= 0) & (F.col("day_n") <= 30)
    )

    # =========================
    # Retained users
    # =========================
    retention_df = df.groupBy("cohort_date", "day_n") \
        .agg(F.countDistinct("user_id").alias("retained_users"))

    # =========================
    # Cohort size
    # =========================
    cohort_size_df = cohort_df.groupBy("cohort_date") \
        .agg(F.countDistinct("user_id").alias("cohort_size"))

    # =========================
    # Join + calculate rate
    # =========================
    result_df = retention_df.join(cohort_size_df, "cohort_date", "left") \
        .withColumn(
            "retention_rate",
            F.round(F.col("retained_users") / F.col("cohort_size"), 4)
        )

    # ========================= 
    # Write table
    # =========================
    result_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("cohort_date") \
        .saveAsTable(target_table)

    logger.info("Gold user retention built successfully")

