from pyspark.sql import SparkSession, functions as F

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def build_user_retention_summary(spark: SparkSession):
    """
    Build retention summary table (D1 / D7 / D30)
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    retention_table = f"{ecommerce_schema}.gold_user_retention"
    target_table = f"{ecommerce_schema}.gold_user_retention_summary"

    logger.info("Building user retention summary, loading retention data...")

    df = spark.table(retention_table)

    # =========================
    # Pivot day_n → columns
    # =========================
    summary_df = df.groupBy("cohort_date", "cohort_size").agg(
    # D1
    F.max(F.when(F.col("day_n") == 1, F.col("retention_rate"))).alias("d1_retention"),
    F.max(F.when(F.col("day_n") == 1, F.col("retained_users"))).alias("d1_users"),

    # D7
    F.max(F.when(F.col("day_n") == 7, F.col("retention_rate"))).alias("d7_retention"),
    F.max(F.when(F.col("day_n") == 7, F.col("retained_users"))).alias("d7_users"),

    # D30
    F.max(F.when(F.col("day_n") == 30, F.col("retention_rate"))).alias("d30_retention"),
    F.max(F.when(F.col("day_n") == 30, F.col("retained_users"))).alias("d30_users")
    )

    # =========================
    # Fill null values
    # =========================
    summary_df = summary_df.fillna({
        "d1_retention": 0.0,
        "d1_users": 0,
        "d7_retention": 0.0,
        "d7_users": 0,
        "d30_retention": 0.0,
        "d30_users": 0
    })

    # =========================
    # Write table
    # =========================
    summary_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("cohort_date") \
        .saveAsTable(target_table)

    logger.info("Gold user retention summary built successfully")