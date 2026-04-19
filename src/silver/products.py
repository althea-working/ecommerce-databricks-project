from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


# =========================
# transform
# =========================
def transform_products(df: DataFrame):
    """
    Silver layer data cleaning step
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """

    return (
        df
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("price") > 0)
        .withColumn("category", F.lower(F.col("category")))
        .withColumn("brand", F.lower(F.col("brand")))
    )


# =========================
# main entry
# =========================
def ingest_products(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    ecommerce_schema = config["paths"]["ecommerce_schema"]

    products_bronze_path = f"{bronze_base}/products"
    target_table = f"{ecommerce_schema}.silver_products"

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Reading from {products_bronze_path}")
    logger.info(f"Writing to table {target_table}")

    try:
        # Load from bronze
        df = spark.read.format("delta").load(products_bronze_path)

        logger.info(f"Input rows = {df.count()}")

        # Cleaning
        df_clean = transform_products(df)

        # Writing to Silver table
        logger.info("Writing to silver_products (overwrite)")

        df_clean.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)

        logger.info("Silver products ingestion completed")

    except Exception as e:
        logger.error(f"Products ingestion failed: {e}", exc_info=True)
        raise
    