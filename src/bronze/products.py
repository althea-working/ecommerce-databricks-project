from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()

# =========================
# utils
# =========================
def get_max_id(spark: SparkSession, path: str, prefix: str):
    max_id = (
        spark.read.format("delta").load(path)
        .select(F.max(F.expr(f"cast(split({prefix},'_')[1] as int)")))
        .collect()[0][0]
    )
    return max_id or 0

# =========================
# generate products
# =========================
def generate_daily_products(spark: SparkSession, run_date_str: str, num_products: int, start_id: int):

    logger.info(f"Generating {num_products} products for date {run_date_str}")

    run_date = F.to_date(F.lit(run_date_str))

    return (
        spark.range(start_id, start_id + num_products)
        .withColumn("product_id", F.concat(F.lit("product_"), F.col("id")))
        .withColumn(
            "category",
            F.when(F.col("id") % 5 == 0, "electronics")
            .when(F.col("id") % 5 == 1, "fashion")
            .when(F.col("id") % 5 == 2, "home")
            .when(F.col("id") % 5 == 3, "beauty")
            .otherwise("sports")
        )
        .withColumn("brand", F.concat(F.lit("brand_"), (F.rand() * 100).cast("int")))
        .withColumn("price", F.round(F.rand() * 500 + 20, 2))
        .withColumn(
            "created_at",
            F.from_unixtime(F.unix_timestamp(run_date) + (F.rand() * 86400).cast("int")).cast("timestamp")
        )
        .withColumn("update_time", F.current_timestamp())
        .drop("id")
    )

# =========================
# main entry (incremental only)
# =========================
def ingest_products(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    products_path = f"{bronze_base}/products"
    num_products = config["ingestion"]["products_per_day"]

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Target path = {products_path}")

    max_product_id = get_max_id(spark, products_path, "product_id")
    logger.info(f"Current max product_id = {max_product_id}")

    new_products = generate_daily_products(
        spark, run_date_str, num_products, max_product_id + 1
    ).repartition(1)

    logger.info("Merging incremental products")

    products_table = DeltaTable.forPath(spark, products_path)
    products_table.alias("t").merge(
        new_products.alias("s"),
        "t.product_id = s.product_id"
    ).whenNotMatchedInsertAll().execute()

    logger.info("Products incremental ingestion completed")
    