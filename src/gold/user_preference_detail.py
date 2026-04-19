from src.common.logger import get_logger
from src.common.utils import load_config
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window  

logger = get_logger(__name__)
config = load_config()

def build_user_preference_detail(spark: SparkSession):

    """
    Build user preference detail table
    - Preference type: category, brand
    - Preference value: category, brand
    - Preference count: view_cnt, purchase_cnt
    - Preference rank: view rank, purchase rank
    """

    ecommerce_schema = config["paths"]["ecommerce_schema"]

    upi_table = f"{ecommerce_schema}.gold_user_product_interaction"
    products_table = f"{ecommerce_schema}.silver_products"
    target_table = f"{ecommerce_schema}.gold_user_preference_detail"

    logger.info("Building user preference detail, loading data...")

    upi_df = spark.table(upi_table)
    products_df = spark.table(products_table) \
        .select("product_id", "category", "brand")

    df = upi_df.join(products_df, "product_id", "left") \
    
    logger.info("User product interaction and products join completed")

    # =========================
    # Category
    # =========================
    category_df = df.filter(F.col("category").isNotNull()) \
        .groupBy("user_id", "category").agg(
            F.sum("view_cnt").alias("view_cnt"),
            F.sum("purchase_cnt").alias("purchase_cnt")
        ).filter(
           (F.col("view_cnt") > 0) | (F.col("purchase_cnt") > 0)
        )

    view_window = Window.partitionBy("user_id").orderBy(F.desc("view_cnt"), F.desc("purchase_cnt"), F.asc("category"))
    purchase_window = Window.partitionBy("user_id").orderBy(F.desc("purchase_cnt"),F.desc("view_cnt"), F.asc("category"))

    view_rank_df = category_df \
        .withColumn("rank", F.row_number().over(view_window)) \
        .filter(F.col("rank") <= 5) \
        .withColumn("rank_type", F.lit("view"))

    purchase_rank_df = category_df \
        .withColumn("rank", F.row_number().over(purchase_window)) \
        .filter(F.col("rank") <= 5) \
        .withColumn("rank_type", F.lit("purchase"))

    category_rank = view_rank_df.unionByName(purchase_rank_df)

    category_rank = category_rank \
        .withColumn("preference_type", F.lit("category")) \
        .withColumnRenamed("category", "preference_value")

    logger.info("Category aggregation completed")

    # =========================
    # Brand
    # =========================
    brand_df = df.filter(F.col("brand").isNotNull()) \
        .groupBy("user_id", "brand").agg(
            F.sum("view_cnt").alias("view_cnt"),
            F.sum("purchase_cnt").alias("purchase_cnt")
        ).filter(
            (F.col("view_cnt") > 0) | (F.col("purchase_cnt") > 0)
        )

    brand_view_window = Window.partitionBy("user_id").orderBy(F.desc("view_cnt"), F.desc("purchase_cnt"), F.asc("brand"))
    brand_purchase_window = Window.partitionBy("user_id").orderBy(F.desc("purchase_cnt"),F.desc("view_cnt"), F.asc("brand"))

    brand_view_rank_df = brand_df \
        .withColumn("rank", F.row_number().over(brand_view_window)) \
        .filter(F.col("rank") <= 5) \
        .withColumn("rank_type", F.lit("view"))

    brand_purchase_rank_df = brand_df \
        .withColumn("rank", F.row_number().over(brand_purchase_window)) \
        .filter(F.col("rank") <= 5) \
        .withColumn("rank_type", F.lit("purchase"))

    brand_rank = brand_view_rank_df.unionByName(brand_purchase_rank_df)

    brand_rank = brand_rank \
        .withColumn("preference_type", F.lit("brand")) \
        .withColumnRenamed("brand", "preference_value")

    logger.info("Brand aggregation completed")

    # =========================
    # Union
    # =========================
    result_df = category_rank.unionByName(brand_rank)

    result_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
    
    logger.info("User preference detail table built successfully")

