from pyspark.sql import functions as F
from pyspark.sql import Window, SparkSession, DataFrame
from delta.tables import DeltaTable


from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()

def transform_df(df: DataFrame):
    """
    Clean + deduplicate users
    """

    window_spec = Window.partitionBy("user_id").orderBy(F.col("update_time").desc())

    # Remove null users
    df = (
         df.filter(F.col("user_id").isNotNull())
           # Fill null country and city
           .fillna({"country": "NZ", "city": "Unknown"})
           # Deduplicate
           .withColumn("rn", F.row_number().over(window_spec))
           .filter(F.col("rn") == 1)
           .drop("rn")
           # Convert signup_time to date
           .withColumn("signup_date", F.to_date("signup_time")) 
        )

    return df

def ingest_users_silver(spark: SparkSession, run_date_str: str):
    """
    Ingest user data from bronze to silver
    """

    bronze_base = config["paths"]["bronze_base"]
    bronze_path = f"{bronze_base}/users"

    target_table = "dev.ecommerce.silver_users"

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Source path = {bronze_path}")
    logger.info(f"Target table = {target_table}")

    try:
        #==========================
        # Read bronze data
        #==========================

        df = spark.read.format("delta").load(bronze_path)

        #==========================
        # cleaning
        #==========================
        cleaned_df = transform_df(df)

        #==========================
        # merge data to target table
        #==========================

        if spark.catalog.tableExists(target_table):
            logger.info("merging data into existing silver_users")
            target = DeltaTable.forName(spark, target_table)
            (
                target.alias("t")
                .merge(
                    cleaned_df.alias("s"),
                    "t.user_id = s.user_id",
                ).whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute()
            )
        else:
            (
                cleaned_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(target_table)
            )

        logger.info("Silver users ingestion completed")
    except Exception as e:
        logger.error(f"Silver users ingestion failed: {e}", exc_info=True)
        raise 

   
    