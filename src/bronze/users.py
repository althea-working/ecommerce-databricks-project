from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.common.logger import get_logger
from src.common.utils import load_config


logger = get_logger(__name__)
config = load_config()

cities = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]

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
# generate users
# =========================
def generate_daily_users(spark, run_date_str, num_users, start_id):

    logger.info(f"Generating {num_users} users for date {run_date_str}")

    run_date = F.to_date(F.lit(run_date_str))
    city_array = F.array(*[F.lit(c) for c in cities])

    return (
        spark.range(start_id, start_id + num_users)
        .withColumn("user_id", F.concat(F.lit("user_"), F.col("id")))
        .withColumn("country", F.lit("NZ"))
        .withColumn("city", city_array[(F.rand() * len(cities)).cast("int")])
        .withColumn(
            "signup_time",
            F.from_unixtime(
                F.unix_timestamp(run_date) + (F.rand() * 86400).cast("int")
            ).cast("timestamp")
        )
        .withColumn("update_time", F.current_timestamp())
        .drop("id")
    )

# =========================
# main entry (incremental only)
# =========================
def ingest_users(spark: SparkSession, run_date_str: str):

    bronze_base = config["paths"]["bronze_base"]
    users_path = f"{bronze_base}/users"
    num_users = config["ingestion"]["users_per_day"]

    logger.info(f"RUN_DATE = {run_date_str}")
    logger.info(f"Target path = {users_path}")

    max_user_id = get_max_id(spark, users_path, "user_id")
    logger.info(f"Current max user_id = {max_user_id}")

    new_users = generate_daily_users(
        spark, run_date_str, num_users, max_user_id + 1
    ).repartition(1)

    logger.info("Merging incremental users")

    users_table = DeltaTable.forPath(spark, users_path)

    users_table.alias("t").merge(
        new_users.alias("s"),
        "t.user_id = s.user_id"
    ).whenNotMatchedInsertAll().execute()

    logger.info("Users incremental ingestion completed")