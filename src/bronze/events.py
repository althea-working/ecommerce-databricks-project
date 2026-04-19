from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.common.logger import get_logger
from src.common.utils import load_config

logger = get_logger(__name__)
config = load_config()


def rand01(*cols):
    return (F.abs(F.hash(*cols)) % 100000000) / 100000000.0


def ingest_events(spark: SparkSession, run_date_str: str):

    SESSION_GAP_SEC = 30 * 60

    bronze_base = config["paths"]["bronze_base"]
    users_path = f"{bronze_base}/users"
    products_path = f"{bronze_base}/products"
    events_path = f"{bronze_base}/events"

    logger.info(f"RUN_DATE = {run_date_str}")
    run_date = F.to_date(F.lit(run_date_str))

    users_df = spark.read.format("delta").load(users_path) \
        .select("user_id", "signup_time")

    products_df = spark.read.format("delta").load(products_path) \
        .select("product_id", "created_at")

    users_sample = (
        users_df
        .filter(F.to_date("signup_time") <= run_date)
        .withColumn("hash_val", F.abs(F.hash("user_id", F.lit(run_date_str))) % 100)
        .filter("hash_val < 12")
        .select("user_id", "signup_time")
    )

    products_valid = products_df.filter(F.to_date("created_at") <= run_date)

    product_ids = [r["product_id"] for r in products_valid.collect()]
    product_arr = F.array(*[F.lit(p) for p in product_ids])

    products_valid = F.broadcast(products_valid)

    sessions = (
        users_sample
        .withColumn("session_cnt", (rand01("user_id", F.lit(run_date_str)) * 3 + 1).cast("int"))
        .withColumn("arr", F.sequence(F.lit(1), F.col("session_cnt")))
        .withColumn("session_seq", F.explode("arr"))
        .drop("arr", "session_cnt")
        .withColumn("session_id", F.concat_ws("_", "user_id", F.lit(run_date_str), "session_seq"))
        .withColumn(
            "device",
            F.when(F.abs(F.hash("user_id")) % 100 < 70, "mobile").otherwise("web")
        )
        .withColumn(
            "session_start",
            F.when(
                F.to_date("signup_time") == run_date,
                F.from_unixtime(
                    F.unix_timestamp("signup_time")
                    + F.col("session_seq") * 180
                    + (rand01("user_id", "session_id", "session_seq") * 60).cast("int")
                )
            ).otherwise(
                F.from_unixtime(
                    F.unix_timestamp(run_date)
                    + F.when(rand01("user_id") < 0.4, 8 * 3600)
                     .when(rand01("user_id") < 0.8, 20 * 3600)
                     .otherwise(14 * 3600)
                    + (rand01("user_id", "session_seq") * 600).cast("int")
                )
            ).cast("timestamp")
        )
    )

    session_products = (
        sessions
        .withColumn("cnt", (rand01("session_id") * 6 + 5).cast("int"))
        .withColumn("arr", F.sequence(F.lit(1), F.col("cnt")))
        .withColumn("event_seq", F.explode("arr"))
        .drop("arr", "cnt")
        .withColumn(
            "product_id",
            product_arr[(rand01("session_id", "event_seq") * len(product_ids)).cast("int")]
        )
    )

    session_max_created = (
        session_products
        .join(products_valid, "product_id", "left")
        .groupBy("session_id")
        .agg(F.max("created_at").alias("max_created_at"))
    )

    sessions = sessions.join(session_max_created, "session_id", "left") \
        .withColumn(
            "session_start",
            F.when(
                F.col("session_start") < F.col("max_created_at"),
                F.col("max_created_at")
                + (rand01("session_id") * 60).cast("int") * F.expr("INTERVAL 1 SECOND")
            ).otherwise(F.col("session_start"))
        ).drop("max_created_at")

    # =========================
    # session start control cumulative max 
    # =========================
    w = Window.partitionBy("user_id") \
        .orderBy("session_seq") \
        .rowsBetween(Window.unboundedPreceding, 0)

    sessions = sessions.withColumn(
        "max_session_start",
        F.max("session_start").over(w)
    )

    sessions = sessions.withColumn(
        "session_start",
        F.from_unixtime(
            F.unix_timestamp("max_session_start")
            + (F.col("session_seq") - 1) * SESSION_GAP_SEC
        ).cast("timestamp")
    ).drop("max_session_start")

    # =========================
    # Login
    # =========================
    login_df = sessions.select(
        "user_id",
        "session_id",
        "device",
        F.lit(None).cast("string").alias("product_id"),
        F.col("session_start").alias("event_time"),
        F.lit("login").alias("event_type")
    )

    # =========================
    # Build events
    # =========================
    base_df = session_products \
        .drop("session_start") \
        .join(
            sessions.select("session_id", "session_start"),
            "session_id"
        )

    def build_events(df, event_type, event_rank):
        return df.withColumn("event_type", F.lit(event_type)) \
            .withColumn(
                "event_time",
                F.from_unixtime(
                    F.unix_timestamp("session_start")
                    + event_rank * 300
                    + F.col("event_seq") * 20
                    + F.pmod(
                        F.hash("session_id", "event_seq", F.lit(event_type)),
                        15
                    )
                ).cast("timestamp")
            )

    impression_df = build_events(base_df, "impression", 0)

    view_df = impression_df \
        .filter(rand01("session_id", "event_seq", F.lit("view")) < 0.4) \
        .transform(lambda df: build_events(df, "view", 1))

    cart_df = view_df \
        .filter(rand01("session_id", "event_seq", F.lit("cart")) < 0.3) \
        .transform(lambda df: build_events(df, "add_to_cart", 2))

    purchase_df = cart_df \
        .filter(rand01("session_id", "event_seq", F.lit("buy")) < 0.2) \
        .transform(lambda df: build_events(df, "purchase", 3))

    non_login_df = impression_df \
        .unionByName(view_df, True) \
        .unionByName(cart_df, True) \
        .unionByName(purchase_df, True)

    # =========================
    # logout = max(event_time) + buffer
    # =========================
    session_end = (
        non_login_df
        .groupBy("session_id")
        .agg(F.max("event_time").alias("max_event_time"))
    )

    logout_df = session_end \
        .join(sessions.select("session_id", "user_id", "device"), "session_id") \
        .withColumn("has_logout", rand01("session_id") < 0.7) \
        .filter(F.col("has_logout")) \
        .select(
            "user_id",
            "session_id",
            "device",
            F.lit(None).cast("string").alias("product_id"),
            (
                F.col("max_event_time")
                + F.expr("INTERVAL 30 SECONDS")
                + F.pmod(F.hash("session_id"), 10) * F.expr("INTERVAL 1 SECOND")
            ).alias("event_time"),
            F.lit("logout").alias("event_type")
        )

    # =========================
    # Final union
    # =========================
    df = non_login_df \
        .unionByName(login_df, True) \
        .unionByName(logout_df, True)

    df = df.withColumn("event_date", F.to_date("event_time")) \
        .filter(F.col("event_date") == run_date) \
        .dropDuplicates(["user_id", "session_id", "event_time", "event_type", "product_id"]) \
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    "user_id",
                    "session_id",
                    F.col("event_time").cast("string"),
                    "event_type",
                    F.coalesce("product_id", F.lit("null"))
                ),
                256
            )
        )

    df.select(
        "event_id",
        "user_id",
        "session_id",
        "device",
        "product_id",
        "event_type",
        "event_time",
        "event_date"
    ).repartition(1, "event_date") \
     .write.format("delta") \
     .mode("overwrite") \
     .option("replaceWhere", f"event_date = '{run_date_str}'") \
     .partitionBy("event_date") \
     .save(events_path)

    logger.info("✅ FINAL STABLE EVENTS GENERATED")