from pyspark.sql import functions as F
from src.common.base_validator import BaseValidator
from pyspark.sql import DataFrame


class UserProfileValidator(BaseValidator):

    def validate(self):

        df = self.spark.table(self.table_name)

        # =========================
        #  Null check
        # =========================
        null_cnt = df.filter(F.col("user_id").isNull()).count()
        if null_cnt > 0:
            self.add_error(f"user_id null count = {null_cnt}")

        # =========================
        #  Duplicate check
        # =========================
        dup_cnt = df.groupBy("user_id").count().filter("count > 1").count()
        if dup_cnt > 0:
            self.add_error(f"duplicate user_id count = {dup_cnt}")

        # =========================
        #  Negative value check
        # =========================
        neg_cnt = df.filter(
            (F.col("total_view_cnt") < 0) |
            (F.col("total_purchase_cnt") < 0)
        ).count()

        if neg_cnt > 0:
            self.add_error(f"negative metrics count = {neg_cnt}")

        # =========================
        #  Logical consistency
        # =========================
        logic_cnt = df.filter(
            F.col("total_purchase_cnt") > F.col("total_view_cnt")
        ).count()

        if logic_cnt > 0:
            self.add_error(f"purchase > view count = {logic_cnt}")

        # =========================
        #  Conversion rate check
        # =========================
        invalid_conv = df.filter(
            (F.col("conversion_rate") < 0) |
            (F.col("conversion_rate") > 1)
        ).count()

        if invalid_conv > 0:
            self.add_error(f"invalid conversion_rate = {invalid_conv}")

        # =========================
        #  RFM sanity check
        # =========================
        rfm_cnt = df.filter(
            (F.col("monetary_value") < 0) |
            (F.col("purchase_frequency") < 0)
        ).count()

        if rfm_cnt > 0:
            self.add_error(f"invalid RFM values = {rfm_cnt}")
