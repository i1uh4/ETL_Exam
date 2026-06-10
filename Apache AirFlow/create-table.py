from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType, DoubleType
)
from pyspark.sql import functions as F
import sys


def main():
    if len(sys.argv) > 1:
        output_path = sys.argv[1].rstrip('/')
    else:
        output_path = "s3a://etl-kafka-2task"

    spark = SparkSession.builder \
        .appName("credit-applications-ingest") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS credit_db")
    spark.sql("USE credit_db")

    num_rows = 2_000_000

    regions = ["DE-HE", "DE-BY", "DE-BE", "DE-NW", "DE-SN", "DE-HH", "DE-BW", "DE-RP"]
    product_types = ["cash_loan", "mortgage", "auto_loan", "credit_card", "consumer_loan"]
    risk_levels = ["low", "medium", "high"]
    decision_statuses = ["approved", "rejected", "manual_review"]
    channels = ["mobile", "web", "branch", "partner", "call_center"]

    df = spark.range(0, num_rows).withColumnRenamed("id", "row_id")

    df = (
        df
        .withColumn(
            "application_id",
            F.concat(F.lit("app_20260501_"), F.lpad(F.col("row_id").cast("string"), 9, "0"))
        )
        .withColumn(
            "event_time",
            (F.lit("2026-05-01 00:00:00").cast(TimestampType()).cast("long")
             + (F.rand(seed=1) * 30 * 24 * 3600).cast("long")
             ).cast(TimestampType())
        )
        .withColumn(
            "customer_id",
            F.concat(F.lit("cust_"), F.lpad((F.rand(seed=2) * 100000).cast("int").cast("string"), 6, "0"))
        )
        .withColumn(
            "region_code",
            F.element_at(F.array(*[F.lit(x) for x in regions]),
                         ((F.rand(seed=3) * len(regions)).cast("int") + 1))
        )
        .withColumn(
            "product_type",
            F.element_at(F.array(*[F.lit(x) for x in product_types]),
                         ((F.rand(seed=4) * len(product_types)).cast("int") + 1))
        )
        .withColumn(
            "requested_amount",
            (F.rand(seed=5) * 95000 + 5000).cast(IntegerType())
        )
        .withColumn(
            "term_months",
            ((F.rand(seed=6) * 10).cast("int") * 6 + 6).cast(IntegerType())
        )
        .withColumn(
            "credit_score",
            (F.rand(seed=7) * 550 + 300).cast(IntegerType())
        )
        .withColumn(
            "risk_level",
            F.when(F.col("credit_score") >= 700, F.lit("low"))
             .when(F.col("credit_score") >= 550, F.lit("medium"))
             .otherwise(F.lit("high"))
        )
        .withColumn(
            "decision_status",
            F.when(F.col("risk_level") == "low", F.lit("approved"))
             .when(F.col("risk_level") == "medium", F.lit("manual_review"))
             .otherwise(F.lit("rejected"))
        )
        .withColumn(
            "approved_amount",
            F.when(F.col("decision_status") == "approved", F.col("requested_amount"))
             .when(F.col("decision_status") == "manual_review",
                   (F.col("requested_amount") * 0.7).cast(IntegerType()))
             .otherwise(F.lit(0))
        )
        .withColumn(
            "channel",
            F.element_at(F.array(*[F.lit(x) for x in channels]),
                         ((F.rand(seed=8) * len(channels)).cast("int") + 1))
        )
        .withColumn(
            "employee_review_flag",
            (F.col("decision_status") == "manual_review")
        )
        .withColumn(
            "processing_time_sec",
            (F.rand(seed=9) * 290 + 10).cast(IntegerType())
        )
        .drop("row_id")
    )

    df = df.select(
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec",
    )

    target_path = f"{output_path}/credit_applications"
    df.write.mode("overwrite").parquet(target_path)

    spark.sql("DROP TABLE IF EXISTS credit_db.credit_applications")
    spark.sql(f"""
        CREATE EXTERNAL TABLE credit_db.credit_applications (
            application_id        STRING,
            event_time            TIMESTAMP,
            customer_id           STRING,
            region_code           STRING,
            product_type          STRING,
            requested_amount      INT,
            term_months           INT,
            credit_score          INT,
            risk_level            STRING,
            decision_status       STRING,
            approved_amount       INT,
            channel               STRING,
            employee_review_flag  BOOLEAN,
            processing_time_sec   INT
        )
        STORED AS PARQUET
        LOCATION '{target_path}'
    """)

    cnt = spark.sql("SELECT COUNT(*) AS cnt FROM credit_db.credit_applications").collect()[0]["cnt"]
    print(f"[INFO] Rows written: {cnt}")

    spark.stop()


if __name__ == "__main__":
    main()