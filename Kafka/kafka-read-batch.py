#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)


KAFKA_BOOTSTRAP = 'rc1a-4p4inr4ocsbl6si5.mdb.yandexcloud.net:9091,rc1b-r9k1e8hbcpeaupub.mdb.yandexcloud.net:9091,rc1d-dhukj53b0592or74.mdb.yandexcloud.net:9091'
KAFKA_TOPIC     = 'dataproc-kafka-topic'
OUTPUT_PATH     = 's3a://kafka-read-batch.py.ку/kafka-read-batch-output'


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-read-stream-app").getOrCreate()

    schema = StructType([
        StructField('application_id', StringType()),
        StructField('customer', StructType([
            StructField('customer_id', StringType()),
            StructField('region', StringType()),
        ])),
        StructField('loan', StructType([
            StructField('amount', IntegerType()),
            StructField('term_months', IntegerType()),
        ])),
        StructField('scoring', StructType([
            StructField('score', IntegerType()),
            StructField('risk_level', StringType()),
        ])),
        StructField('documents', ArrayType(StructType([
            StructField('type', StringType()),
            StructField('status', StringType()),
        ]))),
        StructField('decision_status', StringType()),
        StructField('submitted_at', StringType()),
    ])

    query = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) AS value") \
        .where(F.col("value").isNotNull()) \
        .writeStream \
        .trigger(once=True) \
        .queryName("received_messages") \
        .format("memory") \
        .start()

    query.awaitTermination()

    df = spark.sql("select value from received_messages")

    parsed = df.select(F.from_json(F.col("value"), schema).alias("j")).select("j.*")

    flat = (
        parsed.withColumn("document", F.explode_outer("documents"))
              .select(
                  F.col("application_id"),
                  F.col("customer.customer_id").alias("customer_id"),
                  F.col("customer.region").alias("region"),
                  F.col("loan.amount").alias("loan_amount"),
                  F.col("loan.term_months").alias("loan_term_months"),
                  F.col("scoring.score").alias("scoring_score"),
                  F.col("scoring.risk_level").alias("scoring_risk_level"),
                  F.col("document.type").alias("document_type"),
                  F.col("document.status").alias("document_status"),
                  F.col("decision_status"),
                  F.col("submitted_at"),
              )
    )

    flat.coalesce(2).write.mode("overwrite") \
        .option("header", "true") \
        .csv(OUTPUT_PATH)


if __name__ == "__main__":
    main()