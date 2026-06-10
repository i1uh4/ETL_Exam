#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


KAFKA_BOOTSTRAP = 'rc1a-4p4inr4ocsbl6si5.mdb.yandexcloud.net:9091,rc1b-r9k1e8hbcpeaupub.mdb.yandexcloud.net:9091,rc1d-dhukj53b0592or74.mdb.yandexcloud.net:9091'
KAFKA_TOPIC     = 'dataproc-kafka-topic'

NUM_MESSAGES = 80_000


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    regions    = ['DE-HE', 'DE-BY', 'DE-BE', 'DE-NW', 'DE-SN', 'DE-HH', 'DE-BW', 'DE-RP']
    risks      = ['low', 'medium', 'high']
    decisions  = ['approved', 'rejected', 'manual_review']
    doc_types  = ['passport', 'income_statement', 'employment_contract', 'utility_bill']
    doc_status = ['verified', 'pending', 'rejected']

    df = spark.range(0, NUM_MESSAGES).withColumnRenamed('id', 'n')

    df = (
        df
        .withColumn('application_id',
                    F.concat(F.lit('loan_'),
                             F.lpad(F.col('n').cast('string'), 7, '0')))
        .withColumn('customer',
                    F.struct(
                        F.concat(F.lit('cust_'),
                                 F.lpad((F.rand(seed=1) * 100000).cast('int').cast('string'), 6, '0')
                                 ).alias('customer_id'),
                        F.element_at(F.array(*[F.lit(x) for x in regions]),
                                     ((F.rand(seed=2) * len(regions)).cast('int') + 1)).alias('region')
                    ))
        .withColumn('loan',
                    F.struct(
                        (F.rand(seed=3) * 95000 + 5000).cast(IntegerType()).alias('amount'),
                        (((F.rand(seed=4) * 10).cast('int') * 6) + 6).cast(IntegerType()).alias('term_months')
                    ))
        .withColumn('scoring',
                    F.struct(
                        (F.rand(seed=5) * 550 + 300).cast(IntegerType()).alias('score'),
                        F.element_at(F.array(*[F.lit(x) for x in risks]),
                                     ((F.rand(seed=6) * len(risks)).cast('int') + 1)).alias('risk_level')
                    ))
        .withColumn('documents',
                    F.array(
                        F.struct(
                            F.element_at(F.array(*[F.lit(x) for x in doc_types]),
                                         ((F.rand(seed=7) * len(doc_types)).cast('int') + 1)).alias('type'),
                            F.element_at(F.array(*[F.lit(x) for x in doc_status]),
                                         ((F.rand(seed=8) * len(doc_status)).cast('int') + 1)).alias('status')
                        ),
                        F.struct(
                            F.element_at(F.array(*[F.lit(x) for x in doc_types]),
                                         ((F.rand(seed=9) * len(doc_types)).cast('int') + 1)).alias('type'),
                            F.element_at(F.array(*[F.lit(x) for x in doc_status]),
                                         ((F.rand(seed=10) * len(doc_status)).cast('int') + 1)).alias('status')
                        )
                    ))
        .withColumn('decision_status',
                    F.element_at(F.array(*[F.lit(x) for x in decisions]),
                                 ((F.rand(seed=11) * len(decisions)).cast('int') + 1)))
        .withColumn('submitted_at',
                    F.from_unixtime(
                        F.lit(1746094511) + (F.rand(seed=12) * 30 * 24 * 3600).cast('long'),
                        "yyyy-MM-dd'T'HH:mm:ss'Z'"
                    ))
        .drop('n')
    )

    out = df.select(F.to_json(F.struct(*df.columns)).alias('value'))

    out.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()


if __name__ == "__main__":
    main()