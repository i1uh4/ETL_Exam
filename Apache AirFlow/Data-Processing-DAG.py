import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# =====================================================================
YC_DP_FOLDER_ID            = 'b1gjnhj7rcvkg1em0cnh'
YC_DP_SSH_PUBLIC_KEY       = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBPltCIf3n8QdY2tX9C1I4d12Y8i2mLkUFIMfBJEJSj8 i.l.osipenko@tbank.ru'
YC_DP_SUBNET_ID            = 'e9bfulvo9m9q9iutlane'
YC_DP_SA_ID                = 'ajeq12sa7nr5c5sigfrk'
YC_DP_METASTORE_URI        = '10.128.0.14'
YC_DP_AZ                   = 'ru-central1-a'
YC_DP_GROUP_ID             = 'enplbamq9k7bvt50drka'

YC_BUCKET                  = 'etl-kafka-2task'
YC_SOURCE_BUCKET           = YC_BUCKET
YC_DP_LOGS_BUCKET          = YC_BUCKET
# =====================================================================

with DAG(
        dag_id='DATA_INGEST',
        start_date=datetime.datetime(2026, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['data-processing-and-airflow'],
) as dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Temporary cluster for credit applications PySpark job',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=200,
        computenode_resource_preset='m2.large',
        computenode_disk_type='network-ssd',
        computenode_disk_size=200,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.sql.hive.metastore.sharedPrefixes': 'com.amazonaws,ru.yandex.cloud',
            'spark:spark.sql.warehouse.dir': f's3a://{YC_BUCKET}/warehouse',
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
            'spark:spark.sql.catalogImplementation': 'hive',
        },
        security_group_ids=[YC_DP_GROUP_ID],
    )

    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/create-table.py',
        args=[f's3a://{YC_BUCKET}'],
        properties={
            'spark.submit.deployMode': 'cluster',
            'spark.sql.catalogImplementation': 'hive',
            'spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> create_pyspark_job >> delete_spark_cluster