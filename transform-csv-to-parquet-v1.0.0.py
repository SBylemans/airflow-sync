



# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "transform-to-csv-v1.0.0",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tranform dag",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # t1, t2 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    spark_conf = {
        "spark.kubernetes.container.image":"ghcr.io/sbylemans/hera-deploy-spark:v0.0.26",
        "spark.kubernetes.namespace": "airflow",
        "spark.executor.memory":"2G",
        "spark.executor.cores": 2,
        "spark.hadoop.fs.s3a.connection.timeout": 6000,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:10000",
        "spark.hadoop.fs.s3a.access.key": "local-access",
        "spark.hadoop.fs.s3a.secret.key": "local-secret",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
        "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
        "spark.hadoop.fs.s3a.connection.establish.timeout" : 30000,
        "spark.hadoop.fs.s3a.threads.keepalivetime" : 60000,
        "spark.hadoop.fs.s3a.multipart.purge.age" : 86400000
    }

    t2 = SparkSubmitOperator(
        task_id="transform_csv",
        application="local:///opt/spark-app/spark.py",
        conf=spark_conf,
        verbose=True,
        deploy_mode='cluster'
    )
    t1 >> t2
