
from airflow.sdk import dag, task
import pandas as pd
from datetime import datetime, timedelta


    
@dag(
    "transform-parquet-to-csv-v1.0.1",
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
)
def transform_parquet_to_csv():

    @task()
    def transform():
        storage_options = {
            'key': 'local-access',
            'secret': 'local-secret',
            'endpoint_url': 'http://minio:9000'
        }

        df = pd.read_csv('s3://test/test.csv', storage_options=storage_options)
        df.to_parquet('s3://test/test.parquet', storage_options=storage_options)
    
    transform()

transform_parquet_to_csv()
