



# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

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
        # Point spark to your Kubernetes API server (in-cluster example below)
        'spark.master': 'k8s://https://192.168.49.2:8443',
        # Kubernetes namespace where Spark driver/executors will be created
        'spark.kubernetes.namespace': 'spark',
        # Image that contains Spark runtime + your app (or at least Spark runtime and access to application)
        'spark.kubernetes.container.image': 'apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu',
        # Service account for the driver pod (must have RBAC to create executor pods)
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
        # Optional: pod template to control volumes, nodeSelector, tolerations, etc.
        # 'spark.kubernetes.driver.podTemplateFile': '/opt/airflow/pod_template_driver.yaml',
        # 'spark.kubernetes.executor.podTemplateFile': '/opt/airflow/pod_template_executor.yaml',
        # Tweak resources
        'spark.kubernetes.executor.request.cores': '500m',
        'spark.kubernetes.executor.memory': '2g',
        'spark.kubernetes.driver.memory': '2g',
        # Any other spark confs you need
    }

    t2 = SparkSubmitOperator(
        task_id="transform_csv",
        application="https://github.com/SBylemans/airflow-sync/blob/main/spark.py",
        conn_id='spark_default',
        conf=spark_conf,
        verbose=True,
    )
    t1 >> t2
