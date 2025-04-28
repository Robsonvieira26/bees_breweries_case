from airflow import DAG
from airflow.datasets import Dataset

from datetime import datetime
from operators.connectors.minio_transform_operator import MinIOTransformOperator
from airflow.models import TaskInstance
from airflow.datasets.manager import DatasetManager
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session

@provide_session
def mark_external_change(ti: TaskInstance, session=None):
    dm = DatasetManager()
    dm.register_dataset_change(
        task_instance=ti,
        dataset=Dataset("silver_bystate_parquet"),
        session=session,
    )
    
with DAG(
    dag_id='silver_breweries_bystate_dataset',
    start_date=datetime(2025, 4, 27),
    schedule_interval='20 * * * *',
    catchup=False,
    tags=['silver','minio'],
) as dag:

    parquet_ds = Dataset("silver_bystate_parquet")

    transform_meta_by_state = MinIOTransformOperator(
        task_id='transform_meta_by_state',
        bucket_input='raw-data',
        bucket_output='processed-data',
        s3_conn_id='minio_default',
        outlets=[parquet_ds]
    )
    
    mark_ds = PythonOperator(
    task_id="force_dataset_trigger",
    python_callable=mark_external_change,
    )

transform_meta_by_state >> mark_ds