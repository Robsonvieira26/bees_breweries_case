# dags/fact_breweries_snapshot.py

from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag

from operators.connectors.gold.fact_breweries_snapshot_operator import FactBreweriesSnapshotOperator
from airflow.models import TaskInstance
from airflow.datasets.manager import DatasetManager
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session


@provide_session
def mark_external_change(ti: TaskInstance, session=None):
    dm = DatasetManager()
    dm.register_dataset_change(
        task_instance=ti,
        dataset=Dataset(f"fact_breweries_snapshot_parquet"),
        session=session,
    )
    



gold_brewery_ds = Dataset("gold_dim_brewery_parquet")

fact_ds = Dataset("fact_breweries_snapshot_parquet")

with DAG(
    dag_id="fact_breweries_snapshot",
    schedule=[gold_brewery_ds],   
    start_date=datetime(2025, 4, 27),
    catchup=False,
    tags=["gold","fact"],
) as dag:
    
    fact_breweries_snapshot = FactBreweriesSnapshotOperator(
        task_id="compute_fact_breweries_snapshot",
        bucket="processed-data",
        outlet_dataset=fact_ds
    )
    
    mark_ds = PythonOperator(
    task_id="force_dataset_trigger",
    python_callable=mark_external_change,
    )



fact_breweries_snapshot >> mark_ds
