# dags/gold_dim_brewery.py

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag

from operators.connectors.gold.gold_brewery_dim_operator import GoldBreweryDimOperator
from airflow import DAG
from airflow.models import TaskInstance
from airflow.datasets.manager import DatasetManager
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session


@provide_session
def mark_external_change(ti: TaskInstance, session=None):
    dm = DatasetManager()
    dm.register_dataset_change(
        task_instance=ti,
        dataset=Dataset(f"gold_dim_brewery_parquet"),
        session=session,
    )
    

# Datasets de trigger (silver layer)
silver_brewery_types_ds = Dataset("silver_breweries_brewery_types_parquet")
silver_locations_ds    = Dataset("silver_breweries_locations_parquet")
gold_ds = Dataset("gold_dim_brewery_parquet")

with DAG(
    dag_id="gold_dim_brewery",
    schedule=[silver_brewery_types_ds, silver_locations_ds],
    start_date=datetime(2025, 4, 27),
    catchup=False,
    tags=["gold", "dimension"],
) as dag: 
    
    gold_dim_brewery = GoldBreweryDimOperator(
    task_id="load_dim_brewery",
    bucket="processed-data",
    outlets=[gold_ds]
    )
    
    mark_ds = PythonOperator(
    task_id="force_dataset_trigger",
    python_callable=mark_external_change,
    )



gold_dim_brewery >> mark_ds
