from datetime import datetime
import pandas as pd

from airflow.datasets import Dataset
from airflow import DAG

from operators.connectors.generic_parquet_transform_operator import GenericParquetTransformOperator
from airflow.models import TaskInstance
from airflow.datasets.manager import DatasetManager
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session

TRANSFORM_OBJ="locations"

@provide_session
def mark_external_change(ti: TaskInstance, session=None):
    dm = DatasetManager()
    dm.register_dataset_change(
        task_instance=ti,
        dataset=Dataset(f"silver_breweries_{TRANSFORM_OBJ}_parquet"),
        session=session,
    )
    

input_ds = Dataset("silver_breweries_parquet")

output_ds = Dataset(f"silver_breweries_{TRANSFORM_OBJ}_parquet")

def transform_fn(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera o dataframe silver_locations a partir do dataframe de input.
    
    Parâmetros:
    -----------
    df : pd.DataFrame
        DataFrame original contendo colunas como city, state_province, country, postal_code, etc.
    
    Retorna:
    --------
    silver_locations : pd.DataFrame
        DataFrame com colunas:
         - location_id (surrogate key inteiro começando em 1)
         - city
         - state (renomeado de state_province)
         - country
         - postal_code
    """
    # Seleciona e renomeia colunas relevantes
    silver_locations = (
        df[['city', 'state_province', 'country', 'postal_code']]
        .drop_duplicates()
        .rename(columns={'state_province': 'state'})
        .reset_index(drop=True)
    )
    
    # Cria chave surrogate location_id
    silver_locations.insert(
        0,
        'location_id',
        silver_locations.index + 1
    )
    
    return silver_locations


with DAG(
    dag_id=f"silver_transform_{TRANSFORM_OBJ}",
    start_date=datetime(2025, 4, 27),
    schedule=[input_ds],
    catchup=False,
    tags=['silver', 'transform','minio'],
) as dag:


    transform_operator_call = GenericParquetTransformOperator(
        task_id="transform_operator",
        bucket_input="processed-data",
        input_key_template=(
            "silver/"
            "{{ ds_nodash[0:4] }}/"
            "{{ ds_nodash[4:6] }}/"
            "{{ ds_nodash[6:8] }}/"
            "breweries.parquet"
        ),
        bucket_output="processed-data",
        output_key_template= "silver/{{ ds_nodash[0:4] }}/"
            "{{ ds_nodash[4:6] }}/"
            "{{ ds_nodash[6:8] }}/"
            f"{TRANSFORM_OBJ}.parquet",
        transform_fn=transform_fn,
        outlet_dataset=output_ds,
        secure=False,
    )

    
    mark_ds = PythonOperator(
    task_id="force_dataset_trigger",
    python_callable=mark_external_change,
    )


transform_operator_call >> mark_ds
