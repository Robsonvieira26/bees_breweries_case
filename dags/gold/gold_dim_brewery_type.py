# dags/gold/gold_dim_brewery_type.py

from datetime import datetime
import pandas as pd

from datetime import datetime
import pandas as pd

from airflow.datasets import Dataset
from airflow import DAG

from operators.connectors.generic_parquet_transform_operator import GenericParquetTransformOperator
from airflow.models import TaskInstance
from airflow.datasets.manager import DatasetManager
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session


@provide_session
def mark_external_change(ti: TaskInstance, session=None):
    dm = DatasetManager()
    dm.register_dataset_change(
        task_instance=ti,
        dataset=Dataset(f"gold_dim_brewery_type_parquet"),
        session=session,
    )
    


# Dataset de trigger (silver)
silver_brewery_types_ds = Dataset("silver_breweries_brewery_types_parquet")

# Dataset de saída (gold)
gold_brewery_type_ds = Dataset("gold_dim_brewery_type_parquet")

def transform_brewery_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame raw de brewery_types e
    retorna a dimensão dim_brewery_type com:
     - brewery_type_sk  (surrogate key)
     - brewery_type     (natural key)
     - description
    """
    df_dim = (
        df[['brewery_type', 'description']]
        .drop_duplicates()
        .sort_values('brewery_type')
        .reset_index(drop=True)
    )
    df_dim['brewery_type_sk'] = df_dim.index + 1
    return df_dim[['brewery_type_sk', 'brewery_type', 'description']]

with DAG(
    dag_id="gold_dim_brewery_type",
    start_date=datetime(2025, 4, 27),
    catchup=False,
    schedule=[silver_brewery_types_ds],   # dispara no update desse silver Dataset
    tags=["gold", "dimension"],
) as dag:
      
    gold_dim_brewery_type = GenericParquetTransformOperator(
        task_id="build_dim_brewery_type",
        bucket_input="processed-data",
        input_key_template=(
            "silver/"
            "{{ ds_nodash[0:4] }}/"
            "{{ ds_nodash[4:6] }}/"
            "{{ ds_nodash[6:8] }}/"
            "brewery_types.parquet"
        ),
        bucket_output="processed-data",
        output_key_template=(
            "gold/"
            "{{ ds_nodash[0:4] }}/"
            "{{ ds_nodash[4:6] }}/"
            "{{ ds_nodash[6:8] }}/"
            "dim_brewery_type.parquet"
        ),
        transform_fn=transform_brewery_type,
        outlet_dataset=gold_brewery_type_ds,  
        secure=False,
    )

    mark_ds = PythonOperator(
    task_id="force_dataset_trigger",
    python_callable=mark_external_change,
    )


gold_dim_brewery_type >> mark_ds


