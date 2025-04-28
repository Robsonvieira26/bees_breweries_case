import os
import json
from typing import List, Dict
import datetime
import pandas as pd  
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import get_current_context

from minio import Minio

from operators.modules.md_s3_utils import list_objects_for_date
from operators.modules.md_dataset_utils import create_dataset_for_path


class MinIOTransformListOperator(BaseOperator):


    @apply_defaults
    def __init__(
        self,
        bucket_input: str,
        bucket_output: str = 'processed-data',
        s3_conn_id: str = 'minio_default',
        always_emit_outlet: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_input = bucket_input
        self.bucket_output = bucket_output
        self.s3_conn_id = s3_conn_id
        self.always_emit_outlet = always_emit_outlet

    def execute(self, context):
        # Contexto e data
        ctx = get_current_context()
        ds: str = ctx['ds']            # "YYYY-MM-DD"
        year, month, day = ds.split('-')
        ds_nodash = f"{year}{month}{day}"  # "YYYYMMDD"

        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        prefix = f"Breweries/{year}/{month}/{day}/"
        object_names: List[str] = list_objects_for_date(client, self.bucket_input, prefix)

        all_items: List[Dict] = []
        for obj_name in object_names:
            resp = client.get_object(self.bucket_input, obj_name)
            page_data = json.loads(resp.read().decode('utf-8'))

            all_items.extend(page_data)

        print(f"Total de registros: {len(all_items)}")
        self.log.info(f"Total de registros: {len(all_items)}")
        print(all_items[0:2])

        df = pd.DataFrame(all_items)
        df['created_at'] = datetime.datetime.now()
        self.log.info(f"DataFrame criado com {len(df)} linhas e colunas {list(df.columns)}")

        local_path = f"/tmp/breweries_{ds_nodash}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_path)

        output_key = f"silver/{year}/{month}/{day}/breweries.parquet"
        with open(local_path, 'rb') as f:
            client.put_object(
                bucket_name=self.bucket_output,
                object_name=output_key,
                data=f,
                length=os.path.getsize(local_path),
                content_type='application/octet-stream'
            )

        dataset = create_dataset_for_path(f"{self.bucket_output}/{output_key}")
        self.log.info(f"Dataset criado: {dataset.uri}")
        if self.always_emit_outlet:
            self.outlets = [dataset]
        else:
            self.log.info("Nenhuma mudança detectada, mas forçando emissão de outlet")
            self.outlets = [dataset]

        return output_key
