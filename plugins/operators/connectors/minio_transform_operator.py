import os
import json
from typing import List, Dict

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import get_current_context

from minio import Minio

from operators.modules.md_parquet_utils import write_dicts_to_parquet
from operators.modules.md_dataset_utils import create_dataset_for_path

class MinIOTransformOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        bucket_input: str,
        bucket_output: str = 'processed-data',
        s3_conn_id: str = 'minio_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_input = bucket_input
        self.bucket_output = bucket_output
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        # Contexto e data
        ctx = get_current_context()
        ds: str = ctx['ds']                         # "YYYY-MM-DD"
        year, month, day = ds.split('-')
        ds_nodash = f"{year}{month}{day}"           # "YYYYMMDD"

        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        prefix = f"metadata/{year}/{month}/{day}/"
        key = prefix + "breweries_meta.json"
        self.log.info("Buscando %s/%s", self.bucket_input, key)
        resp = client.get_object(self.bucket_input, key)
        meta = json.loads(resp.read().decode('utf-8'))

        by_state = meta.get("by_state", {})
        items: List[Dict] = [
            {"state": state, "count": count}
            for state, count in by_state.items()
        ]
        if not items:
            self.log.warning("Nenhum dado em by_state para %s", key)

        local_path = f"/tmp/breweries_meta_by_state_{ds_nodash}.parquet"
        write_dicts_to_parquet(items, local_path)

        output_key = (
            f"silver/{year}/{month}/{day}/"
            f"breweries_meta_by_state.parquet"
        )
        with open(local_path, 'rb') as f:
            client.put_object(
                bucket_name=self.bucket_output,
                object_name=output_key,
                data=f,
                length=os.path.getsize(local_path),
                content_type='application/octet-stream'
            )
        self.log.info("Upload concluído: %s/%s", self.bucket_output, output_key)

        dataset = create_dataset_for_path(f"{self.bucket_output}/{output_key}")
        self.log.info("Dataset criado: %s", dataset.uri)
        self.outlets = [dataset]

        return output_key
