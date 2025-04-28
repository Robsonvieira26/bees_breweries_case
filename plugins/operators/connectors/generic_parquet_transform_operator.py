import os
from typing import Callable

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import get_current_context

from minio import Minio

from operators.modules.md_s3_utils import read_object_bytes
from operators.modules.md_parquet_utils import df_from_parquet_bytes, write_df_to_parquet
from operators.modules.md_dataset_utils import create_dataset_for_path


class GenericParquetTransformOperator(BaseOperator):

    template_fields = ('input_key_template', 'output_key_template')

    @apply_defaults
    def __init__(
        self,
        *, 
        bucket_input: str,
        input_key_template: str,
        bucket_output: str,
        output_key_template: str,
        transform_fn: Callable,
        outlet_dataset,
        secure: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_input = bucket_input
        self.input_key_template = input_key_template
        self.bucket_output = bucket_output
        self.output_key_template = output_key_template
        self.transform_fn = transform_fn
        self.outlet_dataset = outlet_dataset
        self.secure = secure

    def execute(self, context):
        ctx = get_current_context()
        ds = ctx['ds']               # "YYYY-MM-DD"
        ds_nodash = ds.replace('-', '')


        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=self.secure)


        input_key  = self.input_key_template
        output_key = self.output_key_template

        data_bytes = read_object_bytes(client, self.bucket_input, input_key)

        df = df_from_parquet_bytes(data_bytes)
        df_transformed = self.transform_fn(df)

        local_path = f"/tmp/{self.task_id}_{ds_nodash}.parquet"
        write_df_to_parquet(df_transformed, local_path)

        with open(local_path, 'rb') as f:
            client.put_object(
                bucket_name=self.bucket_output,
                object_name=output_key,
                data=f,
                length=os.path.getsize(local_path),
                content_type='application/octet-stream'
            )

        self.log.info("Registrando outlet Dataset: %s", self.outlet_dataset.uri)
        self.outlets = [self.outlet_dataset]

        return output_key
