
import os
import pandas as pd
from typing import Optional

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import get_current_context
from airflow.datasets import Dataset

from minio import Minio

from operators.modules.md_s3_utils import read_object_bytes
from operators.modules.md_parquet_utils import df_from_parquet_bytes, write_df_to_parquet
from operators.modules.md_dataset_utils import create_dataset_for_path


class FactBreweriesSnapshotOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str = "processed-data",
        outlet_dataset: Optional[Dataset] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.outlet_dataset = outlet_dataset

    def execute(self, context):
        ctx = get_current_context()
        ds: str = ctx["ds"]               # "YYYY-MM-DD"
        year, month, day = ds.split("-")
        ds_nodash = year + month + day    # "YYYYMMDD"

        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        key_dim = f"gold/{year}/{month}/{day}/dim_brewery.parquet"
        raw = read_object_bytes(client, self.bucket, key_dim)
        df_dim = df_from_parquet_bytes(raw)

        df_fact = (
            df_dim
            .groupby(["brewery_type_sk", "location_sk"])
            .size()
            .reset_index(name="brewery_count")
        )
        df_fact["snapshot_date"] = ds

        local_path = f"/tmp/fact_breweries_snapshot_{ds_nodash}.parquet"
        write_df_to_parquet(df_fact, local_path)

        output_key = f"gold/{year}/{month}/{day}/fact_breweries_snapshot.parquet"
        with open(local_path, "rb") as f:
            client.put_object(
                bucket_name=self.bucket,
                object_name=output_key,
                data=f,
                length=os.path.getsize(local_path),
                content_type="application/octet-stream"
            )

        if self.outlet_dataset:
            dataset = self.outlet_dataset
        else:
            dataset = create_dataset_for_path(f"{self.bucket}/{output_key}")

        self.log.info("Dataset criado: %s", dataset.uri)
        self.outlets = [dataset]

        return output_key