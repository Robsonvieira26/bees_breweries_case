# plugins/operators/connectors/gold_brewery_dim_operator.py

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

class GoldBreweryDimOperator(BaseOperator):

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
        ds: str = ctx["ds"]             # "YYYY-MM-DD"
        year, month, day = ds.split("-")
        ds_nodash = ds.replace("-", "")  # "YYYYMMDD"

        
        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        # lê Parquets silver
        prefix = f"silver/{year}/{month}/{day}"
        # breweries
        key_b = f"{prefix}/breweries.parquet"
        df_b = df_from_parquet_bytes(read_object_bytes(client, self.bucket, key_b))
        # brewery types
        key_t = f"{prefix}/brewery_types.parquet"
        df_t = df_from_parquet_bytes(read_object_bytes(client, self.bucket, key_t))
        df_t = df_t.sort_values("brewery_type").reset_index(drop=True)
        df_t["brewery_type_sk"] = df_t.index + 1
        # locations
        key_l = f"{prefix}/locations.parquet"
        df_l = df_from_parquet_bytes(read_object_bytes(client, self.bucket, key_l))
        df_l = df_l.rename(columns={"location_id": "location_sk"})

        
        df = (
            df_b
            .merge(df_t[["brewery_type", "brewery_type_sk"]], on="brewery_type", how="left")
            .merge(
                df_l[["city", "state", "country", "postal_code", "location_sk"]],
                on=["city", "state", "country", "postal_code"],
                how="left"
            )
        )
        df_dim = df[["id","name","brewery_type_sk","location_sk","website_url","phone"]].copy()
        df_dim = df_dim.rename(columns={"id": "brewery_id"})
        df_dim = df_dim.reset_index(drop=True)
        df_dim["brewery_sk"] = df_dim.index + 1
        df_dim = df_dim[[
            "brewery_sk","brewery_id","name",
            "brewery_type_sk","location_sk","website_url","phone"
        ]]

        #
        local_path = f"/tmp/dim_brewery_{ds_nodash}.parquet"
        write_df_to_parquet(df_dim, local_path)

        # upload em gold/YYYY/MM/DD/
        output_key = f"gold/{year}/{month}/{day}/dim_brewery.parquet"
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
