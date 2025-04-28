from typing import Any, Dict, Optional
import json
import io

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

from minio import Minio
from minio.error import S3Error


class MinIOUploadOperator(BaseOperator):
    """
    Operator to upload a Python dict as JSON to a specified MinIO bucket using Airflow Variables.

    :param bucket_name: Name of the target bucket in MinIO
    :param object_name: Object key (including any prefix) under which the JSON will be stored
    :param data: Python dict to serialize and upload as JSON (can be templated)
    :param secure: Whether to use HTTPS (False for HTTP)
    """

    template_fields = ('bucket_name', 'object_name', 'data')

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        data: Any,
        secure: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.data = data
        self.secure = secure

    def execute(self, context: Dict[str, Any]) -> None:

        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")

        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.secure
        )

        if not minio_client.bucket_exists(self.bucket_name):
            self.log.info(f"Bucket '{self.bucket_name}' não encontrado. Criando.")
            minio_client.make_bucket(self.bucket_name)
        else:
            self.log.info(f"Bucket '{self.bucket_name}' já existe.")

        if isinstance(self.data, str):
            payload = self.data.encode('utf-8')
        else:
            payload = json.dumps(self.data).encode('utf-8')

        bytes_io = io.BytesIO(payload)
        bytes_io.seek(0)

        try:
            minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                data=bytes_io,
                length=len(payload),
                content_type='application/json'
            )
            self.log.info(f"Objeto '{self.object_name}' enviado com sucesso para '{self.bucket_name}'")
        except S3Error as err:
            self.log.error(f"Falha ao enviar para MinIO: {err}")
            raise
