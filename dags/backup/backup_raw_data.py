from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from minio import Minio
from minio.commonconfig import CopySource

def backup_raw_data(execution_date, **context):
    target_date = (execution_date - timedelta(days=1)).date()
    year  = target_date.year
    month = f"{target_date.month:02d}"
    day   = f"{target_date.day:02d}"

    endpoint   = Variable.get("MINIO_ENDPOINT")
    access_key = Variable.get("MINIO_ACCESS_KEY")
    secret_key = Variable.get("MINIO_SECRET_KEY")
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    src_bucket = "raw-data"
    dst_bucket = "backup"
    if not client.bucket_exists(dst_bucket):
        client.make_bucket(dst_bucket)

    prefixes = [
        f"Breweries/{year}/{month}/{day}",
        f"metadata/{year}/{month}/{day}"
    ]

    for prefix in prefixes:
        for obj in client.list_objects(src_bucket, prefix=prefix, recursive=True):
            src = CopySource(src_bucket, obj.object_name)
            result = client.copy_object(
                dst_bucket,
                obj.object_name,
                src
            )
            print(f"Copied {src_bucket}/{obj.object_name} → {dst_bucket}/{result.object_name}")

with DAG(
    dag_id="backup_raw_data",
    schedule_interval="0 0 * * *",  
    start_date=datetime(2025, 4, 28, tzinfo=timezone.utc),
    catchup=False,
    tags=["backup"],
) as dag:

    t_backup = PythonOperator(
        task_id="backup_raw_data",
        python_callable=backup_raw_data,
    )