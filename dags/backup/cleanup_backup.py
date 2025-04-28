from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from minio import Minio

def cleanup_old_backups():
    """
    Apaga do bucket 'backup' todos os objetos com last_modified anterior a 7 dias atrás.
    """
    endpoint   = Variable.get("MINIO_ENDPOINT")
    access_key = Variable.get("MINIO_ACCESS_KEY")
    secret_key = Variable.get("MINIO_SECRET_KEY")
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    bucket = "backup"
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    for obj in client.list_objects(bucket, recursive=True):
        # obj.last_modified é datetime with tzinfo
        if obj.last_modified < cutoff:
            client.remove_object(bucket, obj.object_name)
            print(f"Deleted backup object {bucket}/{obj.object_name} last_modified={obj.last_modified}")

with DAG(
    dag_id="cleanup_backup",
    schedule_interval="0 1 * * *",  # todo dia às 01:00
    start_date=datetime(2025, 4, 28, tzinfo=timezone.utc),
    catchup=False,
    tags=["backup"],
) as dag:

    t_cleanup = PythonOperator(
        task_id="cleanup_old_backups",
        python_callable=cleanup_old_backups,
    )
