import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from operators.connectors.minio_upload_operator import MinIOUploadOperator

# --- constantes do DAG ---
DEFAULT_START = datetime(2025, 4, 27)
BUCKET = "raw-data"
PER_PAGE = 200

@dag(
    dag_id="brewery_metadata_to_minio",
    schedule_interval="15 * * * *",
    start_date=DEFAULT_START,
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["bronze", "minio"],
)
def brewery_meta_to_minio():

    @task
    def fetch_meta() -> dict:
        """Busca metadados da API e retorna o JSON completo."""
        base = Variable.get("OPEN_BREWERY_API_URL")
        resp = requests.get(f"{base}/v1/breweries/meta")
        resp.raise_for_status()
        return resp.json()

    @task
    def extract_total(meta: dict) -> int:
        """
        Extrai o total de registros:
        - first tenta meta['meta']['total']
        - depois meta['total']
        """
        total = meta.get("meta", {}).get("total") or meta.get("total")
        if total is None:
            raise ValueError("Não encontrei 'total' nos metadados")
        return int(total)

    @task
    def upload_meta(meta: dict) -> None:
        """Faz upload do JSON de metadados para metadata/AAAA/MM/DD/breweries_meta.json"""
        ctx = get_current_context()
        ds = ctx["ds_nodash"]  # YYYYMMDD
        path = f"metadata/{ds[0:4]}/{ds[4:6]}/{ds[6:8]}/breweries_meta.json"
        MinIOUploadOperator(
            task_id="upload_meta",
            bucket_name=BUCKET,
            object_name=path,
            data=meta,
            secure=False,
        ).execute(ctx)

    @task
    def paginate_and_upload(total: int) -> None:
        """
        Para cada página de size=200 até total,
        busca /v1/breweries?page=X e faz upload em Breweries/AAAA/MM/DD/breweries_page_X.json
        """
        ctx = get_current_context()
        ds = ctx["ds_nodash"]
        base = Variable.get("OPEN_BREWERY_API_URL")
        pages = (total + PER_PAGE - 1) // PER_PAGE

        for page in range(1, pages + 1):
            resp = requests.get(f"{base}/v1/breweries?page={page}&per_page={PER_PAGE}")
            resp.raise_for_status()
            items = resp.json()

            path = (
                f"Breweries/{ds[0:4]}/{ds[4:6]}/{ds[6:8]}/"
                f"breweries_page_{page}.json"
            )
            MinIOUploadOperator(
                task_id=f"upload_page_{page}",
                bucket_name=BUCKET,
                object_name=path,
                data=items,
                secure=False,
            ).execute(ctx)

    # --- definição do fluxo ---
    meta   = fetch_meta()
    total  = extract_total(meta)
    upload = upload_meta(meta)
    paginate_and_upload(total)

# instância final do DAG
brewery_meta_to_minio = brewery_meta_to_minio()
