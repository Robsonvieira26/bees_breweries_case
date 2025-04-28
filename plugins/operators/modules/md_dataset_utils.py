from airflow.datasets import Dataset

def create_dataset_for_path(path: str) -> Dataset:
    uri = f"s3://{path}"
    return Dataset(uri)
