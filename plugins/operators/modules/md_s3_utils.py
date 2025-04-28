from typing import List
from minio import Minio
import logging

LOG = logging.getLogger(__name__)

def list_objects_for_date(
    client: Minio,
    bucket: str,
    prefix: str
) -> List[str]:
    """
    Retorna os nomes de todos os objetos em `bucket` cujo object_name comece com `prefix`.
    Mesmo que não ache nada, retorna uma lista (vazia).
    """
    LOG.info("Listando objetos em bucket='%s' com prefixo='%s'...", bucket, prefix)
    try:
        objects = client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True)
    except Exception as e:
        LOG.error("Erro ao listar objetos: %s", e)
        return []

    names: List[str] = []
    for obj in objects:
        # obj é do tipo minio.datatypes.Object
        name = getattr(obj, "object_name", None)
        if name:
            names.append(name)
        else:
            LOG.warning("Objeto retornado sem atributo object_name: %s", obj)

    LOG.info("Total de objetos encontrados: %d", len(names))
    return names

def read_object_bytes(
    client: Minio,
    bucket: str,
    key: str
) -> bytes:
    """
    Baixa e retorna os bytes do objeto indicado.
    """
    LOG.info("Lendo objeto %s/%s", bucket, key)
    resp = client.get_object(bucket, key)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return data