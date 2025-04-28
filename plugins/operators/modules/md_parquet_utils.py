import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import List, Dict, Any

def write_dicts_to_parquet(
    data: List[Dict],
    output_filepath: str
) -> None:
    """
    Recebe uma lista de dicionários, converte em DataFrame e escreve em Parquet.
    """
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filepath)
    
def df_from_parquet_bytes(data: bytes) -> pd.DataFrame:
    """
    Converte bytes de um Parquet em pandas.DataFrame.
    """
    table = pq.read_table(BytesIO(data))
    return table.to_pandas()

def write_df_to_parquet(
    df: pd.DataFrame,
    output_filepath: str
) -> None:
    """
    Escreve um pandas.DataFrame em arquivo Parquet local.
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filepath)
    

