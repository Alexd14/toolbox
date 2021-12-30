import pandas as pd

from ..api.sql_connection import SQLConnection


def table_info(table_name: str) -> pd.DataFrame:
    """
    runs the table info PRAGMA query
    """
    con = SQLConnection()
    info_df = con.execute(f"PRAGMA table_info('{table_name}');").fetchdf()
    con.close()
    return info_df
