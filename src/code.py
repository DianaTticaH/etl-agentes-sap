import os
from dotenv import load_dotenv
from hdbcli import dbapi
import pandas as pd

load_dotenv()

def get_hana_connection():
    return dbapi.connect(
        address=os.getenv("HANA_HOST"),
        port=int(os.getenv("HANA_PORT")),
        user=os.getenv("HANA_USER"),
        password=os.getenv("HANA_PASSWORD")
    )

def extraer_pedidos_hana(fecha_desde: str = "2026-01-01", usersign: int = 51) -> pd.DataFrame:
    query = f'''
    SELECT
        "DocEntry",
        "DocNum",
        "DocDate",
        "UpdateDate",
        "CardCode",
        "CardName",
        "DocTotal",
        "VatSum",
        "DiscSum",
        "Comments",
        SUBSTR_REGEXPR('[A-Za-z]+-[0-9]+' IN "Comments") AS "AgenteCodigo"
    FROM "SBODLA"."ORDR"
    WHERE "DocDate" >= '{fecha_desde}'
      AND "UserSign" = {usersign}
    '''

    conn = None
    try:
        conn = get_hana_connection()
        return pd.read_sql(query, conn)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    df = extraer_pedidos_hana()
    print(df.head())