import pandas as pd
from config import get_hana_connection


def extract_orders_from_hana(last_load_ts=None) -> pd.DataFrame:
    conn = None

    try:
        conn = get_hana_connection()

        if last_load_ts:
            query = f"""
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
            WHERE "UserSign" = 51
              AND "UpdateDate" >= '{last_load_ts.strftime("%Y-%m-%d %H:%M:%S")}'
            """
        else:
            query = """
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
            WHERE "DocDate" >= '2026-01-01'
              AND "UserSign" = 51
            """

        df = pd.read_sql(query, conn)
        return df

    finally:
        if conn:
            conn.close()