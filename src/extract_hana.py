import pandas as pd
from config import get_hana_connection


def extract_orders_from_hana(last_source_key=None) -> pd.DataFrame:
    conn = None

    try:
        conn = get_hana_connection()

        if last_source_key:
            query = f"""
            SELECT
                "DocEntry",
                "DocNum",
                "DocDate",
                "UpdateDate",
                "UpdateTS",
                "CardCode",
                "CardName",
                "DocTotal",
                "VatSum",
                "DiscSum",
                "Comments",
                SUBSTR_REGEXPR('[A-Za-z]+-[0-9]+' IN "Comments") AS "AgenteCodigo",
                TO_NVARCHAR("UpdateDate", 'YYYYMMDD') || LPAD(TO_NVARCHAR("UpdateTS"), 6, '0') AS "UpdateKey"
            FROM "SBODLA"."ORDR"
            WHERE "UserSign" = 51
              AND (
                    TO_NVARCHAR("UpdateDate", 'YYYYMMDD') || LPAD(TO_NVARCHAR("UpdateTS"), 6, '0')
                    > '{last_source_key}'
                  )
            """
        else:
            query = """
            SELECT
                "DocEntry",
                "DocNum",
                "DocDate",
                "UpdateDate",
                "UpdateTS",
                "CardCode",
                "CardName",
                "DocTotal",
                "VatSum",
                "DiscSum",
                "Comments",
                SUBSTR_REGEXPR('[A-Za-z]+-[0-9]+' IN "Comments") AS "AgenteCodigo",
                TO_NVARCHAR("UpdateDate", 'YYYYMMDD') || LPAD(TO_NVARCHAR("UpdateTS"), 6, '0') AS "UpdateKey"
            FROM "SBODLA"."ORDR"
            WHERE "DocDate" >= '2026-01-01'
              AND "UserSign" = 51
            """

        df = pd.read_sql(query, conn)
        return df

    finally:
        if conn:
            conn.close()