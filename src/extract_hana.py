import pandas as pd
from config import get_hana_connection


def extract_orders_from_hana(last_source_key=None) -> pd.DataFrame:
    conn = None

    try:
        conn = get_hana_connection()

        if last_source_key:
            query = f"""
            SELECT
                X."DocEntry",
                X."DocNum",
                X."DocDate",
                X."UpdateDate",
                X."UpdateTS",
                X."CardCode",
                X."CardName",
                X."DocTotal",
                X."VatSum",
                X."DiscSum",
                X."TotalExpns",
                X."Comments",
                X."AgenteCodigo",
                SUBSTR_REGEXPR('[A-Za-z]+' IN X."AgenteCodigo") AS "Nomipad",
                SUBSTR_REGEXPR('[0-9]+' IN X."AgenteCodigo") AS "CodPedido",
                X."UpdateKey"
            FROM (
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
                    "TotalExpns",
                    "Comments",
                    SUBSTR_REGEXPR('[A-Za-z]+-[0-9]+' IN "Comments") AS "AgenteCodigo",
                    TO_NVARCHAR("UpdateDate", 'YYYYMMDD') || LPAD(TO_NVARCHAR("UpdateTS"), 6, '0') AS "UpdateKey"
                FROM "SBODLA"."ORDR"
                WHERE "UserSign" = 51
            ) X
            WHERE X."UpdateKey" > '{last_source_key}'
            """
        else:
            query = """
            SELECT
                X."DocEntry",
                X."DocNum",
                X."DocDate",
                X."UpdateDate",
                X."UpdateTS",
                X."CardCode",
                X."CardName",
                X."DocTotal",
                X."VatSum",
                X."DiscSum",
                X."TotalExpns",
                X."Comments",
                X."AgenteCodigo",
                SUBSTR_REGEXPR('[A-Za-z]+' IN X."AgenteCodigo") AS "Nomipad",
                SUBSTR_REGEXPR('[0-9]+' IN X."AgenteCodigo") AS "CodPedido",
                X."UpdateKey"
            FROM (
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
                    "TotalExpns",
                    "Comments",
                    SUBSTR_REGEXPR('[A-Za-z]+-[0-9]+' IN "Comments") AS "AgenteCodigo",
                    TO_NVARCHAR("UpdateDate", 'YYYYMMDD') || LPAD(TO_NVARCHAR("UpdateTS"), 6, '0') AS "UpdateKey"
                FROM "SBODLA"."ORDR"
                WHERE "DocDate" >= '2026-01-01'
                  AND "UserSign" = 51
            ) X
            """

        df = pd.read_sql(query, conn)
        return df

    finally:
        if conn:
            conn.close()

