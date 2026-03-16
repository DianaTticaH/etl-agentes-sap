from sqlalchemy import text
from config import get_postgres_engine


def upsert_orders(df):
    if df.empty:
        return 0

    engine = get_postgres_engine()

    records = df.rename(columns={
        "DocEntry": "docentry",
        "DocNum": "docnum",
        "DocDate": "docdate",
        "UpdateDate": "updatedate",
        "UpdateTS": "updatets",
        "UpdateKey": "updatekey",
        "CardCode": "cardcode",
        "CardName": "cardname",
        "DocTotal": "doctotal",
        "VatSum": "vatsum",
        "DiscSum": "discsum",
        "Comments": "comments",
        "AgenteCodigo": "agentecodigo",
        "Nomipad": "nomipad",
        "CodPedido": "codpedido"
    }).to_dict(orient="records")

    query = text("""
        INSERT INTO stg_sap_orders (
            docentry, docnum, docdate, updatedate, updatets, updatekey,
            cardcode, cardname, doctotal, vatsum, discsum, comments,
            agentecodigo, nomipad, codpedido
        )
        VALUES (
            :docentry, :docnum, :docdate, :updatedate, :updatets, :updatekey,
            :cardcode, :cardname, :doctotal, :vatsum, :discsum, :comments,
            :agentecodigo, :nomipad, :codpedido
        )
        ON CONFLICT (docentry)
        DO UPDATE SET
            docnum = EXCLUDED.docnum,
            docdate = EXCLUDED.docdate,
            updatedate = EXCLUDED.updatedate,
            updatets = EXCLUDED.updatets,
            updatekey = EXCLUDED.updatekey,
            cardcode = EXCLUDED.cardcode,
            cardname = EXCLUDED.cardname,
            doctotal = EXCLUDED.doctotal,
            vatsum = EXCLUDED.vatsum,
            discsum = EXCLUDED.discsum,
            comments = EXCLUDED.comments,
            agentecodigo = EXCLUDED.agentecodigo,
            nomipad = EXCLUDED.nomipad,
            codpedido = EXCLUDED.codpedido,
            load_ts = CURRENT_TIMESTAMP
    """)

    with engine.begin() as conn:
        conn.execute(query, records)

    return len(records)