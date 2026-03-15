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
        "CardCode": "cardcode",
        "CardName": "cardname",
        "DocTotal": "doctotal",
        "VatSum": "vatsum",
        "DiscSum": "discsum",
        "Comments": "comments",
        "AgenteCodigo": "agentecodigo"
    }).to_dict(orient="records")

    query = text("""
        INSERT INTO stg_sap_orders (
            docentry, docnum, docdate, updatedate, cardcode, cardname,
            doctotal, vatsum, discsum, comments, agentecodigo
        )
        VALUES (
            :docentry, :docnum, :docdate, :updatedate, :cardcode, :cardname,
            :doctotal, :vatsum, :discsum, :comments, :agentecodigo
        )
        ON CONFLICT (docentry)
        DO UPDATE SET
            docnum = EXCLUDED.docnum,
            docdate = EXCLUDED.docdate,
            updatedate = EXCLUDED.updatedate,
            cardcode = EXCLUDED.cardcode,
            cardname = EXCLUDED.cardname,
            doctotal = EXCLUDED.doctotal,
            vatsum = EXCLUDED.vatsum,
            discsum = EXCLUDED.discsum,
            comments = EXCLUDED.comments,
            agentecodigo = EXCLUDED.agentecodigo,
            load_ts = CURRENT_TIMESTAMP
    """)

    with engine.begin() as conn:
        conn.execute(query, records)

    return len(records)