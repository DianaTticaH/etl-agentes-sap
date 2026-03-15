from sqlalchemy import text
from config import get_postgres_engine


PROCESS_NAME = "etl_sap_orders"


def get_last_load_ts():
    engine = get_postgres_engine()

    query = text("""
        SELECT last_load_ts
        FROM etl_control_agentes_sap
        WHERE process_name = :process_name
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"process_name": PROCESS_NAME}).fetchone()

    if result and result[0]:
        return result[0]

    return None


def update_control(status: str, message: str, last_load_ts=None):
    engine = get_postgres_engine()

    query = text("""
        INSERT INTO etl_control_agentes_sap (process_name, last_load_ts, last_status, last_message, updated_at)
        VALUES (:process_name, :last_load_ts, :last_status, :last_message, CURRENT_TIMESTAMP)
        ON CONFLICT (process_name)
        DO UPDATE SET
            last_load_ts = EXCLUDED.last_load_ts,
            last_status = EXCLUDED.last_status,
            last_message = EXCLUDED.last_message,
            updated_at = CURRENT_TIMESTAMP
    """)

    with engine.begin() as conn:
        conn.execute(query, {
            "process_name": PROCESS_NAME,
            "last_load_ts": last_load_ts,
            "last_status": status,
            "last_message": message
        })