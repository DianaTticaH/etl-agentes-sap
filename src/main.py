from datetime import datetime
from extract_hana import extract_orders_from_hana
from load_postgres import upsert_orders
from control import get_last_load_ts, update_control


def run_etl():
    try:
        print("Iniciando ETL...")

        last_load_ts = get_last_load_ts()
        print(f"Última fecha de carga: {last_load_ts}")

        df = extract_orders_from_hana(last_load_ts=last_load_ts)
        print(f"Filas extraídas desde HANA: {len(df)}")

        inserted = upsert_orders(df)
        print(f"Filas insertadas/actualizadas en PostgreSQL: {inserted}")

        new_load_ts = datetime.now()
        update_control(
            status="OK",
            message=f"ETL ejecutado correctamente. Registros procesados: {inserted}",
            last_load_ts=new_load_ts
        )

        print("ETL finalizado correctamente.")

    except Exception as e:
        update_control(
            status="ERROR",
            message=str(e),
            last_load_ts=None
        )
        print(f"Error en ETL: {e}")
        raise


if __name__ == "__main__":
    run_etl()