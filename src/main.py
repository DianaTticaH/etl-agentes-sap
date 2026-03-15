from datetime import datetime
from extract_hana import extract_orders_from_hana
from load_postgres import upsert_orders
from control import get_last_source_key, update_control


def run_etl():
    try:
        print("Iniciando ETL...")

        last_source_key = get_last_source_key()
        print(f"Última source_key: {last_source_key}")

        df = extract_orders_from_hana(last_source_key=last_source_key)
        print(f"Filas extraídas desde HANA: {len(df)}")

        inserted = upsert_orders(df)
        print(f"Filas insertadas/actualizadas en PostgreSQL: {inserted}")

        if not df.empty:
            new_last_source_key = df["UpdateKey"].max()
        else:
            new_last_source_key = last_source_key

        update_control(
            status="OK",
            message=f"ETL ejecutado correctamente. Registros procesados: {inserted}",
            last_load_ts=datetime.now(),
            last_source_key=new_last_source_key
        )

        print(f"Nueva source_key guardada: {new_last_source_key}")
        print("ETL finalizado correctamente.")

    except Exception as e:
        update_control(
            status="ERROR",
            message=str(e),
            last_load_ts=None,
            last_source_key=None
        )
        print(f"Error en ETL: {e}")
        raise


if __name__ == "__main__":
    run_etl()