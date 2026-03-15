import os
from dotenv import load_dotenv
from hdbcli import dbapi
from sqlalchemy import create_engine

load_dotenv()


def get_hana_connection():
    return dbapi.connect(
        address=os.getenv("HANA_HOST"),
        port=int(os.getenv("HANA_PORT")),
        user=os.getenv("HANA_USER"),
        password=os.getenv("HANA_PASSWORD")
    )


def get_postgres_engine():
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    db = os.getenv("PG_DB")

    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")