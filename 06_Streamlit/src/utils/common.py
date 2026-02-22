import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine, text

#####   DATA &  VARIABLES  #####
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
TABLE = os.getenv("TABLE")

#### APP ####
engine = create_engine(POSTGRES_DATABASE, echo=True)


def load_db():

    with engine.connect() as conn:
        database = pd.read_sql(f'SELECT * FROM {TABLE} ORDER BY id DESC', conn, index_col="id")
    return database


def write_on_db(estimation):
    #raise RuntimeError("Simulated DB failure for testing")
    try :
        estimation.to_sql(name=TABLE, con=engine, index=False, if_exists="append")
    except Exception as e:
        print(e)


def update_price(index, price, engine=engine, table=TABLE):
    try:
        query = text(f"""
            UPDATE {table}
            SET price = :price
            WHERE id = :id
        """)
        with engine.begin() as conn:
            conn.execute(query, {"price": price, "id": index})
        print(f"Price updated for id={index} -> {price}")
    except Exception as e:
        print("Error while updating:", e)