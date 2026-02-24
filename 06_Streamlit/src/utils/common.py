### LIBRARIES ###
import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine, text
from datetime import datetime

###  DATA &  VARIABLES  ###
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
TABLE = os.getenv("TABLE")
ID = 'id'

### APP ###
engine = create_engine(POSTGRES_DATABASE, echo=True)


def load_db():

    with engine.connect() as conn:
        database = pd.read_sql(f'SELECT * FROM {TABLE} ORDER BY id DESC', conn, index_col={ID})
    return database


def write_on_db(estimation):
    """Write prediction to database with managed ID"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COALESCE(MAX({ID}), 0) FROM {TABLE}'))
            estimation[ID] = result.scalar() + 1
            estimation['created_at'] = int(datetime.now().timestamp() * 1000)
            
            estimation.to_sql(name=TABLE, con=engine, index=False, if_exists="append")
    except Exception as e:
        print(f"DB error: {e}")
        raise


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