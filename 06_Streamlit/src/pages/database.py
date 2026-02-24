#####   LIBRARY  #####
import streamlit as st
import pandas as pd
import time
from utils.common import load_db, update_price

# Load data
data = load_db()

# fix waiting db
if data.empty:
    st.info("Connecting to the database…")
    st.stop()

#####   APP  #####
st.title("Database")

tab1, tab2 = st.tabs([" 📈 Estimation", " 🏷️ Sold"])
with tab1:

    df = data.loc[data['price_predict'].notnull()]
    st.write("This section displays properties with their **predicted market values**, based on our latest analysis.")

    st.dataframe(
        df,
        use_container_width = True,
        column_config={
            "created_at": st.column_config.DatetimeColumn(
                "created_at",
                format="DD-MM-YYYY H:M:S"
            )
        }
    )

with tab2:

    df = data.loc[data['price'].notnull()]
    st.write("This section lists properties that have **already been sold**, along with their final sale prices.")

    st.dataframe(
        df,
        use_container_width = True,
        column_config={
            "created_at": st.column_config.DatetimeColumn(
                "created_at",
                format="DD-MM-YYYY H:M:S"
            )
        }
    )

st.subheader("Update price", divider=True)

house_ids = data.index.tolist()

selected_id = st.selectbox(
    "Select house id",
    options=house_ids,
    index=None,
    placeholder="Select an id ..."
)

if selected_id is not None:
    price_str = st.text_input("New price", placeholder="Type a number...")

    if st.button("Update price"):
        try:
            price = float(price_str)
            if price > 0:
                try:
                    update_price(selected_id, price)
                    st.success(f"Price updated for {selected_id} -> {price}")
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    st.error(f" Could not update price: {e}")
            else:
                st.warning(" Please enter a number greater than 0.")
        except ValueError:
            st.warning(" Please enter a valid number before updating.")