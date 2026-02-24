import streamlit as st
st.set_page_config(layout="wide")

pages = [
    st.Page("pages/home.py", title="Est'Immo", icon="📋"),
    st.Page("pages/architecture.py", title="Architecture", icon="🏗️"),
    st.Page("pages/estimator.py", title="Estimator", icon="📈"),
    st.Page("pages/database.py", title="Database", icon="🛢️"),
]

pg = st.navigation(pages)
pg.run()