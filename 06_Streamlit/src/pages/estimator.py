#####   LIBRARY  #####
from datetime import datetime
import streamlit as st
import requests
import json
import os
from utils.common import write_on_db
import pandas as pd

#####   DATA &  VARIABLES  #####
FASTAPI_URL = os.getenv("FASTAPI_URL")

#####   APP  #####
st.title("Estimator")

# --- Session state initialization ---
if "processing" not in st.session_state:
    st.session_state.processing = False
if "prediction" not in st.session_state:
    st.session_state.prediction = None
if "db_status" not in st.session_state:
    st.session_state.db_status = None  

# --- Form ---
with st.form(key="prediction_form"):
    square_feet = st.slider("Square feet", 50.0, 300.0, 175.0)
    year_built = st.slider("Year of built", 1900, 2022, 1950)

    container = st.container()
    col1, col2, col3 = container.columns(3)
    with col1:
        num_bedrooms = st.number_input("Bedrooms", value=1, min_value=1, max_value=5)
    with col2:
        num_bathrooms = st.number_input("Bathrooms", value=1, min_value=1, max_value=3)
    with col3:
        num_floors = st.number_input("Floors", value=1, min_value=1, max_value=3)

    garage_size = st.slider("Garage size", 10, 49, 30)
    distance_to_center = st.slider("Distance to center", 0.06, 20.00, 10.00)
    location_score = st.slider("Location score", 0.005, 9.995, 5.000)

    container = st.container()
    col1, col2, col3 = container.columns(3)
    with col1:
        has_garden = 1 if st.toggle("Garden", key="has_garden_toggle") else 0
    with col2:
        has_pool = 1 if st.toggle("Pool", key="has_pool_toggle") else 0

    with col3:
        submit_button = st.form_submit_button(
            "Get Estimation",
            disabled=st.session_state.processing
        )

# --- Trigger processing ---
if submit_button and not st.session_state.processing:
    st.session_state.processing = True
    st.rerun()

# --- Processing ---
if st.session_state.processing and st.session_state.prediction is None:
    with st.spinner("Processing ..."):
        try:
            # 1. Features for prediction
            features = {
                "square_feet": square_feet,
                "num_bedrooms": num_bedrooms,
                "num_bathrooms": num_bathrooms,
                "num_floors": num_floors,
                "year_built": year_built,
                "has_garden": has_garden,
                "has_pool": has_pool,
                "garage_size": garage_size,
                "location_score": location_score,
                "distance_to_center": distance_to_center
            }

            # 2. Call FastAPI for prediction
            resp = requests.post(FASTAPI_URL, json=features)
            resp.raise_for_status()
            prediction = resp.json().get("prediction", None)
            if prediction is None:
                raise ValueError("Model did not return an estimation")

            st.session_state.prediction = prediction

            # 3. Write features + prediction to DB
            payload = {**features, "price_predict": prediction}
            try:
                write_on_db(pd.DataFrame.from_dict([payload]))
                st.session_state.db_status = "Archived "
            except Exception as e:
                st.session_state.db_status = f"Archive failed: {e}"

        except Exception as e:
            st.error(f"Error: {e}")
            st.session_state.prediction = "Error"
            st.session_state.db_status = "Could not save data"

        st.session_state.processing = False
        st.rerun()

# --- RESULTS ---
if st.session_state.prediction is not None:
    # Affichage de la prédiction
    if isinstance(st.session_state.prediction, (int, float)):
        st.success(f"Estimation : {round(st.session_state.prediction)} $")
    else:
        st.error(f"Estimation failed: {st.session_state.prediction}")

    # Affichage du status DB
    if st.session_state.db_status:
        st.info(st.session_state.db_status)

    # Reset session_state pour le prochain calcul
    st.session_state.prediction = None
    st.session_state.db_status = None
