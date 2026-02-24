# GCP CREDENTIAL for HF SPACE
import os
if "GCP_KEY" in os.environ:
    with open("/tmp/gcp_key.json", "w") as f:
        f.write(os.environ["GCP_KEY"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp_key.json"


### LIBRARIES ### 
import pandas as pd
import mlflow
import mlflow.sklearn
from pydantic import BaseModel
from typing import Union
from fastapi import FastAPI, HTTPException


### APP ### 
description = """
Housing Prices API provides real-estate predictions.

**Endpoints:**
* /predict — Estimate housing price.
See /docs for request format and examples.
"""

app = FastAPI(
    title="Housing Prices Estimator",
    description=description,
    version="1.0.0"
)


mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])


class PredictionFeatures(BaseModel):
    square_feet: Union[int, float]
    num_bedrooms: Union[int, float]
    num_bathrooms: Union[int, float]
    num_floors: Union[int, float]
    year_built: Union[int, float]
    has_garden: Union[int, float]
    has_pool: Union[int, float]
    garage_size: Union[int, float]
    location_score: Union[int, float]
    distance_to_center: Union[int, float]


@app.post("/predict", tags=["Machine Learning"], response_model=dict)
async def predict(features: PredictionFeatures):
    try:
        df = pd.DataFrame([features.model_dump()])
        MODEL_NAME = os.environ["MODEL_NAME"]
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/latest")
        prediction = model.predict(df)

        return {"prediction": float(prediction[0])}

    except Exception as e:
        raise HTTPException(status_code=500,detail=f"Prediction failed: {str(e)}")