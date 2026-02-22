import pandas as pd
import mlflow.sklearn
import mlflow
from pydantic import BaseModel
from typing import Union
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Housing Price Estimator")

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


@app.post("/predict")
async def predict(predictionFeatures: PredictionFeatures):
    try:
        df = pd.DataFrame([predictionFeatures.dict()])
        mlflow.set_tracking_uri('https://lyx51-mlflow.hf.space')
        model_name = "Housing_prices_estimator_LR"
        model_version = "latest"
        model = mlflow.sklearn.load_model(f"models:/{model_name}/{model_version}")
        prediction = model.predict(df)
        return {"prediction": prediction.tolist()[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch-predict")
async def batch_predict(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
        mlflow.set_tracking_uri('https://lyx51-mlflow.hf.space')
        model_name = "Housing_prices_estimator_LR"
        model_version = "latest"
        model = mlflow.sklearn.load_model(f"models:/{model_name}/{model_version}")
        predictions = model.predict(df)
        return {"predictions": predictions.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
