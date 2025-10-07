import pandas as pd
import numpy as np
import mlflow
import time
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression 
from sklearn.pipeline import Pipeline
import os
import boto3
from io import BytesIO
from botocore.exceptions import ClientError
import argparse


def load_data(file_key: str):
    bucket = 'fp-private-bucket'

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'eu-west-3')
    )

    try:
        response = s3.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(BytesIO(response['Body'].read()))
        return df
    except ClientError as e:
        print(f" ClientError : {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        raise
    except Exception as e:
        print(f" Exception : {e}")
        raise


def preprocess_data(df):
    df.columns = df.columns.str.lower()
    target_variable = "price"
    my_features_list = ['square_feet', 'num_bedrooms', 'num_bathrooms', 'num_floors',
                        'year_built', 'has_garden', 'has_pool', 'garage_size',
                        'location_score', 'distance_to_center']

    X = df[my_features_list]
    y = df[target_variable]

    return train_test_split(X, y, test_size=0.2, random_state=42)


def create_pipeline():
    my_features_list = ['square_feet', 'num_bedrooms', 'num_bathrooms', 'num_floors',
                        'year_built', 'has_garden', 'has_pool', 'garage_size',
                        'location_score', 'distance_to_center']
    categorical_features = []
    numeric_features = [feature for feature in my_features_list if feature not in categorical_features]

    numerical_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numeric_features)
        ])

    return Pipeline(steps=[
        ("Preprocessing", preprocessor),
        ("Regressor", LinearRegression())
    ], verbose=True)


def train_model(pipe, X_train, y_train):
    pipe.fit(X_train, y_train)
    return pipe


def log_metrics_and_model(model, X_train, y_train, X_test, y_test, artifact_path, registered_model_name):
    mlflow.log_metric("Train Score", model.score(X_train, y_train))
    mlflow.log_metric("Test Score", model.score(X_test, y_test))
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path=artifact_path,
        #registered_model_name=registered_model_name  #dont works with mlflow 3.1
    )


def run_experiment(experiment_name, file_key, artifact_path, registered_model_name):
    start_time = time.time()
    df = load_data(file_key)
    X_train, X_test, y_train, y_test = preprocess_data(df)
    pipe = create_pipeline()
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    mlflow.sklearn.autolog()

    with mlflow.start_run(experiment_id=experiment.experiment_id):
        model = train_model(pipe, X_train, y_train)
        log_metrics_and_model(model, X_train, y_train, X_test, y_test, artifact_path, registered_model_name)

    print(f"...Training Done! --- Total training time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_key", type=str, required=True, help="S3 key for the training dataset")
    args = parser.parse_args()
    file_key = args.file_key
    experiment_name = "test"
    artifact_path = "modeling_housing_market"
    registered_model_name = "linear_regression"

    run_experiment(experiment_name, file_key, artifact_path, registered_model_name)
