# LIBRARY
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sqlalchemy import create_engine, text
from evidently.ui.workspace import CloudWorkspace
from evidently import Report
from evidently.metrics import *
from evidently.presets import *
from evidently.tests import *
from evidently import Dataset
from evidently import DataDefinition
import pandas as pd
from datetime import datetime

#Evidently
EVIDENTLY_TOKEN = Variable.get("EVIDENTLY_TOKEN")
EVIDENTLY_PROJECT = Variable.get("EVIDENTLY_PROJECT")
EVIDENTLY_REFERENCE_DATASET = Variable.get("EVIDENTLY_REFERENCE_DATASET")

# PostgreSQL
TABLE = Variable.get("HOUSING_PRICES_TABLE")

# DAG
def extract_db():
    conn = BaseHook.get_connection("postgres_default")
    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    with engine.connect() as conn:
        stmt = text(f"""SELECT * FROM {TABLE}""")
        result = conn.execute(stmt)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        data.to_csv("/tmp/to_predict.csv", index=False)

    #Load
    temp_file = "/tmp/to_predict.csv" 
    data.to_csv(temp_file, index=False)

    return temp_file

def drift_validation(**kwargs):
    temp_file = kwargs["ti"].xcom_pull(task_ids="extract")

    ws = CloudWorkspace(
    token=EVIDENTLY_TOKEN,
    url="https://app.evidently.cloud")

    project = ws.get_project(EVIDENTLY_PROJECT)

    reference = ws.load_dataset(dataset_id = EVIDENTLY_REFERENCE_DATASET)

    current = pd.read_csv(temp_file) 

    current.drop(['price','price_predict'], axis=1, inplace=True)

    data_drift_summary = Report(
    metrics=[
        DataDriftPreset(),
        DataSummaryPreset(),
    ],
    include_tests=True
)
    data_report = data_drift_summary.run(reference_data=reference, current_data=current)

    ws.add_run(project.id, data_report, include_data=False)
    
    data_drift_dict = data_report.dict()

    if data_drift_dict["metrics"][0]['value']['count'] != 0:
        print(f"Drift detected ! ")
        drift_detected = True

    return drift_detected  

# --- DAG ---
with DAG(
    dag_id="monitoring",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    # Task 1 : Extraction
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_db,
    )

    # Task 2 : Validation drift 
    drift_test = ShortCircuitOperator(
        task_id="drift_validation",
        python_callable=drift_validation,
        provide_context=True,
    )

    # Task 3 : train_model if drift
    trigger_training = TriggerDagRunOperator(
        task_id="trigger_train_model",
        trigger_dag_id="dag_training",
    )

    # Dépendances
    extract >> drift_test >> trigger_training