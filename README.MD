# Est'immo : Real Estate Price Estimator

## Context 📇

Est’Immo is a platform designed for **real estate agencies** to help them estimate property prices.
In a constantly evolving market, setting the right price is critical:

* Too high : the property remains unsold for months
* Too low : the seller loses value

By leveraging **AI and historical transaction data**, Est’Immo provides agents with a reliable tool to:

* Instantly estimate property prices
* Strengthen their recommendations to clients
* Increase credibility and efficiency during sales negotiations

## Project goals 🎯

Our business objectives are to:

* Provide agents with accurate and instant property price estimations
* Continuously update the model with new market transactions
* Ensure prediction reliability through monitoring and automated testing
* Deliver an intuitive, easy-to-use platform tailored for professionals

## Where to get data 📊
To develop and maintain this system, we rely on multiple data sources:
- **[This dataset](https://www.kaggle.com/datasets/denkuznetz/housing-prices-regression/data)**
  * Includes location, size, features, and final sale prices of properties  
  * Used for training and validating the machine learning model  
- **Real-time property listings feed**  
  * Keeps the model updated with the latest market trends  
  * Enables continuous ingestion and retraining  

## Added value 💡

With Est’Immo, **real estate agents** can:

* Provide fast and credible valuations to their clients
* Reduce the average time on market by suggesting optimal prices
* Enhance their expertise with a modern, data-driven tool
* Stand out from competitors by offering a more professional service


## The app
You can directly access to:
* [Streamlit](https://lyx51-housing-price-estimator-streamlit.hf.space) : App and architecture
* [Mlflow training](https://lyx51-mlflow.hf.space/#/experiments/2/runs/801f74159f8a4c6d98a88f8c8310977c)
* [FastAPI](https://lyx51-Housing-Price-Estimator-FastAPI.hf.space/docs#/)
* [Video of the app](https://share.vidyard.com/watch/jfmZxQUhsKxLb5x3fkfE2H)

## Architecture

    └──M04-Projet-Final-Estimmo/
        ├── 01_architecture/
        |   └── 01_architecture.ipynb   
        ├── 02_notebook/
        |   ├── 01_eda_model_selection.ipynb
        |   ├── 02_create_postgres_database.ipynb
        |   └── 03_train_model.ipynb    
        ├── 03_airflow/
        |   ├── dags/
        |   |    ├── ETL_from_API.py
        |   |    └── ml_workflow.py
        |   ├── logs/
        |   ├── secrets/
        |   |    └── ec2_key.pem        
        |   ├── .env
        |   ├── docker-compose.yaml
        |   ├── Dockerfile
        |   └── requirements.txt
        ├── 04_CICD/: 
        |   ├── app/     
        |   |    └── train.py
        |   ├── tests/     
        |   |    └── ml_pipeline_test.py
        |   ├── Dockerfile
        |   ├── Jenkinsfile
        |   ├── MLproject
        |   └── requirements.txt       
        ├── 05_Fastapi/
        |   ├── app.py
        |   ├── Dockerfile
        |   ├── README.md
        |   └── requirements.txt
        ├── 06_jenkins_server/
        |   └── Dockerfile
        ├── 07_mlflow_server/
        |   ├── Dockerfile
        |   ├── README.md
        |   └── requirements.txt
        ├── 08_streamlit/
        |   ├── src/
        |   ├── Dockerfile
        |   ├── README.md
        |   └── requirements.txt
        └── README.md
