# LIBRARY
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from pandera import Column, DataFrameSchema, Check
from sqlalchemy import create_engine, text
import requests
import paramiko
import boto3
import time
import json
import pandas as pd
from datetime import datetime
from io import BytesIO
from botocore.exceptions import ClientError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# VARIABLE
# Jenkins 
JENKINS_USERNAME = Variable.get("JENKINS_USERNAME")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_HOUSING_TRAIN")

# Get AWS connection details from Airflow
KEY_PAIR_NAME=Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID=Variable.get("AMI_ID")
SECURITY_GROUP_ID = json.loads(Variable.get("SECURITY_GROUP_ID"))
INSTANCE_TYPE=Variable.get("INSTANCE_TYPE")
aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
AWS_ACCESS_KEY_ID = aws_conn.login
AWS_SECRET_ACCESS_KEY = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')
BUCKET_NAME = Variable.get("S3BucketName")
BUCKET_FOLDER = Variable.get("HOUSING_TRAIN_DATASET_BUCKET_FOLDER")

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI=Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID=Variable.get("MLFLOW_EXPERIMENT_ID")

# PostgreSQL
TABLE = Variable.get("HOUSING_PRICES_TABLE")

# mail 
APP_MAIL = Variable.get("APP_MAIL")
APP_MAIL_PASSWORD = Variable.get("APP_MAIL_PASSWORD")

# DAG
@dag(
    dag_id="dag_training",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False)

def train_model():

    @task()
    def ETL_from_postgres():
        # EXTRACT FROM POSTGRES
        conn = BaseHook.get_connection("postgres_default")
        engine = create_engine(
            f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )

        #  search for data to predict
        with engine.connect() as conn:
            #### Data with price validated
            stmt = text(f"""
                SELECT * FROM {TABLE} 
                WHERE price IS NOT NULL
                """)
            result = conn.execute(stmt)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())

        # LOAD TO S3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"train_dataset_{timestamp}.csv"
        key = BUCKET_FOLDER + filename
        data.to_csv(filename, index=False)
        s3 = S3Hook(aws_conn_id="aws_default")


        try:
            s3.load_file(
                filename = filename,
                key= key,
                bucket_name=BUCKET_NAME,
                replace=True
            )
        except Exception as e:
            raise Exception(f"Failed to load dataset to S3: {e}")
        
        return key
    

    # Step 1: Poll Jenkins Job Status
    @task
    def poll_jenkins_job(key : str):
        """Poll Jenkins for the job status and check for successful build."""

        # Step 1: Get the latest build number from the job API
        job_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/api/json"
        response = requests.get(job_url, auth=(JENKINS_USERNAME, JENKINS_TOKEN))
        if response.status_code != 200:
            raise Exception(f"Failed to query Jenkins API: {response.status_code}")

        job_info = response.json()
        latest_build_number = job_info['lastBuild']['number']

        # Step 2: Poll the latest build's status
        build_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/{latest_build_number}/api/json"

        while True:
            response = requests.get(build_url, auth=(JENKINS_USERNAME, JENKINS_TOKEN))
            if response.status_code == 200:
                build_info = response.json()
                if not build_info['building']:  # Build is finished
                    if build_info['result'] == 'SUCCESS':
                        print("Jenkins build successful!")
                        return True
                    else:
                        raise Exception("Jenkins build failed!")
            else:
                raise Exception(f"Failed to query Jenkins API: {response.status_code}")
            
            time.sleep(30)  # Poll every 30 seconds

    @task
    def create_instance():
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2', region_name=region_name)
        ec2 = hook.get_client_type()

        response = ec2.run_instances(
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            KeyName=KEY_PAIR_NAME,
            SecurityGroupIds=SECURITY_GROUP_ID,
            MinCount=1,
            MaxCount=1
        )

        instance_id = response['Instances'][0]['InstanceId']
        print(f"Instance created: {instance_id}")

        # Wait instance running status
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        print(f"Instance {instance_id} is now running.")

        return instance_id

    @task
    def check_ec2_status(instance_id: str):
        """Checking for checks status with 2/2 OK."""

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2', region_name=region_name)
        ec2 = hook.get_client_type()

        passed_checks = False

        while not passed_checks:
            response = ec2.describe_instance_status(InstanceIds=[instance_id])

            if response['InstanceStatuses']:
                status = response['InstanceStatuses'][0]
                system_status = status['SystemStatus']['Status']
                instance_status = status['InstanceStatus']['Status']

                print(f"System: {system_status}, Instance: {instance_status}")

                if system_status == 'ok' and instance_status == 'ok':
                    print(f"Instance {instance_id} passed both checks.")
                    passed_checks = True
                else:
                    print(f"Waiting for instance {instance_id} to pass checks...")
            else:
                print(f"No status info yet for instance {instance_id}. Retrying...")

            time.sleep(15)

        return instance_id  

    @task
    def get_ec2_public_ip(instance_id: str) -> dict:

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2')
        session = hook.get_session()
        
        ec2 = session.resource('ec2', region_name=region_name)
        instance = ec2.Instance(instance_id)

        # Waiting Public IP available
        instance.wait_until_running()
        instance.reload()

        public_ip = instance.public_ip_address
        print(f"Public IP of EC2 Instance: {public_ip}")

        return {
        "instance_id": instance_id,
        "public_ip": public_ip
    }

    @task
    def run_training_via_paramiko(instance_data: dict, file_key: str) -> dict:
        public_ip = instance_data["public_ip"]
        instance_id = instance_data["instance_id"]

        print("PUBLIC IP:", public_ip)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        private_key = paramiko.RSAKey.from_private_key_file(KEY_PATH)

        try:
            ssh_client.connect(hostname=public_ip, username='ubuntu', pkey=private_key)

            command = f'''
            export PATH=$PATH:/home/ubuntu/.local/bin && \
            export MLFLOW_TRACKING_URI="{MLFLOW_TRACKING_URI}" && \
            export MLFLOW_EXPERIMENT_ID="{MLFLOW_EXPERIMENT_ID}" && \
            export AWS_ACCESS_KEY_ID="{AWS_ACCESS_KEY_ID}" && \
            export AWS_SECRET_ACCESS_KEY="{AWS_SECRET_ACCESS_KEY}" && \
            mlflow run https://github.com/LyXoR51/CICD_estimmo.git --build-image -P file_key="{file_key}"
            '''

            stdin, stdout, stderr = ssh_client.exec_command(command)

            out = stdout.read().decode()
            err = stderr.read().decode()

            print("--- STDOUT ---\n", out)
            print("--- STDERR ---\n", err)

            return {
                'stdout': out,
                'stderr': err,
                'instance_id': instance_id
            }

        except Exception as e:
            print(f"Error occurred during SSH: {str(e)}")
            raise
        finally:
            # Close the SSH connection
            ssh_client.close()


    @task
    def terminate_instance(instance_data: dict):
        instance_id = instance_data["instance_id"]

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2', region_name=region_name)
        ec2 = hook.get_client_type()

        print(f"Terminating instance {instance_id}...")
        ec2.terminate_instances(InstanceIds=[instance_id])

        waiter = ec2.get_waiter('instance_terminated')
        waiter.wait(InstanceIds=[instance_id])
        print(f"Instance {instance_id} terminated.")


    @task()
    def send_alert():
        sender_email = APP_MAIL
        app_password = APP_MAIL_PASSWORD
        receiver_email = APP_MAIL

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = "ESTIMMO Alert - Drift detected - New model to validate"

        html = f"""\
        <html>
        <body>
            <p><b>Data drift detected.</b><br>
            View dashboard : <a href="https://app.evidently.cloud/v2/projects/01986722-f00b-7e7f-b296-5bea814ff798/dashboard">Open dashboard</a><br>
            New model to validate : 
            <a href="https://lyx51-mlflow.hf.space/#/experiments/2">
                View model
            </a><br>
            </p>
        </body>
        </html>
        """
        msg.attach(MIMEText(html, "html"))

        try:
            # Connect to Gmail SMTP
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()  # secure the connexion
            server.login(sender_email, app_password)  # Authentification

            # sent mail
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)

            print("E-mail sent!")
        except Exception as e:
            print(f"Fail: {e}")
        finally:
            server.quit()  # close connexion


    data = ETL_from_postgres()
    jenkins = poll_jenkins_job(data)
    create = create_instance()
    check_status = check_ec2_status(create)
    get_ip = get_ec2_public_ip(check_status)
    train = run_training_via_paramiko(get_ip, data)
    terminate = terminate_instance(train)
    alert  = send_alert()

    data >> jenkins >> create >> check_status >> get_ip >> train >> terminate >> alert 

ML_workflow_instance = train_model()