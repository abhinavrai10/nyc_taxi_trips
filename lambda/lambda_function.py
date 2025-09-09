import requests
import boto3
import os
from botocore.exceptions import ClientError

# Airflow API endpoint (replace <your-ec2-ip-or-dns> with actual EC2 IP or DNS)
AIRFLOW_API_URL = "http://44.204.10.237:8080//api/v1/dags/bronze_to_gold_pipeline/dagRuns"

def trigger_airflow_dag(year, month, file_key, username, password):
    payload = {
        "conf": {
            "year": year,
            "month": month,
            "file_key": file_key
        }
    }
    response = requests.post(
    AIRFLOW_API_URL,
    auth=(username, password),
    json=payload,
    headers={"Content-Type": "application/json"}
    )
    print(f"API Response Status: {response.status_code}")
    print(f"API Response Body: {response.text}")
    if response.status_code not in (200, 201, 202):
        raise Exception(f"Failed to trigger Airflow DAG: {response.text}")

def lambda_handler(event, context):
    # Retrieve credentials from environment variables
    airflow_username = os.environ.get('AIRFLOW_USERNAME')
    airflow_password = os.environ.get('AIRFLOW_PASSWORD')
    
    # Validate environment variables
    if not airflow_username or not airflow_password:
        print("Missing AIRFLOW_USERNAME or AIRFLOW_PASSWORD environment variables")
        return {
            "statusCode": 500,
            "body": "Missing Airflow credentials in environment variables"
        }

    # Define constants
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet"
    bucket = "lakehouse-nyc-taxi"

    # Extract file name and partition
    file_name = os.path.basename(url)
    try:
        year_month = file_name.split('_')[-1].replace('.parquet', '')  # e.g., 2019-01
        year, month = year_month.split('-')  # e.g., 2019, 01
    except (IndexError, ValueError) as e:
        print(f"Error parsing year and month from {file_name}: {e}")
        return {"statusCode": 500, "body": f"Failed to parse year/month: {str(e)}"}

    # S3 key for Bronze layer
    s3_key = f"bronze/yellow_taxi/{year}/{month}/{file_name}"

    # Download file
    print(f"Downloading {url}")
    response = requests.get(url, stream=True)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        return {"statusCode": 500, "body": f"Download failed: {str(http_err)}"}

    # Upload to S3
    s3_client = boto3.client('s3')
    try:
        print(f"Uploading to s3://{bucket}/{s3_key}")
        s3_client.upload_fileobj(Fileobj=response.raw, Bucket=bucket, Key=s3_key)
    except ClientError as s3_err:
        return {"statusCode": 500, "body": f"S3 upload failed: {str(s3_err)}"}

    # Trigger Airflow DAG
    try:
        trigger_airflow_dag(year, month, s3_key, airflow_username, airflow_password)
    except Exception as dag_err:
        return {"statusCode": 500, "body": f"Airflow trigger failed: {str(dag_err)}"}

    return {"statusCode": 200, "body": f"Successfully processed {s3_key}"}