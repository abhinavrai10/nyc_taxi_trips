import requests
import boto3
import os
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Define constants
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet"
    bucket = "lakehouse-nyc-taxi"
    
    # Extract file name from URL
    file_name = os.path.basename(url)  # e.g., yellow_tripdata_2019-01.parquet
    
    # Parse year and month from file name (assuming format: yellow_tripdata_YYYY-MM.parquet)
    try:
        year_month = file_name.split('_')[-1].replace('.parquet', '')  # e.g., 2019-01
        year, month = year_month.split('-')  # e.g., 2019, 01
    except (IndexError, ValueError) as e:
        print(f"Error parsing year and month from file name {file_name}: {e}")
        return {
            'statusCode': 500,
            'body': f"Failed to parse year and month from file name: {str(e)}"
        }
    
    # Construct s3_key dynamically, keeping month as two-digit number
    s3_key = f"bronze/yellow_taxi/{year}/{month}/{file_name}"
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Download file from URL
        print(f"Downloading file from {url}")
        response = requests.get(url, stream=True)
        
        # Check HTTP status
        try:
            response.raise_for_status()  # Raises exception for 4xx/5xx errors
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return {
                'statusCode': 500,
                'body': f"Failed to download file: {str(http_err)}"
            }
        
        # Upload file to S3
        print(f"Uploading to s3://{bucket}/{s3_key}")
        try:
            s3_client.upload_fileobj(
                Fileobj=response.raw,
                Bucket=bucket,
                Key=s3_key
            )
            print(f"Successfully uploaded {s3_key} to {bucket}")
            return {
                'statusCode': 200,
                'body': f"Successfully uploaded {s3_key} to {bucket}"
            }
        except ClientError as s3_err:
            print(f"S3 error occurred: {s3_err}")
            return {
                'statusCode': 500,
                'body': f"Failed to upload to S3: {str(s3_err)}"
            }
        
    except requests.exceptions.RequestException as req_err:
        # Catch network issues, timeouts, etc.
        print(f"Request error occurred: {req_err}")
        return {
            'statusCode': 500,
            'body': f"Failed to download file: {str(req_err)}"
        }
    except Exception as e:
        # Catch any other unexpected errors
        print(f"Unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': f"Unexpected error: {str(e)}"
        }