from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

# Configurations
BUCKET = "lakehouse-nyc-taxi"
SILVER_JOB_NAME = "job_silver"
GOLD_JOB_NAME = "job_gold"
BRONZE_CRAWLER = "NYCTaxiBronzeCrawler"
SILVER_CRAWLER = "NYCTaxiSilverCrawler"
GOLD_CRAWLER = "NYCTaxiGoldCrawler"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@yourcompany.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def validate_conf(**context):
    conf = context.get("dag_run").conf or {}
    year = conf.get('year', '2019')  # Default value
    month = conf.get('month', '01')  # Default value
    if not year or not month:
        raise ValueError("Missing required parameters: year/month")
    print(f"Triggered for Year: {year}, Month: {month}")
    return conf

with DAG(
    dag_id="bronze_to_gold_pipeline",
    description="End-to-end orchestration for NYC Taxi Lakehouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval=None,  # Triggered by Lambda
    max_active_runs=1,
    tags=["nyc-taxi", "etl", "lakehouse"],
) as dag:

    # 1. Validate and extract parameters
    validate_params = PythonOperator(
        task_id="validate_params",
        python_callable=validate_conf,
        provide_context=True,
    )

    # 2. Run Bronze Crawler
    run_bronze_crawler = GlueCrawlerOperator(
        task_id="run_bronze_crawler",
        config={"Name": BRONZE_CRAWLER},
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    # 3. Run Silver ETL
    run_silver_etl = GlueJobOperator(
        task_id="run_silver_etl",
        job_name=SILVER_JOB_NAME,
        iam_role_name="GlueNYCTaxiRole",
        script_args={
            "--year": "{{ dag_run.conf['year'] }}",
            "--month": "{{ dag_run.conf['month'] }}",
            "--JOB_NAME": SILVER_JOB_NAME,
        },
        s3_bucket="aws-glue-assets-588297155433-us-east-1", 
        script_location="s3://aws-glue-assets-588297155433-us-east-1/scripts/job_silver.py",  
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        create_job_kwargs={
            "GlueVersion": "5.0",  # Enforce Glue 5.0
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://aws-glue-assets-588297155433-us-east-1/scripts/job_silver.py",
                "PythonVersion": "3"
            },
            "DefaultArguments": {
                "--enable-job-insights": "false",
                "--job-language": "python"
            },
            "Role": "arn:aws:iam::588297155433:role/GlueNYCTaxiRole"
        },
        update_config=True,
    )

    # 4. Run Silver Crawler
    run_silver_crawler = GlueCrawlerOperator(
        task_id="run_silver_crawler",
        config={"Name": SILVER_CRAWLER},
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    # 5. Run Gold ETL (incremental)
    run_gold_etl = GlueJobOperator(
        task_id="run_gold_etl",
        job_name=GOLD_JOB_NAME,
        script_args={"--JOB_NAME": GOLD_JOB_NAME},
        iam_role_name="GlueNYCTaxiRole",
        s3_bucket="aws-glue-assets-588297155433-us-east-1", 
        script_location="s3://aws-glue-assets-588297155433-us-east-1/scripts/job_gold.py",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        create_job_kwargs={
            "GlueVersion": "5.0",  # Enforce Glue 5.0
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://aws-glue-assets-588297155433-us-east-1/scripts/job_gold.py",
                "PythonVersion": "3"
            },
            "DefaultArguments": {
                "--enable-job-insights": "false",
                "--job-language": "python"
            },
            "Role": "arn:aws:iam::588297155433:role/GlueNYCTaxiRole"
        },
        update_config=True,
    )

    # 6. Run Gold Crawler
    run_gold_crawler = GlueCrawlerOperator(
        task_id="run_gold_crawler",
        config={"Name": GOLD_CRAWLER},
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    validate_params >> run_silver_etl >> run_silver_crawler >> run_gold_etl >> run_gold_crawler
    # validate_params >> run_silver_etl  >> run_gold_etl 