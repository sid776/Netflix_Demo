from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import subprocess, sys, json, os, pathlib

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_analytics_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    params={
        "config_s3_uri": "s3://my-bucket/configs/config.json",
        "output_s3_uri": "s3://my-bucket/outputs/{{ ds }}/",
        "job_name": "daily_etl",
    },
    description="Run analytics ETL with config-driven setup",
) as dag:

    @task
    def fetch_config(config_s3_uri: str) -> str:
        hook = S3Hook(aws_conn_id="aws_default")
        bucket, key = hook.parse_s3_url(config_s3_uri)
        tmp_path = f"/tmp/{os.path.basename(key)}"
        hook.download_file(key, bucket_name=bucket, local_path=tmp_path)
        return tmp_path

    @task
    def run_extractor(config_path: str, job_name: str) -> str:
        runner = "scripts/run_analytics_job/run_analytics_job.py"
        cmd = [sys.executable, runner, "-config_file", config_path, "-job_name", job_name]
        subprocess.run(cmd, check=True)
        return "/opt/airflow/output"

    @task
    def dq_checks(output_dir: str) -> int:
        files = list(pathlib.Path(output_dir).rglob("*"))
        if not files:
            raise ValueError("No outputs found!")
        return len(files)

    @task
    def upload_outputs(output_dir: str, output_s3_uri: str):
        hook = S3Hook(aws_conn_id="aws_default")
        bucket, prefix = hook.parse_s3_url(output_s3_uri)
        for f in pathlib.Path(output_dir).rglob("*"):
            if f.is_file():
                hook.load_file(str(f), f"{prefix}/{f.name}", bucket_name=bucket, replace=True)

    cfg = fetch_config(dag.params["config_s3_uri"])
    out_dir = run_extractor(cfg, dag.params["job_name"])
    _dq = dq_checks(out_dir)
    upload_outputs(out_dir, dag.params["output_s3_uri"])
