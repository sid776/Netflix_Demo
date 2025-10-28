from datetime import datetime
import json, os
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utilities.metadata_repo import upsert_header_with_attrs

DB_URI = os.environ["SQLALCHEMY_DATABASE_URI"]                 
INBOX = os.environ.get("METADATA_DROP_DIR", "/opt/airflow/metadata_inbox")

engine = create_engine(DB_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def _ingest_path(json_path: str):
    with open(json_path, "r") as f:
        payload = json.load(f)
    raw_data_id = int(payload["raw_data_id"])
    log_uid = payload["log_uid"]
    attrs = payload.get("attributes", {})

    db = SessionLocal()
    try:
        upsert_header_with_attrs(raw_data_id=raw_data_id, log_uid=log_uid, attrs=attrs, source_json=payload, session=db)
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()

def _scan_and_ingest():
    if not os.path.isdir(INBOX):
        return
    for name in os.listdir(INBOX):
        if name.endswith(".json"):
            _ingest_path(os.path.join(INBOX, name))

with DAG(
    dag_id="ingest_log_metadata",
    start_date=datetime(2025,1,1),
    schedule_interval=None,      # manual/event-driven for PoC
    catchup=False,
    tags=["metadata","logs"],
) as dag:
    scan = PythonOperator(task_id="scan_and_ingest_json", python_callable=_scan_and_ingest)
