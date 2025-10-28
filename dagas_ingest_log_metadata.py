# -*- coding: utf-8 -*-
"""
Airflow DAG: Ingest ECM log metadata JSON -> log_metadata_header / log_metadata_attr

How it works
------------
- Scans either a local folder (glob) or an S3 prefix (choose via env vars).
- For each JSON file, loads payload:
    {
      "raw_data_id": 12345,                # REQUIRED
      "log_uid": "hlog_2025_01_07_abc",    # REQUIRED
      "attrs": {                            # REQUIRED (can be empty dict)
        "weather": "snow",
        "site": "AZ",
        "vehicle": "793F"
      },
      "source_json": {...}                  # OPTIONAL: original blob for traceability
    }
- Calls metadata_repo.upsert_header_with_attrs() so it’s safe to re-run (no dupes).
- Writes a small success/failure summary into Airflow logs.

Configuration (env vars)
------------------------
- SOURCE_MODE = "local" or "s3"   (default: "local")
- LOCAL_GLOB = "/data/metadata/*.json"   (if SOURCE_MODE=local)
- S3_BUCKET, S3_PREFIX = "bucket-name", "ecm/metadata/"  (if SOURCE_MODE=s3)
- (Optional) BATCH_LIMIT = "1000"   -> cap the number of files per run
- SQLALCHEMY_DATABASE_URI must be available to tasks (Context uses it).
"""

from __future__ import print_function

import json
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# If you already vendor boto3/utilities elsewhere, these imports will still work.
try:
    import boto3
except Exception:
    boto3 = None

from db.context import Context
from db.models import LogMetadataHeader, LogMetadataAttr  # ensures tables are known to metadata
# Put the helper where you committed it:
# e.g. plat_aiesdp_elt_pipeline/test/_utilities/metadata_repo.py  (adjust import path if needed)
from test._utilities.metadata_repo import upsert_header_with_attrs  # <-- update path if different


# ---------- helpers ----------

def _iter_local_paths(glob_pattern, limit):
    import glob
    count = 0
    for p in glob.glob(glob_pattern):
        if p.lower().endswith(".json"):
            yield p
            count += 1
            if limit and count >= limit:
                break


def _iter_s3_keys(bucket, prefix, limit):
    if boto3 is None:
        raise RuntimeError("boto3 not available but SOURCE_MODE=s3 was selected")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".json"):
                yield key
                count += 1
                if limit and count >= limit:
                    return


def _load_s3_json(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return json.loads(body.decode("utf-8"))


def _load_local_json(path):
    with open(path, "r") as f:
        return json.load(f)


def _validate_payload(payload, file_id):
    missing = []
    if not isinstance(payload, dict):
        raise ValueError("payload is not an object")
    if "raw_data_id" not in payload:
        missing.append("raw_data_id")
    if "log_uid" not in payload:
        missing.append("log_uid")
    if "attrs" not in payload:
        missing.append("attrs")
    if missing:
        raise ValueError("missing required fields in {}: {}".format(file_id, ", ".join(missing)))

    if not isinstance(payload.get("attrs", {}), dict):
        raise ValueError("attrs must be an object (key/value dict) in {}".format(file_id))


def _ingest_one_payload(payload, session):
    raw_data_id = int(payload["raw_data_id"])
    log_uid = str(payload["log_uid"])
    attrs = payload.get("attrs", {}) or {}
    source_json = payload.get("source_json")

    header, added = upsert_header_with_attrs(
        raw_data_id=raw_data_id,
        log_uid=log_uid,
        attrs=attrs,
        source_json=source_json,
        session=session,
    )
    return header.id, added


def ingest_log_metadata(**context):
    log = logging.getLogger("ingest_log_metadata")
    mode = os.getenv("SOURCE_MODE", "local").lower()

    batch_limit = os.getenv("BATCH_LIMIT", "")
    try:
        batch_limit = int(batch_limit) if batch_limit else None
    except Exception:
        batch_limit = None

    processed = 0
    created_attrs = 0
    errors = []

    # Ensure DB connectivity early
    _ = Context().get_session()  # initializes engine if needed

    if mode == "s3":
        bucket = os.getenv("S3_BUCKET")
        prefix = os.getenv("S3_PREFIX", "")
        if not bucket:
            raise RuntimeError("S3_BUCKET is required when SOURCE_MODE=s3")

        for key in _iter_s3_keys(bucket, prefix, batch_limit):
            try:
                payload = _load_s3_json(bucket, key)
                _validate_payload(payload, "s3://{}/{}".format(bucket, key))
                with Context().session_scope() as s:
                    _, added = _ingest_one_payload(payload, session=s)
                    created_attrs += added
                processed += 1
                log.info("OK s3://%s/%s (new attrs: %s)", bucket, key, added)
            except Exception as e:
                errors.append("s3://{}/{} -> {}".format(bucket, key, repr(e)))
                log.exception("Failed for s3://%s/%s", bucket, key)

    else:
        glob_pattern = os.getenv("LOCAL_GLOB", "/data/metadata/*.json")
        for path in _iter_local_paths(glob_pattern, batch_limit):
            try:
                payload = _load_local_json(path)
                _validate_payload(payload, path)
                with Context().session_scope() as s:
                    _, added = _ingest_one_payload(payload, session=s)
                    created_attrs += added
                processed += 1
                log.info("OK %s (new attrs: %s)", path, added)
            except Exception as e:
                errors.append("{} -> {}".format(path, repr(e)))
                log.exception("Failed for %s", path)

    # Push a short summary to logs/XCom
    summary = {
        "processed_files": processed,
        "new_attr_rows": created_attrs,
        "errors": errors[:10],  # cap in xcom
    }
    log.info("Ingestion summary: %s", summary)
    return summary


# ---------- DAG ----------

default_args = {
    "owner": "aiesdp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_log_metadata",
    default_args=default_args,
    description="Ingest ECM log metadata JSON into log_metadata_header / log_metadata_attr",
    schedule_interval="@hourly",          # adjust to your need
    start_date=datetime(2025, 1, 1),      # choose a safe backfill start
    catchup=False,
    max_active_runs=1,
    tags=["autonomy", "metadata", "ecm"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest_log_metadata,
        provide_context=True,
    )
