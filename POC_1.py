1) Activate a Python virtual environment

In the repo root (your prompt shows something like /mnt/c/Users/jenas7/elt/elt-pipeline-15):

# from the repo root
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
# if you have a requirements.txt:
[ -f requirements.txt ] && pip install -r requirements.txt


(When you see (.venv) at the prompt, you’re active. To leave later: deactivate.)

2) Create and switch to a feature branch
git status
git remote -v

# If your Git identity isn't set on this machine:
git config user.name  "Siddharth Jena"
git config user.email "siddharth.jena@cat.com"

# New branch name that maps to your Feature/Task
git checkout -b feature/logdb-poc-scaffolding

3) Add real scaffolding (schema + DAG + API + tests)
3a) Alembic migration (schema starter)

If this repo already uses Alembic, run:

# If alembic is already configured (alembic.ini exists):
alembic revision -m "V001: initial log_meta schema" --autogenerate || true


If not using Alembic, create a migration file and a tiny DDL. Example (adjust paths to your project layout):

mkdir -p db/migrations/versions
cat > db/migrations/versions/V001_initial_log_meta.sql <<'SQL'
-- V001: initial Autonomy Log DB (PoC)
-- NOTE: adapt names/types to your stack (MySQL/Postgres)

CREATE TABLE IF NOT EXISTS taxonomy_v4 (
  id           BIGSERIAL PRIMARY KEY,
  code         VARCHAR(64) UNIQUE NOT NULL,
  name         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_data (
  id           BIGSERIAL PRIMARY KEY,
  s3_key       TEXT NOT NULL,
  size_bytes   BIGINT,
  checksum     VARCHAR(64),
  capture_ts   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ptag (
  id           BIGSERIAL PRIMARY KEY,
  ptag_name    TEXT NOT NULL,
  payload      JSONB
);

CREATE TABLE IF NOT EXISTS log_meta (
  id           BIGSERIAL PRIMARY KEY,
  raw_id       BIGINT NOT NULL REFERENCES raw_data(id),
  ptag_id      BIGINT REFERENCES ptag(id),
  taxonomy_id  BIGINT REFERENCES taxonomy_v4(id),
  machine_id   VARCHAR(64) NOT NULL,
  site_id      VARCHAR(64),
  capture_ts   TIMESTAMP NOT NULL,
  size_bytes   BIGINT,
  checksum     VARCHAR(64),
  created_at   TIMESTAMP DEFAULT now(),
  UNIQUE(machine_id, capture_ts, checksum)
);

CREATE TABLE IF NOT EXISTS attr_def (
  id           BIGSERIAL PRIMARY KEY,
  key          TEXT UNIQUE NOT NULL,
  type         TEXT NOT NULL,
  description  TEXT
);

CREATE TABLE IF NOT EXISTS log_attr (
  log_meta_id  BIGINT NOT NULL REFERENCES log_meta(id) ON DELETE CASCADE,
  attr_def_id  BIGINT NOT NULL REFERENCES attr_def(id) ON DELETE CASCADE,
  value_text   TEXT,
  value_num    DOUBLE PRECISION,
  PRIMARY KEY(log_meta_id, attr_def_id)
);

CREATE TABLE IF NOT EXISTS integrity_report (
  id           BIGSERIAL PRIMARY KEY,
  log_meta_id  BIGINT NOT NULL REFERENCES log_meta(id) ON DELETE CASCADE,
  status       VARCHAR(32) NOT NULL,
  details      JSONB,
  created_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics_report (
  id           BIGSERIAL PRIMARY KEY,
  log_meta_id  BIGINT NOT NULL REFERENCES log_meta(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,
  created_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics_report_detail (
  id            BIGSERIAL PRIMARY KEY,
  report_id     BIGINT NOT NULL REFERENCES analytics_report(id) ON DELETE CASCADE,
  metric_key    TEXT NOT NULL,
  metric_value  DOUBLE PRECISION
);

-- helpful indexes
CREATE INDEX IF NOT EXISTS ix_log_meta_tax ON log_meta(taxonomy_id);
CREATE INDEX IF NOT EXISTS ix_log_attr_text ON log_attr(value_text);
SQL


If you do already have DB migrations in this repo, mirror their style (Alembic env + Python revision instead of .sql). The goal is to commit a first migration that your teammates can inspect.

3b) Airflow DAG skeleton (idempotent ingest)

Create a basic DAG file (path often looks like airflow/dags/ingest_raw_to_meta.py):

mkdir -p airflow/dags
cat > airflow/dags/ingest_raw_to_meta.py <<'PY'
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def list_s3_keys(**_): pass
def parse_and_join_ptag(**_): pass
def upsert_log_meta(**_): pass
def upsert_log_attrs(**_): pass

with DAG(
    dag_id="ingest_raw_to_meta",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["logdb","poc"]
) as dag:
    t1 = PythonOperator(task_id="list_raw", python_callable=list_s3_keys)
    t2 = PythonOperator(task_id="parse_ptag", python_callable=parse_and_join_ptag)
    t3 = PythonOperator(task_id="upsert_meta", python_callable=upsert_log_meta)
    t4 = PythonOperator(task_id="upsert_attrs", python_callable=upsert_log_attrs)
    t1 >> t2 >> t3 >> t4
PY

3c) API skeleton (FastAPI)

Create a tiny service so reviewers can hit something concrete:

mkdir -p api
cat > api/main.py <<'PY'
from fastapi import FastAPI, Query
from typing import Optional

app = FastAPI(title="Autonomy Log DB PoC")

@app.get("/health")
def health(): return {"ok": True}

@app.get("/taxonomies/v4")
def list_taxonomies():
    # TODO: wire to DB
    return [{"id": 1, "code": "EX", "name":"Example"}]

@app.get("/logs/search")
def search_logs(
    tax: Optional[str] = None,
    frm: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = 1, size: int = 50,
):
    # TODO: wire to DB search
    return {"page": page, "size": size, "filters": {"tax": tax, "from": frm, "to": to}, "items": []}
PY


Local run (optional):

pip install fastapi uvicorn
uvicorn api.main:app --reload --port 8010
# Open http://localhost:8010/health

3d) Smoke tests (prove CI will pass)
mkdir -p tests
cat > tests/test_smoke.py <<'PY'
def test_math(): 
    assert 1 + 1 == 2
PY

4) Commit and push
git add -A
git commit -m "PoC scaffolding: initial schema migration (V001), ingest DAG skeleton, FastAPI stub, smoke test"
git push --set-upstream origin feature/logdb-poc-scaffolding


If your remote rejects (no permissions / different remote name), check:

git remote -v
# If remote is 'upstream' instead of 'origin':
git push --set-upstream upstream feature/logdb-poc-scaffolding

5) Open a Draft PR (so it’s visible today)

If you have the GitHub CLI:

# In the repo root
gh pr create --draft \
  --title "[Draft] Autonomy Log DB PoC — schema + DAG + API scaffolding" \
  --body "See description; adds migration V001, DAG ingest_raw_to_meta, FastAPI stub, smoke test. Links: <FeatureID/DesignDoc>."


If not, open your repo in the browser → Compare & pull request → mark Create draft PR → paste the body above. Add reviewers.

6) Optional quick validations (nice screenshots for PR/ADO)

DB migration (if you’re using a local DB):

# example with Postgres in docker compose if you already have it
# or just attach the SQL file for review in PR if DB not ready yet.


Airflow: ensure the DAG imports (from your Airflow env container or dev box):

# run wherever your Airflow dev env is
airflow dags list | grep ingest_raw_to_meta || true


API:

curl http://localhost:8010/health
