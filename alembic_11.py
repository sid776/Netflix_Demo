1) Verify the migration actually created the tables
Option A — quick Python (no mysql client needed)
# in repo root, with your venv active and SQLALCHEMY_DATABASE_URI exported
python - <<'PY'
import os, sqlalchemy as sa
uri = os.environ["SQLALCHEMY_DATABASE_URI"]
e = sa.create_engine(uri)
insp = sa.inspect(e)
for t in ("log_metadata_header", "log_metadata_attr"):
    print(t, "=>", "OK" if insp.has_table(t) else "MISSING")
    if insp.has_table(t):
        print("  columns:", [c['name'] for c in insp.get_columns(t)])
PY

Option B — Adminer (per wiki)

Open Adminer and log in to dev DB.

Confirm you can see tables:

log_metadata_header

log_metadata_attr

If either table shows MISSING, stop here and tell me which one; otherwise continue.

2) Grab a valid raw_data.id to use for seeding
python - <<'PY'
import os, sqlalchemy as sa
uri = os.environ["SQLALCHEMY_DATABASE_URI"]
e = sa.create_engine(uri)
with e.connect() as c:
    row = c.execute(sa.text("SELECT id, file_path FROM raw_data ORDER BY id DESC LIMIT 1")).fetchone()
    print("RAW_DATA_ID=", row[0], "\nFILE=", row[1])
PY


Copy the printed RAW_DATA_ID.

3) Seed one demo header + a few attributes (smoke data)

If you committed scripts/create_metadata_tables/seed_metadata.py, run it; otherwise use this inline one-liner:

python - <<'PY'
import os, sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from db.models import LogMetadataHeader, LogMetadataAttr

uri = os.environ["SQLALCHEMY_DATABASE_URI"]
RAW_DATA_ID = int(os.environ.get("RAW_DATA_ID", "0"))  # export this first
LOG_UID = os.environ.get("LOG_UID", "ecm_demo_001")
ATTRS = {"weather":"snow","country":"US","speed_bucket":"0-5"}

e = sa.create_engine(uri, pool_pre_ping=True)
Session = sessionmaker(bind=e)
s = Session()

# upsert-like seed
hdr = s.query(LogMetadataHeader).filter_by(raw_data_id=RAW_DATA_ID).one_or_none()
if hdr is None:
    hdr = LogMetadataHeader(raw_data_id=RAW_DATA_ID, log_uid=LOG_UID, source_json={"note":"seed"})
    s.add(hdr); s.flush()
existing = {(a.attr_key, a.attr_value) for a in s.query(LogMetadataAttr).filter_by(header_id=hdr.id)}
added=0
for k,v in ATTRS.items():
    if (k,v) in existing: continue
    s.add(LogMetadataAttr(header_id=hdr.id, attr_key=k, attr_value=v)); added+=1
s.commit()
print("Seeded header_id=", hdr.id, "added_attr_rows=", added)
PY


Example exports:

export RAW_DATA_ID=<the id from step 2>
export LOG_UID=ecm_2025_10_28_demo_001


Quick DB spot-check:

python - <<'PY'
import os, sqlalchemy as sa
e = sa.create_engine(os.environ["SQLALCHEMY_DATABASE_URI"])
with e.connect() as c:
    print(c.execute(sa.text("SELECT COUNT(*) FROM log_metadata_header")).scalar(), "headers")
    print(c.execute(sa.text("SELECT COUNT(*) FROM log_metadata_attr")).scalar(), "attrs")
PY

4) Query via the Flask endpoint (or directly via the service)
4a) Flask route (if you registered the blueprint)

You added apis/v1/log_metadata.py with bp_log_metadata. Make sure it’s registered:

In your Flask app factory (often apis/app.py or similar):

from apis.v1.log_metadata import bp_log_metadata
app.register_blueprint(bp_log_metadata)


Run the app (pick the command your repo uses, e.g.):

# common dev runner in this repo looks like:
python api_dev.py
# or sometimes: flask run --host=0.0.0.0 --port=9999


Hit the endpoint:

curl "http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50"
# or replace 9999 with your app port


You should see JSON like:

{"count": 1, "results": [{"log_uid": "ecm_2025_10_28_demo_001", "raw_data_id": 1234}]}

4b) Direct service call (no web)
python - <<'PY'
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.services.log_metadata_service import LogMetadataService

e = create_engine(os.environ["SQLALCHEMY_DATABASE_URI"], pool_pre_ping=True)
Session = sessionmaker(bind=e)
s = Session()
rows = LogMetadataService().search_by_kv(s, key="weather", value="snow", limit=10)
print(rows)
PY

5) (Optional) Run the Airflow DAG logic without Airflow

If Airflow isn’t wired up locally, you can exercise the DAG’s ingestion callable directly.

Create a sample JSON in your inbox:

export METADATA_DROP_DIR=/tmp/metadata_inbox
mkdir -p "$METADATA_DROP_DIR"

cat > "$METADATA_DROP_DIR/demo.json" <<JSON
{
  "raw_data_id": $RAW_DATA_ID,
  "log_uid": "$LOG_UID",
  "attributes": { "weather": "snow", "country": "US", "speed_bucket": "0-5" }
}
JSON


Execute the Python in the DAG file:

python - <<'PY'
import os, json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utilities.metadata_repo import upsert_header_with_attrs

DB_URI = os.environ["SQLALCHEMY_DATABASE_URI"]
INBOX = os.environ.get("METADATA_DROP_DIR", "/opt/airflow/metadata_inbox")

e = create_engine(DB_URI, pool_pre_ping=True)
Session = sessionmaker(bind=e)

def _ingest_path(p):
    import json
    s = Session()
    try:
        d = json.load(open(p))
        upsert_header_with_attrs(
            raw_data_id=int(d["raw_data_id"]),
            log_uid=d["log_uid"],
            attrs=d.get("attributes", {}),
            source_json=d,
            session=s
        )
        s.commit()
        print("ingested:", p)
    except Exception as ex:
        s.rollback(); raise
    finally:
        s.close()

for n in os.listdir(INBOX):
    if n.endswith(".json"):
        _ingest_path(os.path.join(INBOX, n))
PY


Then re-run the query in step 4 to confirm it shows up.

6) What to say in stand-up (short + crisp)

“Created two new tables (log_metadata_header, log_metadata_attr) via Alembic; verified in dev DB.”

“Added repository + service layer (utilities/metadata_repo.py, db/services/log_metadata_service.py) to upsert headers/attributes and search by key/value.”

“Exposed a simple Flask GET /logs/metadata endpoint to query logs by attributes.”

“Wrote a seed/smoke script and tested E2E: seed → query returns expected log_uid/raw_data_id.”

“Prepared an Airflow ingestion DAG that watches a drop folder and upserts attributes from JSON manifests; validated the callable locally without full Airflow.”

Next up:

“Hook the Flask blueprint in the main app (if not already), add unit tests, and wire the Airflow DAG into the dev scheduler.”

“Add a couple more search filters (multiple keys, pagination) and basic index review.”
