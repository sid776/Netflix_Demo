Step 1 — Sanity-check you’re on the right DB

In the same shell where the API is running, print the URI the app is using:

echo "$SQLALCHEMY_DATABASE_URI"


Make sure it’s the dev RDS you expect (not SQLite or a different schema).

Step 2 — See if any metadata already exists

Quick peek with SQL (use your usual MySQL client) :

SELECT COUNT(*) AS headers FROM log_metadata_header;
SELECT COUNT(*) AS attrs   FROM log_metadata_attr;


If both are 0, you just need seed data.

Step 3 — Insert one demo row (uses your repo/service patterns)

Pick a real raw_data.id from your DB (FK must succeed):

SELECT id FROM raw_data ORDER BY id DESC LIMIT 1;
-- note the id, e.g. 1234


Now, from your project root (venv active), run this tiny one-off Python:

python - <<'PY'
import os
from db.context import Context
from db.models import LogMetadataHeader, LogMetadataAttr
from sqlalchemy.orm import Session

uri = os.environ["SQLALCHEMY_DATABASE_URI"]
Context().init_session(uri)
s: Session = Context().create_new_session()

RAW_DATA_ID = 1234             # <-- replace with a real raw_data.id
LOG_UID     = "demo_ecm_uid_001"
ATTRS       = {"weather": "snow", "country": "US"}

try:
    # upsert header
    hdr = (s.query(LogMetadataHeader)
             .filter(LogMetadataHeader.raw_data_id == RAW_DATA_ID)
             .one_or_none())
    if hdr is None:
        hdr = LogMetadataHeader(raw_data_id=RAW_DATA_ID, log_uid=LOG_UID, source_json={"seed":"demo"})
        s.add(hdr); s.flush()

    # add attrs if missing
    existing = set(s.query(LogMetadataAttr.attr_key, LogMetadataAttr.attr_value)
                     .filter(LogMetadataAttr.header_id == hdr.id).all())
    added = 0
    for k,v in ATTRS.items():
        if (k,v) not in existing:
            s.add(LogMetadataAttr(header_id=hdr.id, attr_key=k, attr_value=v))
            added += 1

    s.commit()
    print("Seeded:", {"header_id": hdr.id, "added_attrs": added})
except:
    s.rollback()
    raise
finally:
    s.close()
PY

Step 4 — Hit the API again
curl "http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50"


Expected:

{
  "count": 1,
  "results": [
    {"log_uid": "demo_ecm_uid_001", "raw_data_id": 1234}
  ]
}

Step 5 — If still zero, check these common causes

Wrong key/value: your seed used different keys (e.g., Weather vs weather) or values (Snow vs snow). The query is an exact match.

Seed went to a different DB: your seed script shell didn’t have the same SQLALCHEMY_DATABASE_URI as the app shell.

FK blocked insert: RAW_DATA_ID wasn’t a real raw_data.id. Re-run with a valid one.

Transaction didn’t commit: the snippet above commits; if you ran something else, make sure it committed.

Optional: use your existing seed script

You already have scripts/create_metadata_tables/seed_metadata.py. Set RAW_DATA_ID, LOG_UID, ATTRS in that file and run:

export SQLALCHEMY_DATABASE_URI=...   # same URI as the API
python scripts/create_metadata_tables/seed_metadata.py


Then hit:

http://localhost:9999/logs/metadata?key=weather&value=snow

Optional: ingest via the DAG utility

Drop a JSON like this in your inbox directory and run the ingest function:

{
  "raw_data_id": 1234,
  "log_uid": "demo_ecm_uid_002",
  "attributes": {"weather":"snow","country":"US"}
}
