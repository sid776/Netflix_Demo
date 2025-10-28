✅ ✅ OPTION 1 — SSH PORT FORWARDING (recommended)

Run this ON YOUR LOCAL MACHINE, NOT inside Trex:

ssh -L 9999:localhost:9999 jenas7@10.224.82.10


Then, in your laptop browser:

✅ Open → http://localhost:9999/logs/metadata?key=x&value=y

This tunnels:

(local laptop) localhost:9999  --->  (trex) localhost:9999

✅ OPTION 2 — CURL inside Trex box

If you ONLY need to test the API (not view in browser), run directly on Trex:

curl "http://localhost:9999/logs/metadata?key=weather&value=snow"


Or if raw_data_id/test data exist:

curl "http://localhost:9999/logs/metadata?key=speed_bucket&value=0-5"


✅ This should return JSON response like:

{
  "count": 1,
  "results": [
    {"log_uid": "ecm_2025_10_28_demo_001", "raw_data_id": 1234}
  ]
}

✅ OPTION 3 — Use the built-in Flask test client

Inside Python REPL on Trex:

from run_metadata_dev import app
client = app.test_client()
client.get("/logs/metadata?key=weather&value=snow").json

✅ Confirming your API is running

Your screenshot shows:

 * Running on http://10.224.82.10:9999/


✅ That means the app is running perfectly.

🔴 But you CANNOT reach it from home without tunneling.

✅ Why SSH Tunneling Is Required

Your Trex devbox is inside:

✅ internal Caterpillar VPC
✅ internal VLAN
✅ firewall prevents inbound access
✅ only SSH inbound allowed

So you must tunnel the web port through SSH.

✅ EXACT COMMAND TO RUN (COPY/PASTE)
On your laptop terminal:
ssh -L 9999:localhost:9999 jenas7@10.224.82.10


Enter devbox password → tunnel starts.

Then open:

✅ http://localhost:9999/logs/metadata?key=snow&value=weather

(or whatever)

✅ NEXT STEPS (for demo)

Now that Flask is running:

✅ 1. Seed your metadata table (so endpoint returns data)

Inside Trex:

python seed_metadata.py

✅ 2. Query it through your API
curl "http://localhost:9999/logs/metadata?key=weather&value=snow"


OR through SSH tunnel from browser.

✅ 3. Show this working in the demo:

✅ Metadata tables created
✅ SQLAlchemy ingestion (upsert) works
✅ Flask search API works
✅ Raw → header → attributes linkage works
✅ End-to-end metadata MVP complete
