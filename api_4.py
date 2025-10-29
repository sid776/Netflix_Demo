Correct way to run the API on TREX

From your project root:

# 1) activate venv
source venv/bin/activate

# 2) set DB URL (same one you used earlier)
export SQLALCHEMY_DATABASE_URI='mysql+pymysql://<user>:<pass>@<host>/<db>'

# 3) make sure 9999 isn't busy
fuser -n tcp 9999 -k 2>/dev/null || true

# 4) run the dev app with debug on (pick ONE of these)
python run_metadata_dev.py                              # if that file calls app.run(...)
# OR, using Flask runner:
FLASK_APP=run_metadata_dev:create_app flask run --host=0.0.0.0 --port=9999 --debug
# OR, inline (what you did earlier, which is fine):
python - <<'PY'
from run_metadata_dev import create_app
app = create_app()
app.run(host="0.0.0.0", port=9999, debug=True)
PY


You should see:

* Debug mode: on
* Running on http://10.224.82.10:9999/


Don’t run python plat_aiesdp_elt_pipeline or python . — that’s what produced the __main__ error.

Make it reachable from your laptop

Open a terminal on your laptop (not on TREX) and create the SSH tunnel:

# if 9999 is free locally
ssh -N -L 9999:localhost:9999 jenas7@10.224.82.10
# if you get a “bind 127.0.0.1:9999: Permission denied”, use another local port:
# ssh -N -L 5009:localhost:9999 jenas7@10.224.82.10


Keep that window open.

Now hit, from your browser on the laptop:

http://localhost:9999/logs/metadata?key=weather&value=snow

(or http://localhost:5009/... if you used the alternate port)

Tip: / will 404 unless you made a root route. That’s normal.

Sanity checks

On TREX, in another shell:

curl "http://localhost:9999/logs/metadata?key=weather&value=snow"


If that returns JSON, the app is fine; any browser issue is a tunnel problem.

If you see “Internal Server Error,” check the TREX console where Flask is running (since debug=True, the full traceback prints there). Usual culprits we fixed earlier were a missing DB env var or using a None session; your current with Context().session_scope() as session: avoids that.

Optional: keep it running

If you want the server to keep running after you close the SSH session:

# inside venv, with env var set
nohup python run_metadata_dev.py >flask.log 2>&1 &
tail -f flask.log


Shout if any of these steps produce output you’re unsure about, and paste the exact line
