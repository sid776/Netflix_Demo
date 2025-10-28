Good news—the traceback is super clear: your Flask view is passing a None session into the service, so inside search_by_kv the line that does session.query(...) explodes with:

AttributeError: 'NoneType' object has no attribute 'query'


That happens when your Context was never initialized with a DB URL in this process, so Context().get_session() returned None.

Here’s the fix, step-by-step.

1) Ensure the DB URL is exported in the shell you run Flask from
# in the same terminal where you run python run_metadata_dev.py
export SQLALCHEMY_DATABASE_URI='mysql+pymysql://<user>:<pass>@oss-db-service/cdcs'


(Use the exact creds/host you used for Alembic.)

2) Initialize the Context before the app starts

Edit run_metadata_dev.py (or wherever you start the server) and add a one-time init:

# run_metadata_dev.py
from apis.app import create_app
from db.context import Context
import os

# bootstrap DB session factory once for this process
Context().init_session(os.environ["SQLALCHEMY_DATABASE_URI"])

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9999)


If you don’t want to rely on an env var, you can hardcode a dev URL just for this runner (but env var is cleaner).

3) Use a real session in the view (safe pattern)

Update your blueprint to use session_scope() so you always get a valid session and it’s closed properly:

# __test__/apis/v1/log_metadata.py  (your file in the screenshot)
from flask import Blueprint, request, jsonify
from db.context import Context
from db.services.log_metadata_service import LogMetadataService

bp_log_metadata = Blueprint("log_metadata", __name__, url_prefix="/logs")
svc = LogMetadataService()

@bp_log_metadata.route("/metadata", methods=["GET"])
def query_log_metadata():
    key = request.args.get("key")
    value = request.args.get("value")
    limit = int(request.args.get("limit", 100))
    if not key or not value:
        return jsonify({"error": "Missing key or value"}), 400

    # guaranteed non-None session + auto-commit/close
    with Context().session_scope() as session:
        rows = svc.search_by_kv(session=session, key=key, value=value, limit=limit)
        return jsonify({
            "count": len(rows),
            "results": [{"log_uid": r[0], "raw_data_id": r[1]} for r in rows]
        })


(If you keep your old pattern session = Context().get_session(), it will still work as long as step #2 has initialized the Context, but session_scope() is safer.)

4) Restart & test
# same terminal
python run_metadata_dev.py


In another local terminal (with SSH tunnel already up) or on the Trex box:

curl "http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50"
# or from Trex without a tunnel:
curl "http://127.0.0.1:9999/logs/metadata?key=weather&value=snow&limit=50"


Expected JSON (example):

{
  "count": 1,
  "results": [
    {"log_uid": "ecm_2025_10_28_demo_001", "raw_data_id": 1234}
  ]
}

Why this happened

Alembic used the env var and worked because its process had SQLALCHEMY_DATABASE_URI set.

Your Flask runner was started from a shell that didn’t export the var, so Context never initialized → get_session() returned None.

The two changes (export + Context().init_session(...)) make this process robust.

If you still see 500s after this, paste the new stack trace and the exact run_metadata_dev.py and I’ll adjust.
