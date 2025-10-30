AttributeError: 'NoneType' object has no attribute 'close'
That means your Context.session_scope() yielded None (DB session factory wasn’t available in that process), so when the context manager exits it tries to call session.close() on None.

Two quick, safe fixes:

1) Make the Flask endpoint manage the session explicitly (no context manager)
Replace your current __test__/apis/v1/log_metadata.py handler with this (only the function body matters):

python
Copy code
from flask import Blueprint, request, jsonify
from db.context import Context
from __test__.db.services.log_metadata_service import LogMetadataService  # your current path

bp_log_metadata = Blueprint("log_metadata", __name__, url_prefix="/logs")
svc = LogMetadataService()

@bp_log_metadata.route("/metadata", methods=["GET"])
def query_log_metadata():
    key = request.args.get("key")
    value = request.args.get("value")
    limit = int(request.args.get("limit", 100))

    if not key or not value:
        return jsonify({"error": "Missing key or value"}), 400

    ctx = Context()
    session = ctx.get_session()   # <-- returns None if init_session wasn’t called

    if session is None:
        return jsonify({"error": "DB session is not initialized in this process"}), 500

    try:
        rows = svc.search_by_kv(session=session, key=key, value=value, limit=limit)
        return jsonify({
            "count": len(rows),
            "results": [{"log_uid": r[0], "raw_data_id": r[1]} for r in rows]
        })
    finally:
        try:
            session.close()
        except Exception:
            pass
Why this helps: it avoids calling session.close() on None and fails clearly if Context.init_session(...) wasn’t run in this process.

2) (Optional but recommended) Harden the context manager
Open db/context.py and make session_scope() tolerant:

python
Copy code
from contextlib import contextmanager

@contextmanager
def session_scope(self):
    session = self.get_session()
    if session is None:
        # yield None so callers can decide what to do;
        # and don't try to rollback/close on exit.
        yield None
        return
    try:
        yield session
        session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            session.close()
        except Exception:
            pass
(If you adopt #1 above, this #2 is a nice extra guard for any other code paths.)

Ensure the session factory is initialized in THIS process
In run_metadata_dev.py you already have:

python
Copy code
from db.context import Context
from flask import Flask
from __test__.apis.v1.log_metadata import bp_log_metadata
import os

def create_app():
    # MUST be set in the environment before you run this script.
    Context().init_session(os.environ["SQLALCHEMY_DATABASE_URI"])

    app = Flask(__name__)
    app.register_blueprint(bp_log_metadata)
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=9999, debug=True)
Double-check you export the URI in the same shell you start Flask:

bash
Copy code
# in your venv shell on trex
export SQLALCHEMY_DATABASE_URI='mysql+pymysql://awsadmin:Initialpwd...@<host>/<db>'
python run_metadata_dev.py
You should see:

pgsql
Copy code
* Serving Flask app 'run_metadata_dev'
* Debug mode: on
* Running on http://0.0.0.0:9999/ (Press CTRL+C to quit)
Test it
On the trex box:

bash
Copy code
curl "http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50"
If you’re accessing from your laptop, keep the SSH tunnel up:

bash
Copy code
ssh -L 9999:localhost:9999 jenas7@10.224.82.10
# then in your browser on your laptop:
# http://localhost:9999/logs/metadata?key=weather&value=snow
Expected JSON (if you seeded):

json
Copy code
{
  "count": 1,
  "results": [{"log_uid": "ecm_2025_10_28_demo_001", "raw_data_id": 1234}]
}
If your tables are empty you’ll get:

json
Copy code
{"count": 0, "results": []}
Why the error happened
The process running the Flask app didn’t have its SQLAlchemy session factory initialized (or Context().get_session() returned None).

Your session_scope() unconditionally called rollback() / close() on the session variable, which was None, hence 'NoneType' object has no attribute 'close'.

By initializing the context at startup and defensively handling None you avoid the crash.

Quick checklist
export SQLALCHEMY_DATABASE_URI=... in the same shell.

Start the app: python run_metadata_dev.py.

Hit: http://localhost:9999/logs/metadata?key=weather&value=snow.

If you still see 500s, read the traceback in the terminal—now it should show a clear message if the DB session is missing.







