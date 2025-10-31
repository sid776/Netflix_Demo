1) Minimal run_metadata_dev.py
# run_metadata_dev.py
import os
from flask import Flask
from db.context import Context

def create_app():
    # 1) Init the global engine + default session ONCE for this process
    uri = os.environ.get("SQLALCHEMY_DATABASE_URI")
    if not uri:
        raise RuntimeError("Set SQLALCHEMY_DATABASE_URI before starting the app")
    Context().init_session(uri)

    # 2) Create Flask app
    app = Flask(__name__)

    # 3) Register Context's request hooks so g.session is created/cleaned
    app.before_request(Context().before_flask_request)
    # Either of these is fine; using teardown ensures we always close even on exceptions
    app.teardown_request(lambda exc: Context().after_flask_request())

    # 4) Register your blueprint (IMPORT AFTER init to avoid early DB usage)
    from __test__.apis.v1.log_metadata import bp_log_metadata
    app.register_blueprint(bp_log_metadata)

    # 5) quick healthcheck
    @app.get("/healthz")
    def healthz():
        # returns True when a request-bound session exists
        return {"has_request_session": (Context().get_session() is not None)}

    return app

if __name__ == "__main__":
    app = create_app()
    # IMPORTANT: no reloader → single process keeps the initialized Context
    app.run(host="0.0.0.0", port=9999, debug=False, use_reloader=False)

Why this fixes your error


Your earlier 500s (“NoneType has no attribute query/close/rollback”) came from Context().get_session() returning None because g.session hadn’t been set.


The two hooks above ensure every request gets g.session = new Session(), and it’s committed/closed automatically at the end of the request.



2) Keep your blueprint simple
Your existing blueprint can stay as-is; it already uses Context().get_session() or Context().session_scope(). With the hooks in place, both work. If you want the simplest version:
# __test__/apis/v1/log_metadata.py
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

    session = Context().get_session()  # request-bound session from g.session
    rows = svc.search_by_kv(session, key=key, value=value, limit=limit)
    return jsonify({
        "count": len(rows),
        "results": [{"log_uid": r[0], "raw_data_id": r[1]} for r in rows]
    })

(If you prefer with Context().session_scope() as s: ..., that also works now because get_session() will return the request session. Your earlier crash was because it returned None.)

3) Run it correctly
In the same shell (your venv), set the URI and start the app:
# activate your venv first
source venv/bin/activate

# set DB connection for THIS shell
export SQLALCHEMY_DATABASE_URI='mysql+pymysql://awsadmin:***@ics-data-pipeline-db-instance-dev.../cdcs'

# start
python run_metadata_dev.py

You should see:
* Running on http://10.224.82.10:9999/

If you’re on your laptop, tunnel or use VS Code port forwarding:
# From your laptop
ssh -L 9999:localhost:9999 jenas7@10.224.82.10

Now hit:
http://localhost:9999/healthz       -> {"has_request_session": true}
http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50


4) Common gotchas (quick checks)


Saw the same error again? Confirm the two lines are really there:


app.before_request(Context().before_flask_request)


app.teardown_request(lambda exc: Context().after_flask_request())




Reloader spawned a new process (lost init): ensure use_reloader=False.


Different shell: re-export SQLALCHEMY_DATABASE_URI after opening a new terminal.


Wrong import path for blueprint: you’re using from __test__.apis.v1.log_metadata import bp_log_metadata—keep PYTHONPATH pointed at your project root if needed:
export PYTHONPATH=$(pwd)

(This is print working directory, not a password.)


This wiring respects your enterprise Context exactly as-is and cures the NoneType session errors by guaranteeing a request-scoped session exists for each API call.
