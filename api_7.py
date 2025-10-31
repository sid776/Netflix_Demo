1) Patch your runner to always init the DB in the running process

Use this exact run_metadata_dev.py:

from db.context import Context
from flask import Flask
from __test__.apis.v1.log_metadata import bp_log_metadata
import os

def create_app() -> Flask:
    # 1) read conn string and init engine+sessionmaker
    conn = os.environ.get("SQLALCHEMY_DATABASE_URI")
    if not conn:
        raise RuntimeError("SQLALCHEMY_DATABASE_URI not set")
    Context().init_session(conn)

    # 2) build app and mount blueprint
    app = Flask(__name__)
    app.register_blueprint(bp_log_metadata)

    # optional health
    @app.get("/healthz")
    def healthz():
        # If Context.get_session() returns None, init didn't happen
        return {"ok": Context().get_session() is not None}

    return app

if __name__ == "__main__":
    app = create_app()
    # IMPORTANT: no reloader; keep debug off to avoid double-process issues
    app.run(host="0.0.0.0", port=9999, debug=False, use_reloader=False)


Key points:

Context().init_session(...) runs inside create_app().

debug=False, use_reloader=False prevents the reloader from spawning a child that skips your init.

2) Export the URI in the same terminal before starting

In the very same shell where you’ll run python run_metadata_dev.py:

export SQLALCHEMY_DATABASE_URI='mysql+pymysql://awsadmin:YOURPASS@ics-data-pipeline-db-instance-dev.cydex7wxez73.us-east-1.rds.amazonaws.com/cdcs'
echo "$SQLALCHEMY_DATABASE_URI"  # sanity check (should print the URI)
python run_metadata_dev.py


If you’re using VS Code Remote-SSH, export the variable in that terminal tab, then run the script in that same tab.

3) Test locally

From the same box (or via your SSH tunnel):

curl "http://localhost:9999/healthz"
curl "http://localhost:9999/logs/metadata?key=weather&value=snow&limit=50"


If /healthz returns {"ok": true}, the DB session is initialized. If it’s false or you still see the same error, it means the env var wasn’t seen by the process—recheck step 2.

Why this fixes your error

Previously, the dev reloader (or starting the app in a different shell) meant the running process never executed Context().init_session(...), so Context().get_session() returned None, and your handler (or session_scope) eventually hit 'NoneType' object has no attribute close' or your custom JSON error. Initializing in create_app() and disabling the reloader guarantees the real server process has the engine/sessionmaker.

If you want me to also harden Context.session_scope() so it never .close()s a None (defensive programming), paste your db/context.py and I’ll give you a safe version.
