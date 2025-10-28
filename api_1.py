Fix + Run in 3 steps
1) Make the service Py3.6-safe

Your db/services/log_metadata_service.py currently starts with from __future__ import annotations (Py3.7+ feature). Remove that line.

# db/services/log_metadata_service.py
# DELETE this line:
# from __future__ import annotations


The rest of the typing in that file is Py3.6-compatible.

If you see any dict | None or Session | None anywhere, replace with Optional[dict]/Optional[Session]. (Your current service file already uses Optional[...], so you’re good.)

2) Add a minimal app that only registers our endpoint

Create a new file at the repo root (or wherever you run) called run_metadata_dev.py:

# run_metadata_dev.py
from flask import Flask
from apis.v1.log_metadata import bp_log_metadata  # our blueprint only

def create_min_app():
    app = Flask(__name__)
    app.register_blueprint(bp_log_metadata)  # mounts /logs/metadata
    return app

if __name__ == "__main__":
    app = create_min_app()
    app.run(host="0.0.0.0", port=9999)


This avoids importing apis/app.py (which drags in apis/v1/annotation.py and legacy stuff that breaks on Py3.6).

3) Run it
# activate venv
source .venv/bin/activate

# DB connection (use the creds your teammate gave)
export SQLALCHEMY_DATABASE_URI='mysql+pymysql://<user>:<pass>@<host>/<db>'

# start the minimal server
python run_metadata_dev.py


Test the GET:

curl -s "http://localhost:9999/logs/metadata?key=weather&value=snow"


If you haven’t seeded any rows yet, you’ll see {"count": 0, "results": []}.

If you seeded (e.g., via seed_metadata.py), you’ll get matches like:

{
  "count": 1,
  "results": [{"log_uid":"ecm_2025_10_28_demo_001","raw_data_id":1234}]
}

Why your current api_dev.py failed

api_dev.py calls from apis.app import create_app, and that app factory imports all v1 routes (e.g., apis/v1/annotation.py), which in turn imports db/container.py and other modules that:

include Py3.7+ features (e.g., from __future__ import annotations, walrus operator, etc.), or

expect packages not installed.

By isolating to just our blueprint, we dodge those legacy dependencies.

If you still want to keep using api_dev.py

You can make the main app factory conditional so it only registers our blueprint when an env var is set:

# in apis/app.py
import os
from flask import Flask

def create_app():
    app = Flask(__name__)
    mode = os.getenv("APP_MODE", "full")

    if mode == "metadata_only":
        from apis.v1.log_metadata import bp_log_metadata
        app.register_blueprint(bp_log_metadata)
    else:
        # existing behavior that registers everything
        ...
    return app


Then run:

export APP_MODE=metadata_only
python api_dev.py


…but the minimal app approach is simpler and safest on the devbox.

Quick sanity checks

If you hit errors:

ModuleNotFoundError: flask → pip install "flask<2.1" (Flask 2.2+ may pull deps that dislike Py3.6).

TypeError: Invalid type annotation anywhere → remove from __future__ import annotations from that module, or don’t import that module at all (the minimal app prevents most of these).

404 on the route → ensure the minimal app registers bp_log_metadata and you’re calling /logs/metadata?key=...&value=....
