from flask import Blueprint, request, jsonify
from db.context import Context
from db.services.log_metadata_service import LogMetadataService

bp_log_metadata = Blueprint("log_metadata", __name__, url_prefix="/logs")


@bp_log_metadata.route("/metadata", methods=["GET"])
def query_log_metadata():
    """
    GET /logs/metadata?key=weather&value=rain&limit=50
    Returns logs where flattened attribute table contains (key=value)
    """
    key = request.args.get("key")
    value = request.args.get("value")
    limit = int(request.args.get("limit", 100))

    if not key or not value:
        return jsonify({"error": "Missing key or value"}), 400

    ctx = Context()
    session = ctx.get_session()
    svc = LogMetadataService()

    try:
        rows = svc.search_by_kv(session, key=key, value=value, limit=limit)
        return jsonify({"count": len(rows), "results": [{"log_uid": r[0], "raw_data_id": r[1]} for r in rows]})
    finally:
        session.close()
