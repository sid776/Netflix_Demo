import os
from sqlalchemy.orm import Session
from db.context import Context
from db.models import LogMetadataHeader, LogMetadataAttr

def seed_one(session, raw_data_id, log_uid, attrs, source_json=None):
    # header: 1 row per log (--> raw_data.id)
    header = (session.query(LogMetadataHeader)
              .filter(LogMetadataHeader.raw_data_id == raw_data_id)
              .one_or_none())
    if header is None:
        header = LogMetadataHeader(raw_data_id=raw_data_id, log_uid=log_uid, source_json=source_json)
        session.add(header)
        session.flush()

    # de-dup (header_id, key, value)
    existing = set((a.attr_key, a.attr_value)
                   for a in session.query(LogMetadataAttr).filter(LogMetadataAttr.header_id == header.id))

    added = 0
    for k, v in attrs.items():
        if (k, v) in existing:
            continue
        session.add(LogMetadataAttr(header_id=header.id, attr_key=k, attr_value=v))
        added += 1
    return header.id, added

if __name__ == "__main__":
   
    conn = os.getenv("SQLALCHEMY_DATABASE_URI")
    if not conn:
        raise SystemExit("Set SQLALCHEMY_DATABASE_URI")

    ctx = Context()
    ctx.init_session(conn)
    s = ctx.get_session()  # type: Session

    # --- change these two for your dev data ---
    RAW_DATA_ID = 1234      # any existing raw_data.id in dev
    LOG_UID = "ecm_2025_10_28_demo_001"
    ATTRS = {"weather": "snow", "country": "US", "speed_bucket": "0-5"}
    SRC = {"note": "seeded by smoke script"}

    with ctx.session_scope():
        hid, n = seed_one(s, RAW_DATA_ID, LOG_UID, ATTRS, SRC)
        print("header_id:", hid, "new_attr_rows:", n)
