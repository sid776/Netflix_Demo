from typing import Dict, Tuple
from sqlalchemy.orm import Session
from db.context import Context
from db.models import LogMetadataHeader, LogMetadataAttr

def upsert_header_with_attrs(
    raw_data_id: int,
    log_uid: str,
    attrs: Dict[str, str],
    source_json: dict | None = None,
    session: Session | None = None,
) -> Tuple[LogMetadataHeader, int]:
    """
    Creates/updates a header for a given raw_data_id and inserts missing key/value pairs.
    Returns (header, number_of_new_attr_rows).
    """
    session = session or Context().get_session()

    header = (
        session.query(LogMetadataHeader)
        .filter(LogMetadataHeader.raw_data_id == raw_data_id)
        .one_or_none()
    )

    if header is None:
        header = LogMetadataHeader(raw_data_id=raw_data_id, log_uid=log_uid, source_json=source_json)
        session.add(header)
        session.flush()  # get header.id

    # de-dup existing (header_id, key, value)
    existing = {
        (a.attr_key, a.attr_value)
        for a in session.query(LogMetadataAttr).filter(LogMetadataAttr.header_id == header.id)
    }

    added = 0
    for k, v in (attrs or {}).items():
        if (k, v) in existing:
            continue
        session.add(LogMetadataAttr(header_id=header.id, attr_key=k, attr_value=v))
        added += 1

    return header, added
