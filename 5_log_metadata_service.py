from __future__ import annotations
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from db.context import Context
from db.models import LogMetadataAttr, LogMetadataHeader


class LogMetadataService:
    """CRUD + query helpers for the parallel log metadata tables."""

    def get_header_by_raw_data_id(self, session: Session, raw_data_id: int) -> Optional[LogMetadataHeader]:
        return session.execute(
            select(LogMetadataHeader).where(LogMetadataHeader.raw_data_id == raw_data_id)
        ).scalars().first()

    def get_header_by_uid(self, session: Session, log_uid: str) -> Optional[LogMetadataHeader]:
        return session.execute(
            select(LogMetadataHeader).where(LogMetadataHeader.log_uid == log_uid)
        ).scalars().first()

    def ensure_header(
        self,
        session: Session,
        *,
        raw_data_id: int,
        log_uid: str,
        source_json: Optional[dict] = None,
    ) -> LogMetadataHeader:
        hdr = self.get_header_by_raw_data_id(session, raw_data_id)
        if hdr:
            # keep existing; optionally refresh json
            if source_json:
                hdr.source_json = source_json
            return hdr

        hdr = LogMetadataHeader(raw_data_id=raw_data_id, log_uid=log_uid, source_json=source_json)
        session.add(hdr)
        session.flush()  # get hdr.id
        return hdr

    def upsert_attr(
        self, session: Session, *, header_id: int, key: str, value: str
    ) -> LogMetadataAttr:
        row = session.execute(
            select(LogMetadataAttr).where(
                and_(
                    LogMetadataAttr.header_id == header_id,
                    LogMetadataAttr.attr_key == key,
                    LogMetadataAttr.attr_value == value,
                )
            )
        ).scalars().first()
        if row:
            return row
        row = LogMetadataAttr(header_id=header_id, attr_key=key, attr_value=value)
        session.add(row)
        return row

    def bulk_upsert_attrs(
        self, session: Session, *, header_id: int, kv: Dict[str, str]
    ) -> int:
        count = 0
        for k, v in kv.items():
            self.upsert_attr(session, header_id=header_id, key=str(k), value=str(v))
            count += 1
        return count

    # --- query by key/value ----
    def search_by_kv(
        self, session: Session, *, key: str, value: str, limit: int = 100
    ) -> List[Tuple[str, int]]:
        """
        Returns list of (log_uid, raw_data_id) that have attr key=value.
        """
        q = (
            session.query(LogMetadataHeader.log_uid, LogMetadataHeader.raw_data_id)
            .join(LogMetadataAttr, LogMetadataAttr.header_id == LogMetadataHeader.id)
            .filter(LogMetadataAttr.attr_key == key, LogMetadataAttr.attr_value == value)
            .limit(limit)
        )
        return [(r[0], r[1]) for r in q.all()]


def get_log_metadata_service() -> LogMetadataService:
    return LogMetadataService()
