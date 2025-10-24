#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

from typing import Dict, Tuple, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import select

from db.context import Context
from db.models import LogMetadataHeader, LogMetadataAttr


def upsert_header_with_attrs(
    raw_data_id: int,
    log_uid: str,
    attrs: Optional[Dict[str, str]] = None,
    source_json: Optional[dict] = None,
    session: Optional[Session] = None,
) -> Tuple[LogMetadataHeader, int]:
    """
    Create/update a header for a given raw_data_id and insert any missing key/value pairs.
    Returns (header, number_of_new_attr_rows).
    """
    session = session or Context().get_session()

    # 1) get or create header
    header = (
        session.query(LogMetadataHeader)
        .filter(LogMetadataHeader.raw_data_id == raw_data_id)
        .one_or_none()
    )

    if header is None:
        header = LogMetadataHeader(
            raw_data_id=raw_data_id,
            log_uid=log_uid,
            source_json=source_json,
        )
        session.add(header)
        session.flush()  # assigns header.id

    # 2) de-dup existing (key, value) for this header
    existing = set(
        session.query(LogMetadataAttr.attr_key, LogMetadataAttr.attr_value)
        .filter(LogMetadataAttr.header_id == header.id)
        .all()
    )

    added = 0
    if attrs:
        for k, v in attrs.items():
            tup = (k, v)
            if tup in existing:
                continue
            session.add(
                LogMetadataAttr(
                    header_id=header.id,
                    attr_key=k,
                    attr_value=v,
                )
            )
            added += 1

    return header, added


def search_by_kv(
    session: Session,
    key: str,
    value: str,
    limit: int = 100,
) -> List[Tuple[str, int]]:
    """
    Return up to `limit` rows of (log_uid, raw_data_id) where attr_key==key and attr_value==value.
    """
    q = (
        session.query(LogMetadataHeader.log_uid, LogMetadataHeader.raw_data_id)
        .join(LogMetadataAttr, LogMetadataAttr.header_id == LogMetadataHeader.id)
        .filter(LogMetadataAttr.attr_key == key, LogMetadataAttr.attr_value == value)
        .limit(limit)
    )
    return q.all()


def get_or_create_header(
    session: Session,
    raw_data_id: int,
    log_uid: str,
    source_json: Optional[dict] = None,
) -> LogMetadataHeader:
    """
    Helper if you just need the header object.
    """
    header = (
        session.query(LogMetadataHeader)
        .filter(LogMetadataHeader.raw_data_id == raw_data_id)
        .one_or_none()
    )
    if header is None:
        header = LogMetadataHeader(
            raw_data_id=raw_data_id,
            log_uid=log_uid,
            source_json=source_json,
        )
        session.add(header)
        session.flush()
    return header
