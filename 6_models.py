class LogMetadataHeader(Context().db_base, Base):
    """
    One row per ECM log file (parallel to PTAG). Links to RawData.id.
    Keeps the original JSON for traceability (optional).
    """
    __tablename__ = "log_metadata_header"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, nullable=False, unique=True)
    # link to existing raw log
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"), index=True, nullable=False)
    # stable external UID for convenience (ECM filename, GUID, etc.)
    log_uid = Column(String(255), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=True)

    # keep original metadata blob (optional)
    source_json = Column(JSON, nullable=True)

    # relationship (no backref on RawData to avoid side-effects)
    attrs = relationship("LogMetadataAttr", back_populates="header", lazy="dynamic", cascade="all,delete")

    __table_args__ = (
        UniqueConstraint("raw_data_id", name="_log_metadata_header_raw_data_uc"),
    )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class LogMetadataAttr(Context().db_base, Base):
    """
    Key–value attributes for a log (flattened).
    Enforces de-dup per header/key/value.
    """
    __tablename__ = "log_metadata_attr"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, nullable=False, unique=True)
    header_id = Column(Integer, ForeignKey("log_metadata_header.id", ondelete="CASCADE"), index=True, nullable=False)

    attr_key = Column(String(255), index=True, nullable=False)
    attr_value = Column(String(1024), index=True, nullable=False)

    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=True)

    header = relationship("LogMetadataHeader", back_populates="attrs")

    __table_args__ = (
        UniqueConstraint("header_id", "attr_key", "attr_value", name="_log_metadata_attr_uc"),
    )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
