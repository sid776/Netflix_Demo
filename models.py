#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

from __future__ import annotations

import enum
from typing import Any, Dict, List,  Optional
from uuid import uuid4

from sqlalchemy import (JSON, VARCHAR, BigInteger, Boolean, Column, DateTime,
                        Enum, Float, Integer, Text)
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql.schema import ForeignKey, UniqueConstraint
from sqlalchemy.sql.sqltypes import String

from db.context import Context
from db.mixin import Base

from utilities.datetime_format import utcnow


class CatAnnotationSupplierEnum(enum.Enum):
    Unset = "Unset"
    LTTS = "LTTS"
    Scale = "Scale"
    TataElxsi = "TataElxsi"
    UAI = "UAI"


class CollectionTypeEnum(enum.Enum):
    Real = 1
    Sim = 2


class EpochEnum(enum.Enum):
    GPSEpoch = 1
    UnixEpoch = 2
    LogStart = 3


# TODO: go through all the log type strings and drop the columns to replace with enum vals
class RawLogTypeEnum(enum.Enum):
    unknown_log_type = 1
    hlog_amendment = 2
    hlog_autosnap = 3
    hlog_complete_log = 4
    hlog_snapshot = 5
    hlog_snippet = 6
    rosbag = 7
    rosbag_snippet = 8
    zip = 9


class MetricType(enum.Enum):
    int_64 = "int_64"
    double = "double"
    string = "string"


class PointCloudDataEnum(enum.Enum):
    unknown = 1
    lidar = 2
    ouster = 3
    radar = 4

class RawData(Context().db_base, Base):
    """ "This is the raw data(Log), includes rosbag, hlogs and others"""

    __tablename__ = "raw_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path = Column(Text(4294000000))
    converted_file_path = Column(Text(4294000000))
    frames = Column(Integer)
    ptag_id = Column(Integer, ForeignKey("ptag.id"))
    base_log = Column(String(200))
    raw_data_type = Column(Enum(RawLogTypeEnum), nullable=True)
    tag_json = Column(JSON)
    ptag = relationship("PTag", back_populates="raw_datas")
    extracted_datas = relationship(
        "ExtractedData", back_populates="raw_data", lazy="dynamic"
    )
    raw_data_staging_id = relationship(
        "RawDataStaging", back_populates="raw_data", lazy="dynamic"
    )
    analytics_reports = relationship(
        "AnalyticsReports", back_populates="raw_data", lazy="dynamic"
    )
    integrity_check_report = relationship(
        "IntegrityCheckReport", back_populates="raw_data", lazy="dynamic"
    )
    dataset_raw_datas = relationship(
        "DatasetAssociation", backref="raw_data", lazy="dynamic", cascade="all,delete"
    )
    event_raw_datas = relationship(
        "RawDataEvent", backref="raw_data", lazy="dynamic", cascade="all,delete"
    )
    tag_raw_datas = relationship(
        "RawDataTag", backref="raw_data", lazy="dynamic", cascade="all,delete"
    )
    scale_tasks = relationship("ScaleTask", back_populates="raw_data", lazy="dynamic")
    supplier_tasks = relationship(
        "SupplierTask", back_populates="raw_data", lazy="dynamic"
    )
    compute_runs = relationship("ComputeRun", back_populates="raw_data", lazy="dynamic")
    hlogtag = relationship("HlogTag", back_populates="raw_data", lazy="dynamic")
    raw_data_allocations_task = relationship(
        "RawDataAllocationTask",
        backref="raw_data",
        lazy="dynamic",
        cascade="all,delete",
    )

    def __init__(
        self,
        file_path,
        converted_file_path,
        frames,
        ptag_id,
        base_log: str = "",
        raw_data_type: RawLogTypeEnum = RawLogTypeEnum.unknown_log_type,
        tag_json: JSON = None,
    ):
        self.file_path = file_path
        self.converted_file_path = converted_file_path
        self.frames = frames
        if self.frames == "":
            self.frames = 0
        self.ptag_id = ptag_id
        self.base_log = base_log
        self.raw_data_type = raw_data_type
        self.tag_json = tag_json


class RawDataAllocationTask(Context().db_base, Base):
    """ "It builds the connection between the task data and raw data items."""

    __tablename__ = "raw_data_allocation_task"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    supplier_task_id = Column(
        Integer, ForeignKey("supplier_task.id"), index=True, nullable=True
    )
    raw_data_id = Column(
        Integer, ForeignKey("raw_data.id"), index=True, nullable=False
    )

    def __init__(
        self,
        supplier_task_id,
        raw_data_id,
    ):
        self.supplier_task_id = supplier_task_id
        self.raw_data_id = raw_data_id


class RawDataStaging(Context().db_base, Base):
    """ "This is the raw data(Log), in staging instead of unpacked" """

    __tablename__ = "raw_data_staging"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path = Column(Text(4294000000))
    ptag_id = Column(Integer, ForeignKey("ptag.id"), nullable=True)
    raw_data_type = Column(Enum(RawLogTypeEnum), nullable=True)
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"), nullable=True)
    ptag = relationship("PTag", back_populates="raw_data_staging")
    raw_data = relationship("RawData", back_populates="raw_data_staging_id")
    unpacked = Column(Boolean, default=False)
    in_archived = Column(Boolean, default=False)

    def __init__(
        self,
        file_path: str,
        ptag_id: int,
        raw_data_id: int,
        raw_data_type: RawLogTypeEnum = RawLogTypeEnum.unknown_log_type,
        unpacked: bool = False,
        in_archived: bool = False,
    ):
        self.file_path = file_path
        self.ptag_id = ptag_id
        self.raw_data_id = raw_data_id
        self.raw_data_type = raw_data_type
        self.unpacked = unpacked
        self.in_archived = in_archived


class CalData(Context().db_base, Base):
    """ "This is the cal data which includes id, file_path, method of caLibration and ptag_id" """

    __tablename__ = "cal_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path = Column(Text(4294000000))
    method = Column(String(200))
    ptag_id = Column(Integer, ForeignKey("ptag.id"))
    cal_base_log = Column(String(200))
    tag_json = Column(JSON)
    ptag = relationship("PTag", back_populates="cal_datas")
    extracted_datas = relationship(
        "ExtractedData", back_populates="cal_data", lazy="dynamic"
    )

    def __init__(
        self,
        file_path,
        method,
        ptag_id,
        cal_base_log: str = "",
        tag_json: JSON = None,
    ):
        self.file_path = file_path
        self.method = method
        self.ptag_id = ptag_id
        self.cal_base_log = cal_base_log
        self.tag_json = tag_json


class ZipData(Context().db_base, Base):
    """ "This is the zip data which contains hlog and related index data in a compressed format" """

    __tablename__ = "zip_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path = Column(Text(4294000000))
    unzipped_file_location = Column(Text(4294000000))
    ptag_id = Column(Integer, ForeignKey("ptag.id"), nullable=True)
    ptag = relationship("PTag", back_populates="zip_datas")
    unzip_status = Column(Boolean, default=False)
    in_archive = Column(Boolean, default=False)

    def __init__(
        self,
        file_path,
        unzipped_file_location,
        ptag_id,
        unzip_status=False,
        in_archive=False,
    ):
        self.file_path = file_path
        self.unzipped_file_location = unzipped_file_location
        self.ptag_id = ptag_id
        self.unzip_status = unzip_status
        self.in_archive = in_archive


class AssociateCfh1AData(Context().db_base, Base):
    """
    This table associates CFH and 1A files with a merged data entry.
    """

    __tablename__ = "associate_cfh_1a_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path_cfh = Column(Text(4294000000))
    file_path_1a = Column(Text(4294000000))
    merge_status = Column(Boolean, default=False)
    link_cfh_1a_data = relationship("MergedData", back_populates="associate_cfh_1a_data", lazy="dynamic")

    def __init__(
        self,
        file_path_cfh,
        file_path_1a,
        merge_status=False,
    ):
        self.file_path_cfh = file_path_cfh
        self.file_path_1a = file_path_1a
        self.merge_status = merge_status


class MergedData(Context().db_base, Base):
    """
    This table stores merged data files and links to associated CFH/1A data.
    """

    __tablename__ = "merged_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    merged_file_path = Column(Text(4294000000))
    ptag_id = Column(Integer, ForeignKey("ptag.id"), nullable=True)
    ptag = relationship("PTag", back_populates="merged_datas")
    associate_cfh_1a_data_id = Column(Integer, ForeignKey("associate_cfh_1a_data.id"), nullable=True)
    associate_cfh_1a_data = relationship("AssociateCfh1AData", back_populates="link_cfh_1a_data")

    def __init__(
        self,
        merged_file_path,
        ptag_id=None,
        associate_cfh_1a_data_id=None,
    ):
        self.merged_file_path = merged_file_path
        self.ptag_id = ptag_id
        self.associate_cfh_1a_data_id = associate_cfh_1a_data_id


class ScaleTaskParam(Context().db_base, Base):
    """ "It stores supplier params for annotation tasks"""

    __tablename__ = "scale_task_param"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    supplier = Column(
        Enum(CatAnnotationSupplierEnum), nullable=False, server_default=("Unset")
    )
    geometries = Column(Text(4294000000))
    annotation_attributes = Column(Text(4294000000))
    frame_rate = Column(Integer)
    start_time = Column(Integer)
    with_labels = Column(Boolean)
    md5 = Column(String(200))
    content = Column(Text(4294000000))
    scale_tasks = relationship(
        "ScaleTask", back_populates="scale_task_param", lazy="dynamic"
    )

    def __init__(
        self,
        geometries: str,
        annotation_attributes: str,
        frame_rate: int,
        start_time: int,
        with_labels: bool,
        md5: str,
        content: str,
        supplier: CatAnnotationSupplierEnum = CatAnnotationSupplierEnum.Unset,
    ):
        self.supplier = supplier
        self.geometries = geometries
        self.annotation_attributes = annotation_attributes
        self.frame_rate = frame_rate
        self.start_time = start_time
        self.with_labels = with_labels
        self.md5 = md5
        self.content = content


class SupplierTaskParam(Context().db_base, Base):
    """ "It stores supplier params for annotation tasks"""

    __tablename__ = "supplier_task_param"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    supplier = Column(
        Enum(CatAnnotationSupplierEnum), nullable=False, server_default=("Unset")
    )
    geometries = Column(Text(4294000000))
    annotation_attributes = Column(Text(4294000000))
    frame_rate = Column(Integer, nullable=True)
    start_time = Column(Integer, nullable=True)
    with_labels = Column(Boolean, nullable=True)
    md5 = Column(String(200))
    content = Column(Text(4294000000))

    supplier_tasks = relationship(
        "SupplierTask", back_populates="supplier_task_param", lazy="dynamic"
    )
    __table_args__ = {"mysql_charset": "latin1"}

    def __init__(
        self,
        geometries: str,
        annotation_attributes: str,
        frame_rate: int,
        start_time: int,
        with_labels: bool,
        md5: str,
        content: str,
        supplier: CatAnnotationSupplierEnum = CatAnnotationSupplierEnum.Unset,
    ):
        self.geometries = geometries
        self.annotation_attributes = annotation_attributes
        self.frame_rate = frame_rate
        self.start_time = start_time
        self.with_labels = with_labels
        self.md5 = md5
        self.content = content
        self.supplier = supplier


class ScaleTask(Context().db_base, Base):
    """ "It stores supplier task information"""

    __tablename__ = "scale_task"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    task_id = Column(String(5000))
    task_type = Column(String(255))
    task_status = Column(String(255))
    project = Column(String(255))
    batch = Column(String(255))
    unique_id = Column(String(255))
    tags = Column(String(5000))
    data = Column(Text(4294000000))
    is_test = Column(Boolean)
    ingested = Column(Boolean)
    frame_count = Column(Integer)

    task_created_date = Column(DateTime, default=utcnow, nullable=True)
    task_completed_date = Column(DateTime, nullable=True)
    annotations = relationship(
        "Annotation", back_populates="scale_task", lazy="dynamic"
    )
    scale_task_param_id = Column(Integer, ForeignKey("scale_task_param.id"))
    scale_task_param = relationship("ScaleTaskParam", back_populates="scale_tasks")
    version_num = Column(Integer)
    md5_sum = Column(String(200))
    raw_annotations_file_path = Column(String(2048), nullable=True)
    events_file_path = Column(String(2048), nullable=True)
    callback_file_path = Column(String(2048), nullable=True)
    link_file_path = Column(String(2048), nullable=True)
    cat_coco_annotations_file_path = Column(String(2048), nullable=True)
    raw_data_id = Column(
        Integer, ForeignKey("raw_data.id", ondelete="SET NULL"), nullable=True
    )
    raw_data = relationship(
        "RawData", back_populates="scale_tasks", passive_deletes=True
    )
    taxonomy_allocations_task = relationship(
        "TaxonomyAllocationTask",
        backref="scale_task",
        lazy="dynamic",
        cascade="all,delete",
    )
    extracted_data_allocations_task = relationship(
        "ExtractedDataAllocationTask",
        backref="scale_task",
        lazy="dynamic",
        cascade="all,delete",
    )

    def __init__(
        self,
        task_id: str,
        task_type: str,
        task_status: str,
        project: str,
        batch: str,
        unique_id: str,
        tags: str,
        data: str,
        is_test: bool,
        ingested: bool,
        frame_count: int,
        scale_task_param_id: int,
        task_created_date=None,
        task_completed_date=None,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.task_status = task_status
        self.project = project
        self.batch = batch
        self.unique_id = unique_id
        self.tags = tags
        self.data = data
        self.is_test = is_test
        self.ingested = ingested
        self.frame_count = frame_count
        self.scale_task_param_id = scale_task_param_id
        self.task_created_date = task_created_date
        self.task_completed_date = task_completed_date


class SupplierTask(Context().db_base, Base):
    """ "It stores supplier task information"""

    __tablename__ = "supplier_task"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    task_id = Column(String(5000))
    task_type = Column(String(255))
    task_status = Column(String(255))
    project = Column(String(255))
    batch = Column(String(255))
    unique_id = Column(String(255))
    tags = Column(String(5000))
    data = Column(Text(4294000000))
    is_test = Column(Boolean)
    ingested = Column(Boolean)
    frame_count = Column(Integer)

    task_created_date = Column(DateTime, default=utcnow, nullable=True)
    task_completed_date = Column(DateTime, nullable=True)
    annotations = relationship(
        "Annotation", back_populates="supplier_task", lazy="dynamic"
    )
    supplier_task_param_id = Column(
        Integer, ForeignKey("supplier_task_param.id"), nullable=True
    )
    supplier_task_param = relationship(
        "SupplierTaskParam", back_populates="supplier_tasks"
    )
    version_num = Column(Integer)
    md5_sum = Column(String(200))

    raw_annotations_file_path = Column(String(2048), nullable=True)
    events_file_path = Column(String(2048), nullable=True)
    callback_file_path = Column(String(2048), nullable=True)
    link_file_path = Column(String(2048), nullable=True)
    cat_coco_annotations_file_path = Column(String(2048), nullable=True)

    raw_annotation_validation = Column(String(2048), nullable=True)
    events_validation = Column(String(2048), nullable=True)
    callback_validation = Column(String(2048), nullable=True)
    link_validation = Column(String(2048), nullable=True)

    raw_data_id = Column(
        Integer, ForeignKey("raw_data.id", ondelete="SET NULL"), nullable=True
    )
    task_last_updated = Column(DateTime, nullable=True)

    raw_data = relationship(
        "RawData", back_populates="supplier_tasks", passive_deletes=True
    )
    taxonomy_allocations_task = relationship(
        "TaxonomyAllocationTask",
        backref="supplier_task",
        lazy="dynamic",
        cascade="all,delete",
    )
    extracted_data_allocations_task = relationship(
        "ExtractedDataAllocationTask",
        backref="supplier_task",
        lazy="dynamic",
        cascade="all,delete",
    )
    raw_data_allocations_task = relationship(
        "RawDataAllocationTask",
        backref="supplier_task",
        lazy="dynamic",
        cascade="all,delete",
    )

    __table_args__ = {"mysql_charset": "latin1"}

    def __init__(
        self,
        task_id: str,
        task_type: str,
        task_status: str,
        project: str,
        batch: str,
        unique_id: str,
        tags: str,
        data: str,
        is_test: bool,
        ingested: bool,
        frame_count: int,
        supplier_task_param_id: int,
        task_created_date=None,
        task_completed_date=None,
        task_last_updated=None,
        md5_sum=None,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.task_status = task_status
        self.project = project
        self.batch = batch
        self.unique_id = unique_id
        self.tags = tags
        self.data = data
        self.is_test = is_test
        self.ingested = ingested
        self.frame_count = frame_count
        self.supplier_task_param_id = supplier_task_param_id
        self.task_created_date = task_created_date
        self.task_completed_date = task_completed_date
        self.task_last_updated = task_last_updated
        self.md5_sum = md5_sum


class SceneFrame(Context().db_base, Base):
    """store information about scene frames"""

    __tablename__ = "scene_frame"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    ## extracted data here should be the scene frame
    extracted_data_id = Column(Integer, ForeignKey("extracted_data.id"))
    related_extracted_data_id = Column(Integer, ForeignKey("extracted_data.id"))

    extracted_datas = relationship("ExtractedData", foreign_keys=[extracted_data_id])
    related_extracted_datas = relationship(
        "ExtractedData", foreign_keys=[related_extracted_data_id]
    )

    def __init__(
        self,
        extracted_data_id,
        related_extracted_data_id,
    ):
        self.extracted_data_id = extracted_data_id
        self.related_extracted_data_id = related_extracted_data_id


class ExtractedData(Context().db_base, Base):
    """ "It stores individual files that are used for annotation such as jpg and png"""

    __tablename__ = "extracted_data"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    file_path = Column(Text(4294000000))
    s3_loc = Column(Text(4294000000))
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"))
    cal_data_id = Column(Integer, ForeignKey("cal_data.id"))
    ptag_id = Column(Integer, ForeignKey("ptag.id"))
    is_test = Column(Boolean)
    is_in_process = Column(Boolean)

    raw_data = relationship("RawData", back_populates="extracted_datas")
    cal_data = relationship("CalData", back_populates="extracted_datas")
    ptag = relationship("PTag", back_populates="extracted_datas")
    annotations = relationship(
        "Annotation", back_populates="extracted_data", lazy="dynamic"
    )
    extracted_data_allocations_task = relationship(
        "ExtractedDataAllocationTask",
        backref="extracted_data",
        lazy="dynamic",
        cascade="all,delete",
    )

    def __init__(
        self,
        file_path,
        s3_loc,
        raw_data_id,
        cal_data_id=None,
        ptag_id=None,
        is_test=False,
        is_in_process=False,
    ):
        self.file_path = file_path
        self.s3_loc = s3_loc
        self.raw_data_id = raw_data_id
        self.cal_data_id = cal_data_id
        self.ptag_id = ptag_id
        self.is_test = is_test
        self.is_in_process = is_in_process

    def get_s3_loc(self):
        return self.s3_loc


class ExtractedDataAllocationTask(Context().db_base, Base):
    """ "It builds the connection between the task data and extracted data items."""

    __tablename__ = "extracted_data_allocation_task"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    scale_task_id = Column(
        Integer, ForeignKey("scale_task.id"), index=True, nullable=True
    )
    supplier_task_id = Column(
        Integer, ForeignKey("supplier_task.id"), index=True, nullable=True
    )
    extracted_data_id = Column(
        Integer, ForeignKey("extracted_data.id"), index=True, nullable=False
    )

    def __init__(
        self,
        scale_task_id,
        supplier_task_id,
        extracted_data_id,
    ):
        self.scale_task_id = scale_task_id
        self.extracted_data_id = extracted_data_id
        self.supplier_task_id = supplier_task_id


class Dataset(Context().db_base, Base):
    """table for tracking and organizing datasets"""

    __tablename__ = "dataset"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    name = Column(VARCHAR(256), nullable=False, unique=True)
    description = Column(Text(256))
    datasets = relationship(
        "DatasetAssociation", back_populates="dataset", lazy="dynamic"
    )
    compute_runs = relationship("ComputeRun", back_populates="dataset", lazy="dynamic")

    def __init__(
        self,
        name,
        description,
    ):
        self.name = name
        self.description = description

    def as_dict(self, output_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Override the default as_dict method to rename the ID column.

        Args:
            output_columns: List[str] | None;
                List of columns/fields to serialize
                Default: None

        Returns:
            Dict[str, Any]: The Dataset object serialized according to the requested output columns
        """
        if output_columns is None:
            output_columns = []

        result = super().as_dict(output_columns)
        if "id" in result:
            result["dataset_id"] = result.pop("id")

        try:
            result["dataset_id"] = int(result["dataset_id"])
        except ValueError:
            pass

        return result


class DatasetAssociation(Context().db_base, Base):
    """table for tracking and organizing datasets"""

    __tablename__ = "dataset_association"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    dataset_id = Column(Integer, ForeignKey("dataset.id"), index=True)
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"), index=True)
    dataset = relationship("Dataset", back_populates="datasets")
    __table_args__ = (
        UniqueConstraint("dataset_id", "raw_data_id", name="_dataset_association_uc"),
    )

    def __init__(
        self,
        dataset_id,
        raw_data_id,
    ):
        self.dataset_id = dataset_id
        self.raw_data_id = raw_data_id


class Tag(Context().db_base, Base):
    """It stores tag information"""

    __tablename__ = "tag"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    name = Column(VARCHAR(256), nullable=False)
    value = Column(VARCHAR(256))

    raw_data_tags = relationship(
        "RawDataTag", backref="tag", lazy="dynamic", cascade="all,delete"
    )
    raw_data_events = relationship(
        "RawDataEvent", backref="tag", lazy="dynamic", cascade="all,delete"
    )

    def __init__(
        self,
        name,
        value="",
    ):
        self.name = name
        self.value = value


class RawDataEvent(Context().db_base, Base):
    """It stores event information for raw data logs"""

    __tablename__ = "raw_data_event"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    tag_id = Column(Integer, ForeignKey("tag.id"), index=True)
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"), index=True)
    log_start_time = Column(Integer, nullable=False)
    log_end_time = Column(Integer, nullable=False)
    epoch_type = Column(Enum(EpochEnum), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "tag_id",
            "raw_data_id",
            "log_start_time",
            "log_end_time",
            name="_raw_data_event_uc",
        ),
    )

    def __init__(self, tag_id, raw_data_id, log_start_time, log_end_time, epoch_type):
        self.tag_id = tag_id
        self.raw_data_id = raw_data_id
        self.log_start_time = log_start_time
        self.log_end_time = log_end_time
        self.epoch_type = epoch_type


class RawDataTag(Context().db_base, Base):
    """It stores tag information for raw data logs"""

    __tablename__ = "raw_data_tag"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    tag_id = Column(Integer, ForeignKey("tag.id"), index=True)
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"), index=True)

    __table_args__ = (
        UniqueConstraint("tag_id", "raw_data_id", name="_raw_data_tag_uc"),
    )

    def __init__(
        self,
        tag_id,
        raw_data_id,
    ):
        self.tag_id = tag_id
        self.raw_data_id = raw_data_id


class Annotation(Context().db_base, Base):
    """ "It stores annotation data for each extracted data"""

    __tablename__ = "annotation"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    version_num = Column(Integer)
    md5_sum = Column(String(200))
    annotation_s3_loc = Column(Text(4294000000))
    extracted_data_id = Column(Integer, ForeignKey("extracted_data.id"))
    extracted_data = relationship("ExtractedData", back_populates="annotations")
    scale_task_id = Column(Integer, ForeignKey("scale_task.id"))
    scale_task = relationship("ScaleTask", back_populates="annotations")
    taxonomy_allocations = relationship(
        "TaxonomyAllocation", back_populates="annotation", lazy="dynamic"
    )
    scale_qa_done = Column(Boolean)
    supplier_task_id = Column(Integer, ForeignKey("supplier_task.id"))
    supplier_task = relationship("SupplierTask", back_populates="annotations")
    supplier_qa_done = Column(Boolean)
    customer_review_status = Column(Boolean)
    customer_review_comments = Column(Text(4294000000))

    def __init__(
        self,
        annotation_s3_loc: str,
        extracted_data_id: int,
        supplier_task_id: int,
        scale_qa_done: bool,
        customer_review_status: bool,
        customer_review_comments: str,
    ):
        self.annotation_s3_loc = annotation_s3_loc
        self.extracted_data_id = extracted_data_id
        self.supplier_task_id = supplier_task_id
        self.scale_qa_done = scale_qa_done
        self.customer_review_status = customer_review_status
        self.customer_review_comments = customer_review_comments


class TaxonomyAllocation(Context().db_base, Base):
    """ "It builds the connection between the extracted data and annotated taxonomy items. It is used for search purpose"""

    __tablename__ = "taxonomy_allocation"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    annotation_id = Column(Integer, ForeignKey("annotation.id"))
    annotation = relationship("Annotation", back_populates="taxonomy_allocations")
    taxonomy_id = Column(Integer, ForeignKey("taxonomy.id"))
    taxonomy = relationship("Taxonomy", back_populates="taxonomy_allocations")

    def __init__(self, annotation_id: int, taxonomy_id: int):
        self.annotation_id = annotation_id
        self.taxonomy_id = taxonomy_id


class TaxonomyAllocationTask(Context().db_base, Base):
    """ "It builds the connection between the scale task data and annotated taxonomy items."""

    __tablename__ = "taxonomy_allocation_task"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    scale_task_id = Column(
        Integer, ForeignKey("scale_task.id"), index=True, nullable=True
    )
    supplier_task_id = Column(
        Integer, ForeignKey("supplier_task.id"), index=True, nullable=True
    )
    taxonomy_id = Column(Integer, ForeignKey("taxonomy.id"), index=True, nullable=False)

    def __init__(self, scale_task_id: int, supplier_task_id: int, taxonomy_id: int):
        self.scale_task_id = scale_task_id
        self.supplier_task_id = supplier_task_id
        self.taxonomy_id = taxonomy_id


class Taxonomy(Context().db_base, Base):
    """ "It stores individual taxonomy items"""

    __tablename__ = "taxonomy"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    name = Column(String(255), nullable=True)
    parent_id = Column(Integer, ForeignKey("taxonomy.id"))
    children = relationship("Taxonomy", backref=backref("parent", remote_side=[id]))
    validation_pattern = Column(String(255), nullable=True)
    taxonomy_allocations = relationship(
        "TaxonomyAllocation", back_populates="taxonomy", lazy="dynamic"
    )
    taxonomy_allocations_task = relationship(
        "TaxonomyAllocationTask",
        backref="taxonomy",
        lazy="dynamic",
        cascade="all,delete",
    )

    def __init__(self, name, parent_id):
        self.name = name
        self.parent_id = parent_id


class PTag(Context().db_base, Base):
    """This is the ptag data that includes metadata of collections"""

    __tablename__ = "ptag"
    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String(5000), nullable=True)
    md5 = Column(String(200))
    data_collection_name = Column(String(200))
    version = Column(String(200), nullable=True)
    collection_date = Column(DateTime, nullable=True)
    collection_type = Column(Enum(CollectionTypeEnum), nullable=True)
    country = Column(String(200))
    state = Column(String(200))
    city = Column(String(200))
    project = Column(String(200))
    team = Column(String(200))
    customer = Column(String(200))
    site = Column(String(200))
    product_group = Column(String(200))
    machine_model = Column(String(200))
    application_type = Column(String(200))
    serial_number = Column(String(200))
    additional_info = Column(JSON)
    content = Column(Text)
    raw_datas = relationship("RawData", back_populates="ptag", lazy="dynamic")
    cal_datas = relationship("CalData", back_populates="ptag", lazy="dynamic")
    raw_data_staging = relationship(
        "RawDataStaging", back_populates="ptag", lazy="dynamic"
    )
    modalities = relationship("Modality", back_populates="ptag", lazy="dynamic")
    extracted_datas = relationship(
        "ExtractedData", back_populates="ptag", lazy="dynamic"
    )
    zip_datas = relationship("ZipData", back_populates="ptag", lazy="dynamic")
    merged_datas = relationship("MergedData", back_populates="ptag", lazy="dynamic")

    def __init__(
        self,
        path,
        version,
        collection_date,
        collection_type,
        md5,
        data_collection_name,
        country,
        state,
        city,
        project,
        team,
        customer,
        product_group,
        machine_model,
        application_type,
        serial_number,
        site,
        additional_info,
        content,
    ):
        self.path = path
        self.version = version
        self.collection_date = collection_date
        self.collection_type = collection_type
        self.md5 = md5
        self.data_collection_name = data_collection_name
        self.country = country
        self.state = state
        self.city = city
        self.project = project
        self.team = team
        self.customer = customer
        self.product_group = product_group
        self.machine_model = machine_model
        self.application_type = application_type
        self.serial_number = serial_number
        self.site = site
        self.additional_info = additional_info
        self.content = content


class Modality(Context().db_base, Base):
    """Currently this table is not in used, it is designed to allow searching annotation by modality"""

    __tablename__ = "modality"
    id = Column(Integer, primary_key=True, autoincrement=True)
    modality_type = Column(String(200))
    sensor_type = Column(String(200))
    loc1 = Column(String(200))
    loc2 = Column(String(200))
    loc3 = Column(String(200))
    firmware_version = Column(String(200))
    reference_point = Column(String(200))
    roll = Column(Integer)
    pitch = Column(Integer)
    yaw = Column(Integer)
    pos_horizontal_field_of_view = Column(Integer)
    neg_horizontal_field_of_view = Column(Integer)
    pos_vertical_field_of_view = Column(Integer)
    neg_vertical_field_of_view = Column(Integer)
    lever_arm_x_wrt_ref_point = Column(Integer)
    lever_arm_y_wrt_ref_point = Column(Integer)
    lever_arm_z_wrt_ref_point = Column(Integer)
    data_rate = Column(Integer)
    roll_pitch_yaw_reference = Column(String(200))
    sensor_nickname = Column(String(200))
    output_channels = Column(Text)
    ptag_id = Column(Integer, ForeignKey("ptag.id"))
    ptag = relationship("PTag", back_populates="modalities")

    def __init__(
        self,
        modality_type,
        sensor_type,
        loc1,
        loc2,
        loc3,
        firmware_version,
        reference_point,
        roll,
        pitch,
        yaw,
        pos_horizontal_field_of_view,
        neg_horizontal_field_of_view,
        pos_vertical_field_of_view,
        neg_vertical_field_of_view,
        lever_arm_x_wrt_ref_point,
        lever_arm_y_wrt_ref_point,
        lever_arm_z_wrt_ref_point,
        data_rate,
        roll_pitch_yaw_reference,
        sensor_nickname,
        output_channels,
        ptag_id,
    ):
        self.modality_type = modality_type
        self.sensor_type = sensor_type
        self.loc1 = loc1
        self.loc2 = loc2
        self.loc3 = loc3
        self.firmware_version = firmware_version
        self.reference_point = reference_point
        self.roll = roll
        self.pitch = pitch
        self.yaw = yaw
        self.pos_horizontal_field_of_view = pos_horizontal_field_of_view
        self.neg_horizontal_field_of_view = neg_horizontal_field_of_view
        self.pos_vertical_field_of_view = pos_vertical_field_of_view
        self.neg_vertical_field_of_view = neg_vertical_field_of_view
        self.lever_arm_x_wrt_ref_point = lever_arm_x_wrt_ref_point
        self.lever_arm_y_wrt_ref_point = lever_arm_y_wrt_ref_point
        self.lever_arm_z_wrt_ref_point = lever_arm_z_wrt_ref_point
        self.data_rate = data_rate
        self.roll_pitch_yaw_reference = roll_pitch_yaw_reference
        self.sensor_nickname = sensor_nickname
        self.output_channels = output_channels
        self.ptag_id = ptag_id


class TaxonomyArchive(Context().db_base, Base):
    """ "It stores the archived taxonomy to support historical querying"""

    __tablename__ = "taxonomy_archive"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    added_date = Column(String(255), nullable=True)
    taxonomy_name = Column(String(255), nullable=True)
    taxonomy_full_name = Column(String(255), nullable=True)
    taxonomy_id = Column(Integer, ForeignKey("taxonomy.id"))
    created_date = Column(String(255), nullable=True)

    def __init__(self, taxonomy_name, taxonomy_full_name, taxonomy_id, created_date):
        self.taxonomy_name = taxonomy_name
        self.taxonomy_full_name = taxonomy_full_name
        self.taxonomy_id = taxonomy_id
        self.created_date = created_date


class TaskParam(Context().db_base, Base):
    """ "It stores task params for annotations tasks"""

    __tablename__ = "task_param"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    generic_content = Column(Text(4294000000))
    ptag_team_name = Column(String(255), nullable=True)
    ptag_id = Column(Integer, ForeignKey("ptag.id"))
    do_not_use = Column(Boolean, nullable=True)

    def __init__(self, generic_content, ptag_team_name, ptag_id, do_not_use):
        self.generic_content = generic_content
        self.ptag_team_name = ptag_team_name
        self.ptag_id = ptag_id
        self.do_not_use = do_not_use


# TODO: singular table names
class AnalyticsReports(Context().db_base, Base):
    """ "It stores records of findings of integrity checks run on the data collection data"""

    __tablename__ = "analytics_reports"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    report_name = Column(Text(4294000000), nullable=True)
    md5 = Column(Text(4294000000), nullable=True)
    bag_duration = Column(Float, nullable=True)
    bag_file_size_in_kb = Column(Float, nullable=True)
    distance_travelled = Column(Float, nullable=True)
    report_details = relationship(
        "AnalyticsReportsDetails", back_populates="analyticsReports", lazy="dynamic"
    )
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"))
    raw_data = relationship("RawData", back_populates="analytics_reports")

    def __init__(
        self,
        report_name,
        md5,
        bag_duration,
        bag_file_size_in_kb,
        distance_travelled,
        raw_data_id,
    ):
        self.report_name = report_name
        self.md5 = md5
        self.bag_duration = bag_duration
        self.bag_file_size_in_kb = bag_file_size_in_kb
        self.distance_travelled = distance_travelled
        self.raw_data_id = raw_data_id


# TODO: singular table names
class AnalyticsReportsDetails(Context().db_base, Base):
    """ "It stores records of findings of integrity checks run on the data collection data"""

    __tablename__ = "analytics_reports_details"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    sensor_name = Column(
        String(255),
    )
    bag_id = Column(Integer, ForeignKey("analytics_reports.id"))
    ptag_id = Column(Integer, nullable=True)
    start_time = Column(Float, nullable=True)
    sensor_nickname = Column(String(255), nullable=True)
    actual_msgs = Column(Integer, nullable=True)
    expected_msgs = Column(Float, nullable=True)
    duration_in_seconds = Column(Integer, nullable=True)
    missing_sequence_count = Column(Integer, nullable=True)
    min_time_diff_in_nano_secs = Column(Integer, nullable=True)
    max_time_diff_in_nano_secs = Column(Integer, nullable=True)
    average_time_diff_in_nano_secs = Column(Float, nullable=True)
    expected_freq_hz = Column(Integer, nullable=True)
    actual_freq_hz = Column(Float, nullable=True)
    extracted_frame_rate = Column(Integer, nullable=True)
    analyticsReports = relationship("AnalyticsReports", back_populates="report_details")

    def __init__(
        self,
        sensor_name,
        bag_id,
        ptag_id,
        start_time,
        sensor_nickname,
        actual_msgs,
        expected_msgs,
        duration_in_seconds,
        missing_sequence_count,
        min_time_diff_in_nano_secs,
        max_time_diff_in_nano_secs,
        average_time_diff_in_nano_secs,
        expected_freq_hz,
        actual_freq_hz,
        extracted_frame_rate,
    ):
        self.sensor_name = sensor_name
        self.bag_id = bag_id
        self.ptag_id = ptag_id
        self.start_time = start_time
        self.sensor_nickname = sensor_nickname
        self.actual_msgs = actual_msgs
        self.expected_msgs = expected_msgs
        self.duration_in_seconds = duration_in_seconds
        self.missing_sequence_count = missing_sequence_count
        self.min_time_diff_in_nano_secs = min_time_diff_in_nano_secs
        self.max_time_diff_in_nano_secs = max_time_diff_in_nano_secs
        self.average_time_diff_in_nano_secs = average_time_diff_in_nano_secs
        self.expected_freq_hz = expected_freq_hz
        self.actual_freq_hz = actual_freq_hz
        self.extracted_frame_rate = extracted_frame_rate


class Experiment(Context().db_base, Base):

    __tablename__ = "experiment"

    id = Column(
        Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
        nullable=False,
    )
    name = Column(String(255))
    compute_run = relationship(
        "ComputeRun", back_populates="experiments", lazy="dynamic"
    )

    def __init__(self, name):
        self.name = name


class ComputeRun(Context().db_base, Base):
    """It stores records of findings of integrity checks run on the data collection data"""

    __tablename__ = "compute_run"

    id = Column(String(36), primary_key=True, index=True, unique=True, nullable=False)
    name = Column(String(255), nullable=True)
    time_stamp_start = Column(DateTime(timezone=True))
    duration_ns = Column(BIGINT(unsigned=True), nullable=True)
    process_exit_code = Column(Integer, nullable=True)
    user = Column(String(30))
    experiment_id = Column(
        Integer, ForeignKey("experiment.id", ondelete="SET NULL"), nullable=True
    )
    reference_url = Column(String(1024), nullable=True)
    reference = Column(String(255), nullable=True)
    experiments = relationship(
        "Experiment", back_populates="compute_run", passive_deletes=True
    )
    metrics_compute_run = relationship(
        "MetricComputeRun",
        back_populates="compute_run",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )
    raw_data_id = Column(
        Integer, ForeignKey("raw_data.id", ondelete="SET NULL"), nullable=True
    )
    raw_data = relationship(
        "RawData", back_populates="compute_runs", passive_deletes=True
    )
    dataset_id = Column(
        Integer, ForeignKey("dataset.id", ondelete="SET NULL"), nullable=True
    )
    dataset = relationship(
        "Dataset", back_populates="compute_runs", passive_deletes=True
    )

    def __init__(self, **kwargs):
        if "id" not in kwargs:
            kwargs["id"] = str(uuid4())
        self.__dict__.update(kwargs)


class MetricComputeRun(Context().db_base, Base):

    __tablename__ = "metric_compute_run"

    id = Column(
        Integer,
        primary_key=True,
        index=True,
        unique=True,
        autoincrement=True,
        nullable=False,
    )
    key = Column(String(100), index=True)
    value_string = Column(String(100), nullable=True)
    value_int = Column(Integer, nullable=True)
    value_double = Column(Float, nullable=True)
    metric_type = Column(Enum(MetricType), nullable=False)
    compute_run_id = Column(String(36), ForeignKey("compute_run.id", ondelete="CASCADE"))
    json = Column(JSON, nullable=True)
    compute_run = relationship("ComputeRun", back_populates="metrics_compute_run", passive_deletes=True)
    __table_args__ = (UniqueConstraint("compute_run_id", "key", name="_computerun_metrickey_uc"),) 
        
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class IntegrityCheckReport(Context().db_base, Base):
    """ "It stores records of findings of integrity checks run on the data collection data"""

    __tablename__ = "integrity_check_report"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    report_path = Column(Text(4294000000), nullable=True)
    md5 = Column(Text(4294000000), nullable=True)
    hlog_duration = Column(Float, nullable=True)
    distance_travelled = Column(Float, nullable=True)
    pcd_occlusions_detected = Column(Boolean, default=False)
    num_split_files_used = Column(Integer, nullable=True)
    raw_data_id = Column(Integer, ForeignKey("raw_data.id"))
    raw_data = relationship("RawData", back_populates="integrity_check_report")
    integrity_check_report_latency_issues = relationship(
        "IntegrityCheckReportLatencyIssue",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_missing_channels = relationship(
        "IntegrityCheckReportMissingChannel",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_not_calibrated_sensors = relationship(
        "IntegrityCheckReportNotCalibratedSensor",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_point_cloud_occluded_channels = relationship(
        "IntegrityCheckReportPointCloudOccludedChannel",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_sequence_out_of_order_channels = relationship(
        "IntegrityCheckReportSequenceOutOfOrderChannel",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_tf_dynamic_tf_static_mismatches = relationship(
        "IntegrityCheckReportTFDynamicTFStaticMismatch",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )
    integrity_check_report_transmission_time_delays = relationship(
        "IntegrityCheckReportTransmissionTimeDelay",
        backref="integrity_check_report",
        lazy="dynamic",
        cascade="all,delete",
    )

    def __init__(
        self,
        report_path,
        md5,
        hlog_duration,
        distance_travelled,
        pcd_occlusions_detected,
        num_split_files_used,
        raw_data_id,
    ):
        self.report_path = report_path
        self.md5 = md5
        self.hlog_duration = hlog_duration
        self.distance_travelled = distance_travelled
        self.pcd_occlusions_detected = pcd_occlusions_detected
        self.num_split_files_used = num_split_files_used
        self.raw_data_id = raw_data_id


class IntegrityCheckReportLatencyIssue(Context().db_base, Base):
    """ "It stores records of findings of latency issues found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_latency_issue"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    latency_issue_channel_name = Column(String(255), nullable=False)
    latency_issue_count = Column(Integer, nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        latency_issue_channel_name,
        latency_issue_count,
        split_file_num,
        integrity_check_report_id,
    ):
        self.latency_issue_channel_name = latency_issue_channel_name
        self.latency_issue_count = latency_issue_count
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportMissingChannel(Context().db_base, Base):
    """ "It stores records of findings of missing channels found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_missing_channel"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    missing_channel_name = Column(String(255), nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(self, missing_channel_name, split_file_num, integrity_check_report_id):
        self.missing_channel_name = missing_channel_name
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportNotCalibratedSensor(Context().db_base, Base):
    """ "It stores records of findings of sensor not calibrated issues found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_not_calibrated_sensor"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    sensor_not_calibrated_channel_name = Column(String(255), nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        sensor_not_calibrated_channel_name,
        split_file_num,
        integrity_check_report_id,
    ):
        self.sensor_not_calibrated_channel_name = sensor_not_calibrated_channel_name
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportPointCloudOccludedChannel(Context().db_base, Base):
    """ "It stores records of findings of occluded channels found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_point_cloud_occluded_channel"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    point_cloud_occluded_channel_name = Column(String(255), nullable=False)
    point_cloud_data_type = Column(Enum(PointCloudDataEnum), nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        point_cloud_occluded_channel_name,
        point_cloud_data_type,
        split_file_num,
        integrity_check_report_id,
    ):
        self.point_cloud_occluded_channel_name = point_cloud_occluded_channel_name
        self.point_cloud_data_type = PointCloudDataEnum(point_cloud_data_type)
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportSequenceOutOfOrderChannel(Context().db_base, Base):
    """ "It stores records of findings of missing channels found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_sequence_out_of_order_channel"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    sequence_out_of_order_channel_name = Column(String(255), nullable=False)
    sequence_out_of_order_count = Column(Integer, nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        sequence_out_of_order_channel_name,
        sequence_out_of_order_count,
        split_file_num,
        integrity_check_report_id,
    ):
        self.sequence_out_of_order_channel_name = sequence_out_of_order_channel_name
        self.sequence_out_of_order_count = sequence_out_of_order_count
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportTFDynamicTFStaticMismatch(Context().db_base, Base):
    """ "It stores records of findings of tf mismatch issues found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_tf_dynamic_tf_static_mismatch"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    tf_dynamic_tf_static_mismatch_channel_name = Column(String(255), nullable=False)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        tf_dynamic_tf_static_mismatch_channel_name,
        split_file_num,
        integrity_check_report_id,
    ):
        self.tf_dynamic_tf_static_mismatch_channel_name = (
            tf_dynamic_tf_static_mismatch_channel_name
        )
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class IntegrityCheckReportTransmissionTimeDelay(Context().db_base, Base):
    """ "It stores records of findings of time delay issues found in integrity check run on the data collection of hlogs"""

    __tablename__ = "integrity_check_report_transmission_time_delay"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    transmission_time_delay_channel_name = Column(String(255), nullable=False)
    transmission_time_delay_avg = Column(Float, nullable=True)
    transmission_time_delay_max = Column(Float, nullable=True)
    transmission_time_delay_min = Column(Float, nullable=True)
    transmission_timestamp_invalid_count = Column(BigInteger, nullable=True)
    split_file_num = Column(Integer, nullable=True)
    integrity_check_report_id = Column(
        Integer, ForeignKey("integrity_check_report.id"), index=True
    )

    def __init__(
        self,
        transmission_time_delay_channel_name,
        transmission_time_delay_avg,
        transmission_time_delay_max,
        transmission_time_delay_min,
        transmission_timestamp_invalid_count,
        split_file_num,
        integrity_check_report_id,
    ):
        self.transmission_time_delay_channel_name = transmission_time_delay_channel_name
        self.transmission_time_delay_avg = transmission_time_delay_avg
        self.transmission_time_delay_max = transmission_time_delay_max
        self.transmission_time_delay_min = transmission_time_delay_min
        self.transmission_timestamp_invalid_count = transmission_timestamp_invalid_count
        self.split_file_num = split_file_num
        self.integrity_check_report_id = integrity_check_report_id


class RawDataAmendment(Context().db_base, Base):
    """ "It stores records of metadata of annotation amendment to raw logs process"""

    __tablename__ = "raw_data_amendment"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )
    annotation_path = Column(String(255), nullable=False)
    log_filter = Column(String(255), nullable=False)
    amendment_artifacts_path = Column(String(255), nullable=True)
    run_status = Column(Boolean)
    raw_log_id = Column(Integer, ForeignKey("raw_data.id"))

    def __init__(
        self,
        annotation_path,
        log_filter,
        amendment_artifacts_path,
        run_status,
        raw_log_id,
    ):
        self.annotation_path = annotation_path
        self.log_filter = log_filter
        self.amendment_artifacts_path = amendment_artifacts_path
        self.run_status = run_status
        self.raw_log_id = raw_log_id


class HlogTag(Context().db_base, Base):
    """It stores tag information"""

    __tablename__ = "hlogtag"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        index=True,
        nullable=False,
        unique=True,
    )

    file_path = Column(Text(4294000000))
    raw_data_id = Column(
        Integer, ForeignKey("raw_data.id", ondelete="SET NULL"), nullable=True
    )
    raw_data = relationship("RawData", back_populates="hlogtag", passive_deletes=True)
    name = Column(VARCHAR(256), nullable=False)
    json = Column(Text(4294000000), nullable=False)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __ne__(self, other):
        if not isinstance(other, HlogTag):
            # Don't recognise "other", so let *it* decide if we're equal
            return NotImplemented
        return self.name == other.name and (
            self.json != other.json or self.raw_data_id == None
        )

    def __eq__(self, other):
        if not isinstance(other, HlogTag):
            # Don't recognise "other", so let *it* decide if we're equal
            return NotImplemented
        return self.name == other.name and self.json == other.json
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
