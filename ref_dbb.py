#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

from __future__ import annotations

import ntpath
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from botocore.client import BaseClient
from dependency_injector.wiring import inject, Provide
from sqlalchemy.orm import Session

from db.container import Container
from db.context import Context
from db.models import (
    Annotation,
    ExtractedData,
    ExtractedDataAllocationTask,
    IntegrityCheckReport,
    PTag,
    RawData,
    RawDataAllocationTask,
)
from db.services.annotation_service import AnnotationService
from db.services.cal_data_service import CalDataService
from db.services.raw_data_service import RawDataService
from db.services.raw_data_staging_service import RawDataStagingService
from db.services.extracted_data_service import ExtractedDataService
from db.services.ptag_service import PTagService
from db.services.scene_frame_service import SceneFrameService
from db.services.supplier_service import SupplierService
from db.services.scale_service import ScaleService
from db.services.integrity_check_report_service import IntegrityCheckReportService
from db.services.hlog_tag_service import HlogTagService
from db.services.zip_data_service import ZipDataService

from utilities.aws import (
    get_file_format_from_s3_path,
    get_file_name_from_s3_path,
    get_sn_path_from_s3_path,
    json_file_in_s3_to_dict,
    split_s3url_to_bucket_key,
)
from utilities.project_constants import _DB_STEP_SIZE
from utilities.db_model_gen import (
    get_modality_from_modality_dict,
    get_ptag_obj_from_dict,
    is_ptag_missing_req_fields,
    update_ptag_obj_from_dict,
)
from utilities.project_name_constants import (
    _1ahs_2d_polygon_linking_project_name,
    _1ahs_3d_cuboid_linking_project_name,
    _cms_mttt_2d_polygon_linking_project_name,
    _cms_mttt_3d_cuboid_linking_project_name,
)

_supplier_project_name_to_bucket_str = {
    _1ahs_2d_polygon_linking_project_name: "ics-1ahs",
    _1ahs_3d_cuboid_linking_project_name: "ics-1ahs",

    _cms_mttt_2d_polygon_linking_project_name: "ics-cms",
    _cms_mttt_3d_cuboid_linking_project_name: "ics-cms",

    "unknown": "unknown",
}


def get_bucket_prefix_from_supplier_project_name(project_name: str):
    return _supplier_project_name_to_bucket_str.get(project_name)

@inject
def get_archive_ready_raw_data_staging_paths(
    raw_data_staging_paths: List[str],
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
) -> List[str]:
    """ Get raw data staging file paths with unpacked -> 1 and archived -> 0 in raw data staging table

    Args:
        List[str]: list of raw data staging paths
    """
    archive_ready_raw_data_staging_paths = set()

    for x in range(0, len(raw_data_staging_paths), _DB_STEP_SIZE):
        archived_raw_data_staging_list = raw_data_staging_service.get_file_paths_ready_to_be_archived(raw_data_staging_paths[x:x+_DB_STEP_SIZE])
        if not archived_raw_data_staging_list:
            continue
        raw_data_staging_file_paths = [
            archived_raw_data_staging.file_path
            for archived_raw_data_staging in archived_raw_data_staging_list
        ]
        archive_ready_raw_data_staging_paths.update(raw_data_staging_file_paths)
    return list(archive_ready_raw_data_staging_paths)

@inject
def get_extracted_data_s3_locs_from_annotation_supplier_path(
    supplier_bucket: str,
    project_name: str,
    annotation_supplier_attachments: List[str],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
    debug_print: bool = False,
) -> Tuple[List[str], Dict]:
    """ Get multiple extracted data s3 locs from annotation supplier attachments based on the project name and the first attachment path encoded date
       "LTTS/CAT_INPUT_2024/CMS_MTTT_2D3D_Linking/2024.11.18_ppgPhase3/6675eb1d82b454994095c331/scene_frames/1400943867.025.json",
        "s3/cat-input-2024/LTTS/CAT_INPUT_2024/CMS_MTTT_2D3D_Linking/2024.11.18_ppgPhase1/6675eb04e14ff6af3054d860/Camera4ImageCompressed/1400875488.027734.jpg",

    -> "s3://ics-cms-prod/Unpacked/Cat/PPG/2024.05/2024.05.28/LNC09991/15.03.48_3-24/ExtractedData/for_scale/1400943867.025.json"
       "s3://ics-cms-prod/Unpacked/Cat/PPG/2024.05/2024.05.27/LNC09991/20.04.06_3-26/ExtractedData/Camera4ImageCompressed/1400875488.027734.jpg"
    """
    
    bucket_name = ""
    # get original bucket name
    bucket_prefix = get_bucket_prefix_from_supplier_project_name(project_name)
    if not bucket_prefix:
        raise ValueError(f"could not get bucket prefix from {project_name}")
    bucket_name += bucket_prefix
    if "dev" in supplier_bucket:
        bucket_name += "-dev"
    else:
        bucket_name += "-prod"
    
    file_format = get_file_format_from_s3_path(annotation_supplier_attachments[0])

    extracted_datas = extracted_data_service.get_extracted_data_like_s3_path_multiple([bucket_name, file_format])

    if not extracted_datas:
        raise RuntimeError(f"got no extracted datas from: {bucket_name} {file_format}")

    # create extracted data dict
    extracted_data_file_name_dict = {}
    for extracted_data in extracted_datas:
        extracted_data_file_name = get_file_name_from_s3_path(extracted_data.s3_loc)
        if debug_print and extracted_data_file_name_dict.get(extracted_data_file_name):
            print(f"potential collision at {extracted_data.s3_loc}")
        extracted_data_file_name_dict[extracted_data_file_name] = extracted_data.s3_loc

    # match file name
    attachments = []
    for annotation_supplier_attachment in annotation_supplier_attachments:
        attachment_file_name = get_file_name_from_s3_path(annotation_supplier_attachment)
        matched_attachment = extracted_data_file_name_dict.get(attachment_file_name)
        if not matched_attachment:
            raise ValueError(f"could not match attachment: {annotation_supplier_attachment}")
        attachments.append(matched_attachment)

    return attachments, extracted_data_file_name_dict

@inject
def get_hierarchical_ptag_paths(
    child_ptag_path: str,
    ptag_service: PTagService = Provide[Container.ptag_service],
) -> List[str]:
    child_base_path = get_sn_path_from_s3_path(child_ptag_path)
    if child_base_path is None:
        print("get_sn_path_from_s3_path could not match: {}".format(child_ptag_path))
        return []
    ptags = ptag_service.get_ptags_by_name_like(child_base_path)

    child_slice_index = child_ptag_path.rfind("/")
    child_dir = child_ptag_path[:child_slice_index]

    parent_ptags = []

    for ptag in ptags:
        # skip self
        if ptag.path == child_ptag_path:
            continue
        # check similarity to child base path
        ptag_slice_index = ptag.path.rfind("/")
        ptag_path_dir = ptag.path[:ptag_slice_index]

        if ptag_path_dir in child_dir:
            parent_ptags.append(ptag.path)

    return parent_ptags

@inject
def get_indexed_cal_data_paths(
    cal_data_paths: List[str],
    cal_data_service: CalDataService = Provide[Container.cal_data_service],
):
    """ Get indexed cal data file paths

    Args:
        List[str]: list of cal data file paths
    """

    indexed_cal_data_file_paths = set()
    indexed_cal_data_update_candidates = {}

    for x in range(0, len(cal_data_paths), _DB_STEP_SIZE):
        indexed_cal_data_list = cal_data_service.get_cal_datasets_by_file_paths(cal_data_paths[x:x+_DB_STEP_SIZE])
        if not indexed_cal_data_list:
            continue
        for indexed_cal_data in indexed_cal_data_list:
            if (
                indexed_cal_data.ptag_id is not None and
                indexed_cal_data.cal_base_log is not None and
                indexed_cal_data.tag_json is not None and
                indexed_cal_data.cal_base_log != ""
            ):
                indexed_cal_data_file_paths.add(indexed_cal_data.file_path)
            else:
                indexed_cal_data_update_candidates[indexed_cal_data.file_path] = indexed_cal_data
    return indexed_cal_data_file_paths, indexed_cal_data_update_candidates

@inject
def get_indexed_extracted_data_allocation_dict(
    supplier_task_id: int,
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    indexed_extracted_data_allocation_dict = {}

    indexed_extracted_data_task_allocations_list: List[ExtractedDataAllocationTask] = extracted_data_service.get_extracted_data_allocation_tasks_by_supplier_task_id(supplier_task_id)
    if indexed_extracted_data_task_allocations_list is not None:
        for indexed_extracted_data_task_allocation in indexed_extracted_data_task_allocations_list:
            indexed_extracted_data_allocation_dict[indexed_extracted_data_task_allocation.extracted_data_id] = indexed_extracted_data_task_allocation
    return indexed_extracted_data_allocation_dict

@inject
def get_indexed_extracted_data_s3_locs(
    sensor_data_s3_locs: List[str],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    """ Get indexed extracted data s3 locs

    Args:
        set[str]: list of sensor data s3 locs
    """

    indexed_extracted_data_dict = {}
    indexed_extracted_data_update_candidates = {}

    for x in range(0, len(sensor_data_s3_locs), _DB_STEP_SIZE):
        indexed_extracted_data_list = extracted_data_service.get_extracted_data_by_s3_paths(sensor_data_s3_locs[x:x+_DB_STEP_SIZE])
        if not indexed_extracted_data_list:
            continue
        for indexed_extracted_data in indexed_extracted_data_list:
            if (
                # cal data is not yet linked
                # indexed_extracted_data.cal_data_id is not None and
                indexed_extracted_data.ptag_id is not None and
                indexed_extracted_data.raw_data_id is not None
            ):
                indexed_extracted_data_dict[indexed_extracted_data.s3_loc] = indexed_extracted_data
            else:
                indexed_extracted_data_update_candidates[indexed_extracted_data.s3_loc] = indexed_extracted_data
    return indexed_extracted_data_dict, indexed_extracted_data_update_candidates

def get_indexed_hlog_tag_paths(
    hlog_tag_paths: List[str],
    hlog_tag_service: HlogTagService = Provide[Container.hlog_tag_service],
) -> List[str]:
    """ Get indexed hlog tag file paths

    Args:
        List[str]: list of hlog tag paths
    """

    indexed_hlog_tag_file_paths = set()

    for x in range(0, len(hlog_tag_paths), _DB_STEP_SIZE):
        indexed_hlog_tag_list = hlog_tag_service.get_hlog_tags_by_file_paths(hlog_tag_paths[x:x+_DB_STEP_SIZE])       
        
        if indexed_hlog_tag_list is None:
            continue
        
        for indexed_hlog_tag in indexed_hlog_tag_list:
            indexed_hlog_tag_file_paths.add(indexed_hlog_tag.file_path)        

    return list(indexed_hlog_tag_file_paths)

@inject
def get_indexed_integrity_check_report_data_paths(
    integrity_check_report_data_paths: List[str],
    integrity_check_report_service: IntegrityCheckReportService = Provide[Container.integrity_check_report_service],
) -> List[str]:
    """ Get indexed integrity check report data paths

    Args:
        List[str]: list of integrity check report data paths
    """
    indexed_integrity_check_report_data_paths = set()

    for x in range(0, len(integrity_check_report_data_paths), _DB_STEP_SIZE):
        indexed_integrity_check_report_data_path_list = integrity_check_report_service.get_integrity_check_report_by_s3_paths(integrity_check_report_data_paths[x:x+_DB_STEP_SIZE])
        if not indexed_integrity_check_report_data_path_list:
            continue
        for indexed_integrity_check_data in indexed_integrity_check_report_data_path_list:
            if indexed_integrity_check_data.raw_data_id is None:
                continue
            indexed_integrity_check_report_data_paths.add(indexed_integrity_check_data.report_path)
    return list(indexed_integrity_check_report_data_paths)

@inject
def get_indexed_ptag_paths(
    ptag_paths: List[str],
    ptag_service: PTagService = Provide[Container.ptag_service],
):
    """ Get indexed ptag paths

    Args:
        List[str]: list of indexed ptag paths
    """

    indexed_ptag_paths = set()
    indexed_ptag_update_candidates = {}
    indexed_ptag_memo_dict = {}

    for x in range(0, len(ptag_paths), _DB_STEP_SIZE):
        indexed_ptag_list = ptag_service.get_indexed_ptags_by_paths(ptag_paths[x:x+_DB_STEP_SIZE])
        if not indexed_ptag_list:
            continue
        for indexed_ptag in indexed_ptag_list:
            if (
                not is_ptag_missing_req_fields(indexed_ptag)
            ):
                indexed_ptag_paths.add(indexed_ptag.path)
                indexed_ptag_memo_dict[indexed_ptag.path] = indexed_ptag
            else:
                indexed_ptag_update_candidates[indexed_ptag.path] = indexed_ptag
    return indexed_ptag_paths, indexed_ptag_memo_dict

@inject
def get_indexed_raw_data_paths(
    raw_data_paths: List[str],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
):
    """ Get indexed raw data file paths

    Args:
        List[str]: list of raw data paths
    """

    indexed_raw_data_file_paths = set()
    indexed_raw_data_update_candidates = {}

    for x in range(0, len(raw_data_paths), _DB_STEP_SIZE):
        indexed_raw_data_list = raw_data_service.get_raw_data_by_file_paths(raw_data_paths[x:x+_DB_STEP_SIZE])
        if not indexed_raw_data_list:
            continue
        for indexed_raw_data in indexed_raw_data_list:
            if (
                indexed_raw_data.ptag_id is not None and
                indexed_raw_data.tag_json is not None
            ):
                indexed_raw_data_file_paths.add(indexed_raw_data.file_path)
            else:
                indexed_raw_data_update_candidates[indexed_raw_data.file_path] = indexed_raw_data
    return indexed_raw_data_file_paths, indexed_raw_data_update_candidates

@inject
def get_indexed_raw_data_staging_paths(
    raw_data_staging_paths: List[str],
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """ Get indexed raw data staging file paths

    Args:
        List[str]: list of indexed raw data staging paths
    """
    indexed_raw_data_staging_file_paths = set()
    indexed_raw_data_staging_update_candidates = {}

    for x in range(0, len(raw_data_staging_paths), _DB_STEP_SIZE):
        indexed_raw_data_staging_list = raw_data_staging_service.get_raw_data_staging_by_file_paths(raw_data_staging_paths[x:x+_DB_STEP_SIZE])
        if not indexed_raw_data_staging_list:
            continue
        for indexed_raw_data_staging in indexed_raw_data_staging_list:
            if (
                indexed_raw_data_staging.ptag_id is not None and
                indexed_raw_data_staging.unpacked is not None
            ):
                indexed_raw_data_staging_file_paths.add(indexed_raw_data_staging.file_path)
            else:
                indexed_raw_data_staging_update_candidates[indexed_raw_data_staging.file_path] = indexed_raw_data_staging
    return indexed_raw_data_staging_file_paths, indexed_raw_data_staging_update_candidates

@inject
def get_indexed_supplier_scene_frame_data_paths(
    scene_frame_data_paths: List[str],
    scene_frame_service: SceneFrameService = Provide[Container.scene_frame_service],
):
    """ Get indexed scene frame data paths

    Args:
        List[str]: list of scene frame data paths
    """
    indexed_scene_frame_data_paths = set()

    for x in range(0, len(scene_frame_data_paths), _DB_STEP_SIZE):
        indexed_scene_frame_data_path_list = scene_frame_service.get_scene_frame_extracted_data_paths_by_s3_paths(scene_frame_data_paths[x:x+_DB_STEP_SIZE])
        if not indexed_scene_frame_data_path_list:
            continue
        indexed_scene_frame_data_paths.update(indexed_scene_frame_data_path_list)
    return indexed_scene_frame_data_paths

@inject
def get_indexed_supplier_task_obj_ids(
    supplier_task_ids: List[str],
    supplier_service: SupplierService = Provide[Container.supplier_service],
):
    indexed_supplier_task_obj_id_memo_dict = {}

    for x in range(0, len(supplier_task_ids), _DB_STEP_SIZE):
        indexed_supplier_task_list = supplier_service.get_supplier_task_by_supplier_task_ids(supplier_task_ids[x:x+_DB_STEP_SIZE])
        if not indexed_supplier_task_list:
            continue
        for indexed_supplier_task in indexed_supplier_task_list:
            indexed_supplier_task_obj_id_memo_dict[indexed_supplier_task.task_id] = indexed_supplier_task.id

    return indexed_supplier_task_obj_id_memo_dict

@inject
def get_indexed_zip_data_paths(
    zip_data_paths: List[str],
    zip_data_service: ZipDataService = Provide[Container.zip_data_service],
):
    """ Get indexed zip data file paths

    Args:
        List[str]: list of zip data paths
    """
    indexed_zip_data_file_paths = set()
    indexed_zip_data_update_candidates = {}

    for x in range(0, len(zip_data_paths), _DB_STEP_SIZE):
        indexed_zip_data_list = zip_data_service.get_zip_data_by_file_paths(zip_data_paths[x:x+_DB_STEP_SIZE])
        if not indexed_zip_data_list:
            continue
        for indexed_zip_data in indexed_zip_data_list:
            if (
                indexed_zip_data.ptag_id is not None
            ):
                indexed_zip_data_file_paths.add(indexed_zip_data.file_path)
            else:
                indexed_zip_data_update_candidates[indexed_zip_data.file_path] = indexed_zip_data
    return indexed_zip_data_file_paths, indexed_zip_data_update_candidates

@inject
def get_linking_extracted_data_paths_by_supplier_task_info(
    supplier_task_id: str,
    project_name: str,
    task_type: str,
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service]
) -> List[str]:
    """ Right now this gets extracted data that was allocated to a specific task, which in this case is the scene frame json
    """

    extracted_data_loc_list = extracted_data_service.get_extracted_data_s3_locs_by_task_info(supplier_task_id, project_name, task_type)
    extracted_data_loc_list.sort()

    return extracted_data_loc_list


@inject
def get_or_generate_ptag_db_obj(
    s3_client: BaseClient,
    ptag_path: str,
    ptag_memo_dict: dict[str, PTag | None] | None = None,
    session: Session | None = None,
    test_run: bool = False,
    ptag_service: PTagService = Provide[Container.ptag_service],
):
    """Get or generate ptag object from ptag_path

    Args:
        ptag_path: ptag path to retrieve or generate
    """
    if ptag_memo_dict is None:
        ptag_memo_dict = {}

    ptag = ptag_memo_dict.get(ptag_path, ptag_service.get_ptag_by_path(ptag_path))
    if ptag and not is_ptag_missing_req_fields(ptag):
        return ptag

    ptag_bucket, ptag_key = split_s3url_to_bucket_key(ptag_path)
    md5: str = Path(ptag_key).name
    if "_" in md5:
        md5 = md5.rsplit("_", 1)[-1]

    try:
        ptag_dict = json_file_in_s3_to_dict(s3_client, ptag_bucket, ptag_key)
    except ValueError:
        # parent ptag not guaranteed to exist
        return None

    session = session or Context().get_session()

    if ptag:
        ptag = update_ptag_obj_from_dict(ptag, ptag_dict, md5)

        if not test_run:
            session.add(ptag)

        ptag_memo_dict[ptag_path] = ptag

        return ptag

    ptag = get_ptag_obj_from_dict(ptag_path, ptag_dict, md5)
    if ptag is None:
        return None

    # add new ptag in db
    if not test_run:
        try:
            session.add(ptag)
        except Exception as e:
            raise ValueError(f"Error while adding PTag with {ptag_path=} to session: {e}") from e

    ptag_memo_dict[ptag_path] = ptag

    # add modalities
    for location in ptag_dict.get("modalities", []):
        for modality in location.get("modalities", []):
            modality_obj = get_modality_from_modality_dict(modality)

            modality_obj.ptag = ptag
            if test_run:
                continue

            try:
                session.add(modality_obj)
            except Exception as e:
                raise ValueError(f"Error while adding Modality for {ptag_path=} to session: {e}") from e

    return ptag


@inject
def get_paged_extracted_data_by_file_format(
    file_format: str,
    path_filter: str,
    unannotated: bool = False,
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    extracted_data_list = []
    page = 1
    while True:
        paged_result = extracted_data_service.get_extracted_datasets(
            unannotated=unannotated,
            file_format=file_format,
            keyword_path=path_filter,
            page=page,
            per_page=500
        )
        if not paged_result:
            break
        page += 1
        extracted_data_list += paged_result

    return extracted_data_list

@inject
def get_raw_data_staging_paths_like_staging_prefix(
    aws_s3_bucket: str,
    staging_prefix: str,
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """ Get unpacked raw data staging file paths

    Args:
        str : Staging prefix
    """
    unpacked_raw_data_staging_list = raw_data_staging_service.get_all_raw_data_staging_paths(aws_s3_bucket,staging_prefix)
    return unpacked_raw_data_staging_list

@inject
def get_related_attachments_from_scene_frame_paths(
    s3_locs: List[str],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
    scene_frame_service: SceneFrameService = Provide[Container.scene_frame_service],
):
    scene_frame_extracted_datas = extracted_data_service.get_extracted_data_by_s3_paths(s3_locs)
    scene_frame_extracted_datas_ids = [scene_frame_extracted_data.id for scene_frame_extracted_data in scene_frame_extracted_datas]
    related_extracted_data_ids = scene_frame_service.get_scene_frame_related_extracted_data_ids_from_extracted_data_ids(scene_frame_extracted_datas_ids)
    related_extracted_datas = extracted_data_service.get_extracted_data_by_ids(related_extracted_data_ids)
    related_extracted_datas_paths = [related_extracted_data.s3_loc for related_extracted_data in related_extracted_datas]
    return related_extracted_datas_paths

@inject
def get_scale_tasks_for_cat_coco_conversion(
    project_name: str,
    scale_service: ScaleService = Provide[Container.scale_service],
) -> List[str]:
    """obtain multiple scale tasks based on project name and batch name
    Args:
        project_name (str): scale project name
        Returns:
            List[str]: Scale task ids
    """
    scale_tasks = scale_service.get_scale_tasks_for_cat_coco_conversion(project_name)
    scale_task_id_list = [task.task_id for task in scale_tasks]

    return scale_task_id_list

@inject
def get_ungenerated_scene_frame_pcd_paths(
    path_filter: str,
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
    scene_frame_service: SceneFrameService = Provide[Container.scene_frame_service],
):
    """ Get all extracted data pcds not already in scene frame table """
    indexed_pcds = extracted_data_service.get_extracted_data_like_s3_path(".pcd", 1000000)
    
    indexed_pcd_ids = [
        indexed_pcd.id for indexed_pcd in indexed_pcds
        if (
            path_filter in indexed_pcd.s3_loc
        )
    ]

    generated_pcd_ids = set()

    for x in range(0, len(indexed_pcd_ids), _DB_STEP_SIZE):
        pcd_id_list = scene_frame_service.get_scene_frame_related_extracted_data_ids_from_related_extracted_data_ids(indexed_pcd_ids[x:x+_DB_STEP_SIZE])
        if pcd_id_list is not None:
            generated_pcd_ids.update(pcd_id_list)

    generated_pcd_ids_list = list(generated_pcd_ids)
    ungenerated_pcds = set()

    for indexed_pcd in indexed_pcds:
        if indexed_pcd.id in generated_pcd_ids_list:
            continue
        ungenerated_pcds.add(indexed_pcd)

    ungenerated_pcd_paths = [
        ungenerated_pcd.s3_loc
        for ungenerated_pcd in ungenerated_pcds
        if (
            path_filter in ungenerated_pcd.s3_loc
        )
    ]

    return ungenerated_pcd_paths

@inject
def get_unpack_ready_raw_data_staging_paths(
    raw_data_staging_paths: List[str],
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """ Get unpacked raw data staging file paths

    Args:
        List[str]: list of unpacked raw data staging paths
    """
    unpack_ready_raw_data_staging_paths = set()

    for x in range(0, len(raw_data_staging_paths), _DB_STEP_SIZE):
        unpacked_raw_data_staging_list = raw_data_staging_service.get_raw_data_staging_not_unpacked_yet(raw_data_staging_paths[x:x+_DB_STEP_SIZE])
        if not unpacked_raw_data_staging_list:
            continue
        raw_data_staging_file_paths = [
            unpacked_raw_data_staging.file_path
            for unpacked_raw_data_staging in unpacked_raw_data_staging_list
        ]
        unpack_ready_raw_data_staging_paths.update(raw_data_staging_file_paths)
    return unpack_ready_raw_data_staging_paths

# consider changing the 'merge' to a create new merged copy
@inject
def merge_extracted_data_list(
    extracted_data_list: List[ExtractedData],
    annotation_service: AnnotationService = Provide[Container.annotation_service],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    extracted_data_first = True
    extracted_data_id_with_annotation: Optional[int] = None
    extracted_data_id_with_task_allocation: Optional[int] = None
    delete_count = 0

    # check to see if extracted data allocation task exists, if it does, should be considered primary copy
    for duplicate_extracted in extracted_data_list:
        extracted_data_allocation_task: Optional[ExtractedDataAllocationTask] = extracted_data_service.get_extracted_data_allocation_task_by_extracted_data_id(duplicate_extracted.id)
        if extracted_data_allocation_task:
            extracted_data_id_with_task_allocation = duplicate_extracted.id
            break
    # check to see if annotation exists, if it does, should be considered primary copy
    for duplicate_extracted in extracted_data_list:
        annotation: Optional[Annotation] = annotation_service.get_annotation_by_extracted_data_id(duplicate_extracted.id)
        if annotation:
            extracted_data_id_with_annotation = duplicate_extracted.id
            break
    for duplicate_extracted in extracted_data_list:
        if extracted_data_id_with_task_allocation:
            if duplicate_extracted.id == extracted_data_id_with_task_allocation:
                continue
            extracted_data_service.delete_extracted_data_by_id(duplicate_extracted.id)
            delete_count += 1
        elif extracted_data_id_with_annotation:
            if duplicate_extracted.id == extracted_data_id_with_annotation:
                continue
            extracted_data_service.delete_extracted_data_by_id(duplicate_extracted.id)
            delete_count += 1
        else:
            if not extracted_data_first:
                extracted_data_service.delete_extracted_data_by_id(duplicate_extracted.id)
                delete_count += 1
            extracted_data_first = False
    return delete_count

@inject
def merge_raw_data_list(
    raw_data_list: List[RawData],
    integrity_check_service: IntegrityCheckReportService = Provide[Container.integrity_check_report_service],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
) -> int:
    raw_data_first = True
    raw_data_id_with_integrity_check: Optional[int] = None
    delete_count = 0
    # check to see if integrity check report exists, if it does, should be considered primary copy
    for duplicate_raw in raw_data_list:
        integrity_check: Optional[IntegrityCheckReport] = integrity_check_service.get_integrity_check_report_by_raw_data_id(duplicate_raw.id)
        if integrity_check:
            raw_data_id_with_integrity_check = duplicate_raw.id
            break
    for duplicate_raw in raw_data_list:
        if raw_data_id_with_integrity_check:
            if duplicate_raw.id == raw_data_id_with_integrity_check:
                continue
            raw_data_service.delete_raw_data_by_id(duplicate_raw.id)
            delete_count += 1
        else:
            if not raw_data_first:
                raw_data_service.delete_raw_data_by_id(duplicate_raw.id)
                delete_count += 1
            raw_data_first = False
    return delete_count

@inject
def task_param_content_needs_cleaning(
    task_param_dict: Dict,
):
    attachments = task_param_dict.get("attachments")
    frame_attachments = task_param_dict.get("frame_attachments")
    if attachments or attachments is not None:
        return True
    if frame_attachments or frame_attachments is not None:
        return True
    if task_param_dict.get("lidar_task"):
        return True
    if task_param_dict.get("task_id"):
        return True
    if task_param_dict.get("batch"):
        return True
    if task_param_dict.get("unique_id"):
        return True
    tags = task_param_dict.get("tags")
    if tags or tags is not None:
        return True
    return False

@inject
def update_raw_data_staging_table_with_archived(
    raw_data_staging_path: str,
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """ updates raw data staging file paths with archived status

    Args:
        str : Archive ready raw data staging path
    """
    updated_raw_data_staging_id = raw_data_staging_service.update_raw_data_staging_with_archived(raw_data_staging_path)
    return updated_raw_data_staging_id

@inject
def update_raw_data_staging_table_with_unpacked(
    staging_paths: list[str],
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
) -> list[int]:
    """Update raw data staging objects with unpacked status = True.

    Args:
        list[str]: list of unpacked raw data staging paths

    Returns:
        list[int]: list of updated raw data staging ids
    """
    updated_ids: list[int] = []
    for path in staging_paths:
        updated_id = raw_data_staging_service.update_raw_data_staging_with_unpacked(path)
        updated_ids.append(updated_id)

    return updated_ids

def windowed_query(q, column, windowsize):
    """"Break a Query into chunks on a given column. from https://github.com/sqlalchemy/sqlalchemy/wiki/RangeQuery-and-WindowedRangeQuery"""

    single_entity = q.is_single_entity
    q = q.add_column(column).order_by(column)
    last_id = None

    while True:
        subq = q
        if last_id is not None:
            subq = subq.filter(column > last_id)
        chunk = subq.limit(windowsize).all()
        if not chunk:
            break
        last_id = chunk[-1][-1]
        for row in chunk:
            if single_entity:
                yield row[0]
            else:
                yield row[0:-1]
