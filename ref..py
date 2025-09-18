#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

import json
import logging

from airflow.decorators import task
from airflow.operators.python import get_current_context
from dependency_injector.wiring import inject

from utilities.analytics_utilities import (
    create_batch_client,
    run_pipeline_job_in_batch,
    get_batch_job_name,
    run_snippet_generation_job_in_batch,
    run_ais2ros_pipeline_job_in_batch,
    get_json_hash_value,
    push_json_object_to_s3,
    create_s3_client
)
from utilities.log_constants import _DEFAULT_LOGGER_NAME


@inject
@task()
def run_analytics_batch_job(
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_token: str,
    aws_batch_region: str
):
    """run analytics batch job

    Args:
        aws_batch_key (str): [description]
        aws_batch_secret (str): [description]   
        aws_batch_token (str): [description]
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf  
    hash_val = conf.get("config_hash", "")  

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    airflow_logger.info("run_analytics_batch_job")
    # aws_region = "us-east-1"

    # hash_val = 'd0bcb79d7196367c0bb6e3271c39325b8a82874f'

    if hash_val != "":

        batch_client = create_batch_client(aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)

        batch_job_name = get_batch_job_name('PIPELINE')

        job_reponse = run_pipeline_job_in_batch(batch_client, hash_val, batch_job_name, aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)

    else:

        raise Exception('CONFIG hash value not specified. Unable to start JOB')


@inject
@task()
def run_snippet_generation_batch_job(
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_token: str,
    aws_batch_region: str  
):
    """run analytics batch job

    Args:
        aws_batch_key (str): [description]
        aws_batch_secret (str): [description]   
        aws_batch_token (str): [description]
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    airflow_logger.info("run_analytics_batch_job")

    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf   
    # prefix_path = conf.get("prefix_path", "")
    airflow_logger.info("dag config: {}".format(conf)) 

    conf_str = json.dumps(conf)

    # config_file = json.loads(conf_str)

    hash_val = get_json_hash_value(conf_str)
    
    # aws_region = "us-east-1"

    # hash_val = 'd0bcb79d7196367c0bb6e3271c39325b8a82874f'

    s3_client = create_s3_client(aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)

    is_error = push_json_object_to_s3(s3_client, hash_val, conf_str)

    batch_client = create_batch_client(aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)    

    batch_job_name = get_batch_job_name('SNIPPETGEN')

    job_reponse = run_snippet_generation_job_in_batch(batch_client, hash_val, batch_job_name, aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)


@inject
@task()
def run_ais2ros_pipeline_batch_job(
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_token: str,
    aws_batch_region: str
):
    """run analytics batch job

    Args:
        aws_batch_key (str): [description]
        aws_batch_secret (str): [description]   
        aws_batch_token (str): [description]
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf  
    analytics_hash_val = conf.get("analytics_config_hash", "")  
    ros_conversion_hash_val = conf.get("ros_conversion_config_hash", "")  

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    airflow_logger.info("run_ais2ros_pipeline_batch_job")
    # aws_region = "us-east-1"

    # hash_val = 'd0bcb79d7196367c0bb6e3271c39325b8a82874f'

    if analytics_hash_val != "" and ros_conversion_hash_val != "":

        batch_client = create_batch_client(aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)

        batch_job_name = get_batch_job_name('AISROSPIPELINE')

        job_reponse = run_ais2ros_pipeline_job_in_batch(batch_client, analytics_hash_val, ros_conversion_hash_val, batch_job_name, aws_batch_key, aws_batch_secret, aws_batch_token, aws_batch_region)

    else:

        raise Exception('Analytics and/or Ros conversion config hash value not specified. Unable to start the JOB')
###########################
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

import ast
import json
import logging
import re
from typing import Any, Dict, List, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from dependency_injector.wiring import Provide, inject
from sqlalchemy.orm import Query

from db.container import Container
from db.context import Context
from db.models import (Annotation, CatAnnotationSupplierEnum, ExtractedData,
                       ExtractedDataAllocationTask, RawData, RawLogTypeEnum,
                       ScaleTask, ScaleTaskParam, SupplierTask,
                       SupplierTaskParam, TaxonomyAllocationTask)
from db.services.annotation_service import AnnotationService
from db.services.dataset_service import DatasetService
from db.services.dataset_association_service import DatasetAssociationService
from db.services.extracted_data_service import ExtractedDataService
from db.services.integrity_check_report_service import (
    IntegrityCheckReportService)
from db.services.raw_data_service import RawDataService
from db.services.raw_data_staging_service import RawDataStagingService
from db.services.scale_service import ScaleService
from db.services.supplier_service import SupplierService, get_task_attachments_locs
from db.services.taxonomy_service import TaxonomyService
from utilities.aws import get_base_log_or_snippet_name_from_s3_path
from utilities.db import (get_indexed_supplier_task_obj_ids,
                          merge_extracted_data_list, merge_raw_data_list,
                          task_param_content_needs_cleaning)
from utilities.project_constants import _DB_STEP_SIZE
from utilities.log_constants import _DEFAULT_LOGGER_NAME
from utilities.validation import get_dict_md5


@inject
@task()
def clean_extracted_and_raw_data_table(
    annotation_service: AnnotationService = Provide[Container.annotation_service],
    extracted_data_service: ExtractedDataService = Provide[
        Container.extracted_data_service
    ],
    integrity_check_service: IntegrityCheckReportService = Provide[
        Container.integrity_check_report_service
    ],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    raw_data_staging_service: RawDataStagingService = Provide[
        Container.raw_data_staging_service
    ],
):
    """remove redundant raw, raw data staging and extracted data values

    Args:

    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    duplicate_raw_data_paths = raw_data_service.get_duplicate_raw_data_paths()
    airflow_logger.info(
        "duplicate_raw_data_paths len: {}".format(len(duplicate_raw_data_paths))
    )

    duplicate_extracted_data_paths = (
        extracted_data_service.get_duplicate_extracted_data_paths()
    )
    airflow_logger.info(
        "duplicate_extracted_data_paths len: {}".format(
            len(duplicate_extracted_data_paths)
        )
    )

    duplicate_raw_data_staging = (
        raw_data_staging_service.get_duplicate_raw_data_staging_records()
    )
    airflow_logger.info(
        f"duplicate_raw_data_staging len: {len(duplicate_raw_data_staging)}"
    )

    unlinked_extracted_datas: List[ExtractedData] = extracted_data_service.get_unlinked_extracted_data()

    raw_data_id_memo: Dict = {}

    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        try:
            airflow_logger.info("start deduplicate raw data")
            upsert_count = 0
            for raw_data_path in duplicate_raw_data_paths:
                if upsert_count >= _DB_STEP_SIZE:
                    airflow_logger.info(
                        f"reached commit limit, committing {_DB_STEP_SIZE} objects"
                    )
                    session.commit()
                    upsert_count = 0
                duplicate_raw_data_list = (
                    raw_data_service.get_raw_datasets_by_file_path(raw_data_path)
                )
                upsert_count += merge_raw_data_list(
                    duplicate_raw_data_list,
                    integrity_check_service=integrity_check_service,
                    raw_data_service=raw_data_service,
                )
            session.commit()

            airflow_logger.info("start deduplicate extracted data")
            upsert_count = 0
            for extracted_data_path in duplicate_extracted_data_paths:
                if upsert_count >= _DB_STEP_SIZE:
                    airflow_logger.info(
                        f"reached commit limit, committing {_DB_STEP_SIZE} objects"
                    )
                    session.commit()
                    upsert_count = 0
                duplicate_extracted_data_list = (
                    extracted_data_service.get_extracted_dataset_by_s3_path(
                        extracted_data_path
                    )
                )
                upsert_count += merge_extracted_data_list(
                    duplicate_extracted_data_list,
                    annotation_service=annotation_service,
                    extracted_data_service=extracted_data_service,
                )
            session.commit()

            airflow_logger.info("start deduplicate raw data staging")
            upsert_count = 0
            for raw_data_staging_file_path in duplicate_raw_data_staging:
                if upsert_count >= _DB_STEP_SIZE:
                    airflow_logger.info(
                        f"reached commit limit, committing {_DB_STEP_SIZE} objects"
                    )
                    session.commit()
                    upsert_count = 0
                raw_data_staging_first_record = True
                duplicate_raw_data_staging_list = (
                    raw_data_staging_service.get_raw_data_staging_by_file_paths(
                        [raw_data_staging_file_path.file_path]
                    )
                )
                for duplicate_raw_data_staging in duplicate_raw_data_staging_list:
                    if not raw_data_staging_first_record:
                        raw_data_staging_service.delete_raw_data_staging_record_by_id(
                            duplicate_raw_data_staging.id
                        )
                    raw_data_staging_first_record = False
                upsert_count += 1
            session.commit()

            airflow_logger.info("start linking raw data column in extracted data")
            for extracted_data in unlinked_extracted_datas:
                base_name = get_base_log_or_snippet_name_from_s3_path(extracted_data.s3_loc)
                raw_data_id: Optional[RawData] = raw_data_id_memo.get(base_name)
                if not raw_data_id:
                    raw_data = raw_data_service.get_raw_data_by_file_path_like_op(base_name)
                    if not raw_data:
                        airflow_logger.warning("Could not link {} {}".format(extracted_data.s3_loc, base_name))
                        continue
                    raw_data_id = raw_data.id
                    raw_data_id_memo[base_name] = raw_data_id

                extracted_data.raw_data_id = raw_data_id
                session.add(extracted_data)

            session.commit()

        except:
            session.rollback()
            raise


@inject
@task()
def clean_link_tables(
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    supplier_service: SupplierService = Provide[Container.supplier_service],
):
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    supplier_tasks: List[SupplierTask] = supplier_service.get_all_supplier_tasks()

    supplier_task_ids = [supplier_task.id for supplier_task in supplier_tasks]

    raw_data_task_allocations = raw_data_service.get_raw_data_allocation_tasks_by_supplier_task_ids(supplier_task_ids)

    raw_data_allocation_task_dict: Dict[str, List] = {}
    for raw_data_allocation_task in raw_data_task_allocations:
        if raw_data_allocation_task_dict.get(raw_data_allocation_task.supplier_task_id):
            raw_data_allocation_task_dict[raw_data_allocation_task.supplier_task_id].append(raw_data_allocation_task)
        else:
            raw_data_allocation_task_dict[raw_data_allocation_task.supplier_task_id] = [raw_data_allocation_task]

    with Context().new_session_scope() as session:
        # link tasks to raw data in raw data allocation task table
        try:
            airflow_logger.info("start linking raw data allocation task")
            for supplier_task in supplier_tasks:
                allocations = raw_data_allocation_task_dict.get(supplier_task.task_id)
                if allocations:
                    continue
                supplier_task_dict = json.loads(supplier_task.data)
                s3_locs = get_task_attachments_locs(supplier_task_dict)
                try:
                    raw_data = raw_data_service.get_raw_data_by_task_loc(s3_locs[0])
                    if not raw_data:
                        raise RuntimeError(f"could not get raw data id from path {s3_locs[0]}")
                except RuntimeError as e:
                    airflow_logger.error(f"failed to link {supplier_task.task_id}, {e}")
                    continue
                raw_data_service.create_raw_data_allocation_task(
                    supplier_task.id, raw_data.id
                )

            session.commit()
        except:
            session.rollback()
            raise
    


def attempt_clean_tag_str(tag_str: str) -> Optional[str]:
    """Clean up the tags in scale task stored in DB.

    Due to a mistake in how the tags were passed in to the ingest function, they were stored in
    an incorrect format.

    Args:
        tag_str (str): The tags string in the incorrect format

    Returns:
        str: The tags string in the correct format

    Usage:
        tag_str = "[,',t,a,g,1,',,,',t,a,g,2,',]"
        Output: "tag1,tag2"

        tag_str = "N,o,n,e"
        Output: None

        tag_str: "tag1,tag2"
        Output: "tag1,tag2"
    """
    # ",".join(str(None))
    if tag_str == "N,o,n,e":
        return None

    # in case the tag str is already clean
    if "]" not in tag_str and "[" not in tag_str:
        return tag_str

    # comma followed by any alphanumeric char/a comma/a hyphen/a whitespace char
    tag = re.sub(r",([\w,\s-])", r"\g<1>", tag_str)

    # remove any spaces, clean the start and end delimiters, as well as quotes
    tag = tag.replace(" ", "").replace("[,", "[").replace(",]", "]").replace(",'", "'")

    # replace more than 1 consecutive comma with a single comma
    tag = re.sub(r",+", ",", tag)

    # ensure that the cleaned up str is a valid python list
    tags: Optional[List[str]] = ast.literal_eval(tag)
    if tags is None:
        return None

    tags = [t for t in tags if t]

    return ",".join(tags)


@inject
@task()
def clean_tags_in_scale_task_table():
    """Clean up the tags in scale task stored in DB."""
    query: Query = ScaleTask.get()

    ctx = get_current_context()
    config: Dict[str, Any] = ctx["dag_run"].conf

    if "offset" in config:
        query = query.offset(config["offset"])

    if "limit" in config:
        query = query.limit(config["limit"])

    batch_size: int = config.get("batch_size", 100)

    scale_tasks: List[ScaleTask] = query.all()

    with Context().session_scope() as session:
        batch: List[ScaleTask] = []

        for task in scale_tasks:
            if len(batch) >= batch_size:
                # add all the tasks from the current batch
                session.add_all(batch)
                session.flush()
                session.commit()

                # clear the batch for the next round of processing
                batch.clear()

            task.tags = attempt_clean_tag_str(task.tags)
            batch.append(task)


@inject
def clean_task_param_table_helper(
    airflow_logger: logging.Logger,
    supplier_enum: CatAnnotationSupplierEnum = CatAnnotationSupplierEnum.Unset,
    task_param_ids: List[int] = [],
    scale_service: ScaleService = Provide[Container.scale_service],
    supplier_service: SupplierService = Provide[Container.supplier_service],
):
    missing_annotation_attributes: List[int] = []
    missing_geometries: List[int] = []
    annotation_attributes_updated: List[int] = []
    geometries_updated: List[int] = []
    cleaned: List[int] = []
    removed: List[int] = []

    with Context().new_session_scope() as session:
        if supplier_enum == CatAnnotationSupplierEnum.Scale:
            task_params = scale_service.get_scale_task_params()
        else:
            task_params = supplier_service.get_all_supplier_task_params()
        for task_param in task_params:
            if task_param_ids:
                if task_param.id not in task_param_ids:
                    continue
                else:
                    airflow_logger.debug(f"cleaning {task_param.id}")
            content_dict: Optional[Dict] = json.loads(task_param.content)
            if not content_dict:
                raise ValueError(
                    f"could not get content from: {task_param.id} - {task_param.md5}"
                )
            # check md5
            calc_md5 = get_dict_md5(content_dict)
            if calc_md5 != task_param.md5:
                airflow_logger.warning(f"task param: {task_param.id} calc md5 mismatch")
                task_param.md5 = calc_md5
                session.add(task_param)
                session.commit()
            if (
                task_param.annotation_attributes is None
                or task_param.annotation_attributes == ""
            ):
                # fix annotation_attributes
                annotation_attributes = content_dict.get("annotation_attributes")
                if not annotation_attributes:
                    airflow_logger.debug(
                        f"could not get annotation_attributes from:  {task_param.id} - {task_param.md5} - {content_dict}"
                    )
                    missing_annotation_attributes.append(task_param.id)
                else:
                    task_param.annotation_attributes = json.dumps(annotation_attributes)
                    session.add(task_param)
                    annotation_attributes_updated.append(task_param.id)
            if task_param.geometries is None or task_param.geometries == "":
                # fix geometries
                geometries = content_dict.get("geometries")
                if not geometries:
                    airflow_logger.debug(
                        f"could not get geometries from:  {task_param.id} - {task_param.md5} - {content_dict}"
                    )
                    missing_geometries.append(task_param.id)
                else:
                    task_param.geometries = json.dumps(geometries)
                    session.add(task_param)
                    geometries_updated.append(task_param.id)
            if task_param_content_needs_cleaning(content_dict):
                content_dict.pop("attachments", None)
                content_dict.pop("frame_attachments", None)
                content_dict.pop("lidar_task", None)
                content_dict.pop("task_id", None)
                content_dict.pop("batch", None)
                content_dict.pop("unique_id", None)
                content_dict.pop("tags", None)
                # update md5
                calc_md5 = get_dict_md5(content_dict)
                airflow_logger.debug(f"new calc_md5 for {task_param.id} - {calc_md5}")
                if supplier_enum == CatAnnotationSupplierEnum.Scale:
                    old_task_param: ScaleTaskParam = (
                        scale_service.get_scale_task_param_by_md5(calc_md5)
                    )
                else:
                    old_task_param: SupplierTaskParam = (
                        supplier_service.get_supplier_task_param_by_md5(calc_md5)
                    )
                if not old_task_param:
                    task_param.md5 = calc_md5
                    task_param.content = json.dumps(content_dict)
                    session.add(task_param)
                    cleaned.append(task_param.id)
                elif old_task_param.id != task_param.id:
                    # link tasks if already exists
                    if supplier_enum == CatAnnotationSupplierEnum.Scale:
                        tasks = scale_service.get_scale_tasks_by_param_id(task_param.id)
                    else:
                        tasks = supplier_service.get_supplier_tasks_by_param_id(
                            task_param.id
                        )
                    airflow_logger.debug(
                        f"replacing {len(tasks)} tasks with old task param {old_task_param.id}"
                    )
                    for task in tasks:
                        if supplier_enum == CatAnnotationSupplierEnum.Scale:
                            task.scale_task_param_id = old_task_param.id
                        else:
                            task.supplier_task_param_id = old_task_param.id
                        session.add(task)
                    session.commit()
                    # delete task_param
                    if supplier_enum == CatAnnotationSupplierEnum.Scale:
                        scale_service.delete_scale_task_param_by_id(task_param.id)
                    else:
                        supplier_service.delete_supplier_task_param_by_id(task_param.id)
                    removed.append(task_param.id)
            else:
                airflow_logger.debug(f"task param {task_param.id} content passed")

        airflow_logger.info(
            f"cleaned: {len(cleaned)} removed: {len(removed)} annotation_attributes_updated: {len(annotation_attributes_updated)} geometries_updated: {len(geometries_updated)}"
        )
        if missing_annotation_attributes:
            airflow_logger.warning(
                f"missing_annotation_attributes: {missing_annotation_attributes}"
            )
        if missing_geometries:
            airflow_logger.warning(f"missing_geometries: {missing_geometries}")

        session.commit()


@inject
@task()
def clean_task_param_table(
    scale_service: ScaleService = Provide[Container.scale_service],
    supplier_service: SupplierService = Provide[Container.supplier_service],
):
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    task_param_ids: List[int] = conf.get("task_param_ids", [])
    log_level_debug: bool = conf.get("log_level_debug", False)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    clean_task_param_table_helper(
        airflow_logger,
        CatAnnotationSupplierEnum.Scale,
        task_param_ids,
        scale_service,
        supplier_service,
    )
    clean_task_param_table_helper(
        airflow_logger,
        CatAnnotationSupplierEnum.Unset,
        task_param_ids,
        scale_service,
        supplier_service,
    )


@inject
@task()
def delete_matching_extracted_data(
    extracted_data_service: ExtractedDataService = Provide[
        Container.extracted_data_service
    ],
):
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    path_keywords = conf.get("path_keywords", [])
    log_level_debug: bool = conf.get("log_level_debug", False)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    if not path_keywords:
        airflow_logger.error("no path keywords given")
        return

    with Context().new_session_scope() as session:
        extracted_datas = (
            extracted_data_service.get_extracted_data_like_s3_path_multiple(
                path_keywords, max_return=1000000
            )
        )
        airflow_logger.info(f"got {len(extracted_datas)} extracted datas")

        upsert_count: int = 0

        for extracted_data in extracted_datas:
            if upsert_count >= _DB_STEP_SIZE:
                airflow_logger.info(
                    f"reached commit limit, committing {_DB_STEP_SIZE} objects"
                )
                session.commit()
                upsert_count = 0

            extracted_data_service.delete_extracted_data_by_id(
                extracted_data_id=extracted_data.id
            )
            upsert_count += 1


@inject
@task()
def rename_extracted_data_paths(
    extracted_data_service: ExtractedDataService = Provide[
        Container.extracted_data_service
    ],
):
    """change extracted data paths

    Args:
        s3_key (str): [description]
        s3_bucket (str): [description]
        s3_secret (str): [description]
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    target_str = conf.get("target_str")
    rename_str = conf.get("rename_str")
    total = int(conf.get("total", 1000))
    if not target_str or not rename_str:
        raise Exception(
            "target_str and rename_str required: {} - {}".format(target_str, rename_str)
        )

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    # get extracted data with target_str
    extracted_datas = extracted_data_service.get_extracted_data_like_s3_path(
        target_str, total
    )

    airflow_logger.info(
        "Renaming {} extracted data entries file_path and s3_loc".format(
            len(extracted_datas)
        )
    )

    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        try:
            for extracted_data in extracted_datas:
                file_path = extracted_data.file_path
                s3_loc = extracted_data.s3_loc
                extracted_data_service.update_extracted_data(
                    extracted_data.id,
                    file_path.replace(target_str, rename_str),
                    s3_loc.replace(target_str, rename_str),
                )
        except:
            session.rollback()
            raise


@inject
@task()
def supplier_rollover(
    annotation_service: AnnotationService = Provide[Container.annotation_service],
    extracted_data_service: ExtractedDataService = Provide[
        Container.extracted_data_service
    ],
    scale_service: ScaleService = Provide[Container.scale_service],
    supplier_service: SupplierService = Provide[Container.supplier_service],
    taxonomy_service: TaxonomyService = Provide[Container.taxonomy_service],
):
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    scale_task_params = scale_service.get_scale_task_params()

    airflow_logger.info(f"got {len(scale_task_params)} scale_task_params")

    with Context().new_session_scope() as session:

        scale_tasks = scale_service.get_scale_tasks()
        airflow_logger.info(f"got {len(scale_tasks)} scale tasks")

        indexed_supplier_tasks = supplier_service.get_supplier_tasks()
        indexed_supplier_tasks_unique_ids = [
            indexed_supplier_task.unique_id
            for indexed_supplier_task in indexed_supplier_tasks
        ]

        work_rollover_count = 0
        work_limit_count = 0

        supplier_task_count = 0
        for scale_task in scale_tasks:
            if work_limit_count >= _DB_STEP_SIZE:
                work_limit_count = 0
                airflow_logger.info(
                    f"reached commit limit, commiting before next rollover total ingested: {work_rollover_count}"
                )
                session.commit()

            if scale_task.unique_id in indexed_supplier_tasks_unique_ids:
                continue

            cleaned_tag_str = attempt_clean_tag_str(scale_task.tags)
            supplier_task = SupplierTask(
                task_id=scale_task.task_id,
                task_type=scale_task.task_type,
                task_status=scale_task.task_status,
                project=scale_task.project,
                batch=scale_task.batch,
                unique_id=scale_task.unique_id,
                tags=cleaned_tag_str,
                data=scale_task.data,
                is_test=scale_task.is_test,
                ingested=scale_task.ingested,
                frame_count=scale_task.frame_count,
                supplier_task_param_id=None,
            )
            supplier_task.raw_annotations_file_path = (
                scale_task.raw_annotations_file_path
            )
            supplier_task.events_file_path = scale_task.events_file_path
            supplier_task.callback_file_path = scale_task.callback_file_path
            supplier_task.link_file_path = scale_task.link_file_path
            session.add(supplier_task)
            supplier_task_count += 1
            work_rollover_count += 1
            work_limit_count += 1

        session.commit()

        airflow_logger.info(f"rolled over {supplier_task_count} supplier tasks")

        airflow_logger.info(f"rolling over {len(scale_task_params)} task params")

        # param link
        supplier_task_param_count = 0
        for scale_task_param in scale_task_params:
            if work_limit_count >= _DB_STEP_SIZE:
                work_limit_count = 0
                airflow_logger.info(
                    f"reached commit limit, commiting before next rollover total ingested: {work_rollover_count}"
                )
                session.commit()
            scale_task_param_params: Dict = json.loads(scale_task_param.content)
            new_supplier_task_param: SupplierTaskParam = (
                supplier_service.add_supplier_task_param(
                    scale_task_param_params, CatAnnotationSupplierEnum.Scale
                )
            )
            match_scale_tasks: List[ScaleTask] = (
                scale_service.get_scale_tasks_by_param_id(scale_task_param.id)
            )
            match_scale_tasks_task_ids: List[str] = [
                match_scale_task.task_id for match_scale_task in match_scale_tasks
            ]
            match_supplier_tasks: List[SupplierTask] = (
                supplier_service.get_supplier_task_by_supplier_task_ids(
                    match_scale_tasks_task_ids
                )
            )
            for match_supplier_task in match_supplier_tasks:
                match_supplier_task.supplier_task_param_id = new_supplier_task_param.id
                session.add(match_supplier_task)
                work_rollover_count += 1
                work_limit_count += 1
            supplier_task_param_count += 1

        session.commit()

        airflow_logger.info(
            f"rolled over {supplier_task_param_count} supplier task params"
        )

        # make supplier id memo
        scale_task_task_ids = [scale_task.task_id for scale_task in scale_tasks]
        supplier_task_id_dict = get_indexed_supplier_task_obj_ids(
            scale_task_task_ids, supplier_service
        )

        airflow_logger.info(
            f"generated supplier id memo with {len(supplier_task_id_dict)} ids"
        )

        # annotation link
        link_annotation_count = 0
        for scale_task in scale_tasks:
            match_annotations: List[Annotation] = (
                annotation_service.get_annotations_by_supplier_task_id(scale_task.id)
            )
            match_supplier_id: int = supplier_task_id_dict.get(scale_task.task_id)
            for match_annotation in match_annotations:
                if work_limit_count >= _DB_STEP_SIZE:
                    work_limit_count = 0
                    airflow_logger.info(
                        f"reached commit limit, commiting before next rollover total ingested: {work_rollover_count}"
                    )
                    session.commit()
                match_annotation.supplier_task_id = match_supplier_id
                session.add(match_annotation)
                link_annotation_count += 1
                work_rollover_count += 1
                work_limit_count += 1

        session.commit()

        airflow_logger.info(f"rolled over {link_annotation_count} annotation links")

        # extracted data allocation link
        link_extracted_data_allocation_count = 0
        for scale_task in scale_tasks:
            match_extracted_data_allocations: List[ExtractedDataAllocationTask] = (
                extracted_data_service.get_extracted_data_allocation_tasks_by_supplier_task_id(
                    scale_task.id
                )
            )
            match_supplier_id: int = supplier_task_id_dict.get(scale_task.task_id)
            for match_extracted_data_allocation in match_extracted_data_allocations:
                if work_limit_count >= _DB_STEP_SIZE:
                    work_limit_count = 0
                    airflow_logger.info(
                        f"reached commit limit, commiting before next rollover total ingested: {work_rollover_count}"
                    )
                    session.commit()
                match_extracted_data_allocation.supplier_task_id = match_supplier_id
                session.add(match_extracted_data_allocation)
                link_extracted_data_allocation_count += 1
                work_rollover_count += 1
                work_limit_count += 1

        session.commit()

        airflow_logger.info(
            f"rolled over {link_extracted_data_allocation_count} extracted data allocation links"
        )

        # taxonomy allocation task link
        link_taxonomy_allocation_count = 0
        for scale_task in scale_tasks:
            match_taxonomy_allocations: List[TaxonomyAllocationTask] = (
                taxonomy_service.get_taxonomy_allocations_by_supplier_task_id(
                    scale_task.id
                )
            )
            match_supplier_id: int = supplier_task_id_dict.get(scale_task.task_id)
            for match_taxonomy_allocation in match_taxonomy_allocations:
                if work_limit_count >= _DB_STEP_SIZE:
                    work_limit_count = 0
                    airflow_logger.info(
                        f"reached commit limit, commiting before next rollover total ingested: {work_rollover_count}"
                    )
                    session.commit()
                match_taxonomy_allocation.supplier_task_id = match_supplier_id
                session.add(match_taxonomy_allocation)
                link_taxonomy_allocation_count += 1
                work_rollover_count += 1
                work_limit_count += 1

        session.commit()

        airflow_logger.info(
            f"rolled over {link_taxonomy_allocation_count} taxonomy allocation links"
        )


@inject
@task()
def swap_extracted_data_table_to_hlog(
    preset_prefix_path: str = "",
    extracted_data_service: ExtractedDataService = Provide[
        Container.extracted_data_service
    ],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
):
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path
    log_level_debug: bool = conf.get("log_level_debug", False)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    with Context().new_session_scope() as session:
        extracted_datas = extracted_data_service.get_extracted_data_like_s3_path(
            prefix_path, max_return=1000000
        )

        airflow_logger.info(f"got {len(extracted_datas)} extracted datas")

        upsert_count: int = 0

        for extracted_data in extracted_datas:

            if upsert_count >= _DB_STEP_SIZE:
                airflow_logger.info(
                    f"reached commit limit, committing {_DB_STEP_SIZE} objects"
                )
                session.commit()
                upsert_count = 0

            raw_data_id = extracted_data.raw_data_id
            if not raw_data_id:
                airflow_logger.debug(
                    f"skipping extracted data {extracted_data.id}, no raw data id"
                )
                continue
            raw_data = raw_data_service.get_raw_data_by_id(raw_data_id)

            if not raw_data:
                airflow_logger.warning(f"could not get raw data with id {raw_data_id}")
                continue

            airflow_logger.debug(f"got {raw_data_id} and {raw_data.id}")

            try:
                if (
                    raw_data.raw_data_type == RawLogTypeEnum.hlog_complete_log
                    or raw_data.raw_data_type == RawLogTypeEnum.hlog_autosnap
                    or raw_data.raw_data_type == RawLogTypeEnum.hlog_snapshot
                    or raw_data.raw_data_type == RawLogTypeEnum.hlog_amendment
                    or raw_data.raw_data_type == RawLogTypeEnum.hlog_snippet
                ):
                    # check for hlog exist
                    base_log_or_snippet_name = (
                        get_base_log_or_snippet_name_from_s3_path(extracted_data.s3_loc)
                    )
                    raw_data_hlog = (
                        raw_data_service.get_raw_data_by_file_path_like_op_hlog_only(
                            base_log_or_snippet_name
                        )
                    )
                    if raw_data_hlog.id != raw_data.id:
                        extracted_data.raw_data_id = raw_data_hlog.id
                        extracted_data.upsert()
                        upsert_count += 1
            except AttributeError as e:
                airflow_logger.error(f"got {raw_data_id} and {raw_data}")
                raise AttributeError(e)
@inject
@task()
def create_default_dataset(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    dataset_service: DatasetService = Provide[Container.dataset_service],
    dataset_association_service: DatasetAssociationService = Provide[
        Container.dataset_association_service
    ],
):
    """Create default dataset for newly ingested data from data collection activity"""
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    dataset_name: str = conf.get("dataset_name", "")
    complete_raw_log_ids: List[int] = conf.get("complete_raw_log_ids", [])
    snapshots_raw_log_ids: List[int] = conf.get("snapshots_raw_log_ids", [])
    dataset_name_complete = dataset_name + "_complete"
    dataset_name_snapshots = dataset_name + "_snapshots"
    dataset_complete_description = "Complete logs from " + dataset_name_complete.replace("_", " ")
    dataset_snapshots_description = "Snapshots from " + dataset_name_snapshots.replace("_", " ")

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    airflow_logger.info(f"Creating default dataset_name_complete {dataset_name_complete}")
    airflow_logger.info(f"Creating default dataset_name_snapshots {dataset_name_snapshots}")


    with Context().new_session_scope() as session:
        dataset_complete, dataset_snapshot = dataset_service.check_dataset_availability(dataset_name_complete, dataset_name_snapshots)
        airflow_logger.info(f"complete and snapshots value {dataset_complete, dataset_snapshot}")
        if dataset_complete != -1 and dataset_snapshot != -1:
            airflow_logger.info("exiting dataset creation as dataset names provided are already available")
        else:
            airflow_logger.info(f"No dataset available during the check, Hence creating the dataset {dataset_name_complete, dataset_name_snapshots}")
            dataset_complete = dataset_service.create_dataset(dataset_name_complete, dataset_complete_description)
            dataset_snapshot = dataset_service.create_dataset(dataset_name_snapshots, dataset_snapshots_description)
            airflow_logger.info(f"id of dataset created for complete logs is {dataset_complete}")
            airflow_logger.info(f"id of dataset created for snapshots is {dataset_snapshot}")
        
        result_complete_logs = dataset_association_service.check_raw_data_association_to_dataset(tuple(complete_raw_log_ids), dataset_complete)
        result_snapshots = dataset_association_service.check_raw_data_association_to_dataset(tuple(snapshots_raw_log_ids), dataset_snapshot)
        if len(result_complete_logs) <= 0:
            airflow_logger.info("No existing associations found for the provided complete log ids; proceeding to create associations.")
        else:
            result_complete_logs = [item[0] for item in result_complete_logs]
            result_set = set(result_complete_logs)
            complete_raw_log_ids = [id for id in complete_raw_log_ids if id not in result_set]
            airflow_logger.info(f"Associations already exist for some complete log ids; will establish associations for ids: {complete_raw_log_ids}")
        
        
        if len(result_snapshots) <= 0:
            airflow_logger.info("No existing associations found for the provided snapshot ids; proceeding to create associations")
        else:
            result_snapshots = [item[0] for item in result_snapshots]
            result_set = set(result_snapshots)
            snapshots_raw_log_ids = [id for id in snapshots_raw_log_ids if id not in result_set]
            airflow_logger.info(f"Associations already exist for some snapshot log ids; will establish associations for: {snapshots_raw_log_ids}")
            
        
        ## Establish Association
        if len(complete_raw_log_ids) <= 0:
            airflow_logger.info("No log id to create association")
        else:
            result_complete_logs_association = dataset_association_service.associate_raw_logs_to_dataset(
                tuple(complete_raw_log_ids), dataset_complete
            )
            airflow_logger.info(f"The list of complete raw logs associated with dataset {result_complete_logs_association, dataset_complete}")
        

        if len(snapshots_raw_log_ids) <= 0:
            airflow_logger.info("No log id to create association")
        else:
            result_snapshots_association = dataset_association_service.associate_raw_logs_to_dataset(
                tuple(snapshots_raw_log_ids), dataset_snapshot
            )
            airflow_logger.info(f"The list of snapshots associated with dataset {result_snapshots_association, dataset_snapshot}")
        
        session.commit()
###############################################################################
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################
# Copyright (C) Caterpillar Inc. All Rights Reserved.
# Caterpillar: Confidential Yellow
###############################################################################

from __future__ import annotations

import concurrent.futures
import datetime as dt
import enum
import json
import logging
import math
import ntpath
import operator
import os
import time
import re

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.batch_client import AwsBatchClientHook
from airflow.utils.trigger_rule import TriggerRule
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from dependency_injector.wiring import Provide, inject
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect

from db.container import Container
from db.context import Context

from db.models import (
    AssociateCfh1AData,
    AnalyticsReports,
    AnalyticsReportsDetails,
    CalData,
    ExtractedData,
    IntegrityCheckReport,
    IntegrityCheckReportLatencyIssue,
    IntegrityCheckReportMissingChannel,
    IntegrityCheckReportNotCalibratedSensor,
    IntegrityCheckReportPointCloudOccludedChannel,
    IntegrityCheckReportSequenceOutOfOrderChannel,
    IntegrityCheckReportTFDynamicTFStaticMismatch,
    IntegrityCheckReportTransmissionTimeDelay,
    MergedData,
    PointCloudDataEnum,
    PTag,
    RawData,
    RawDataStaging,
    SceneFrame,
    ZipData
)

from db.services.analytics_reports_service import AnalyticsReportsService
from db.services.cal_data_service import CalDataService
from db.services.extracted_data_service import ExtractedDataService
from db.services.hlog_tag_service import HlogTagService
from db.services.integrity_check_report_service import IntegrityCheckReportService
from db.services.ptag_service import PTagService
from db.services.raw_data_service import RawDataService
from db.services.raw_data_staging_service import RawDataStagingService
from db.services.scene_frame_service import SceneFrameService
from db.services.zip_data_service import ZipDataService
from db.services.associate_cfh_1a_data_service import AssociateCfh1ADataService
from db.services.merge_cfh_1a_data_service import MergeCfh1ADataService
from pipeline.utilities.data_ingest import DataIngestContext
from pipeline.utilities.integrity_report import (
    _INTEGRITY_REPORT_VERSION_1_0_0, _INTEGRITY_REPORT_VERSION_1_0_2,
    _INTEGRITY_REPORT_VERSION_1_1_0, _INTEGRITY_REPORT_VERSION_1_2_0)

from utilities.analytics_utilities import (
    calculate_s3_prefix_size_gb, create_batch_client_v2, create_fsx_client,
    create_fsx_filesystem, create_fsx_filesystem_v2, create_sts_client, 
    delete_fsx_filesystem, get_batch_job_name, poll_aws_job_status, 
    poll_job_status_to_release_data, run_cfh_1a_merge_job_in_batch,
    run_extraction_pipeline_job_in_batch, run_hltag_to_json_job_in_batch,
    run_log_integrity_report_generation_job_in_batch, run_unpack_priority_snapshots_job_in_batch,
    run_unpacking_pipeline_job_in_batch,
    run_unzip_job_in_batch, WorkflowType
)

from utilities.aws import (convert_json_to_valid, create_s3_client,
                           create_s3_resource, flatten_tree_for_batch,
                           get_archive_destination_from_s3_key,
                           get_base_log_or_snippet_name_from_s3_path,
                           get_base_log_path_from_data_path,
                           get_common_staging_prefixes,
                           get_extraction_index_fullpath, get_file_paths,
                           get_hltag_objects_from_json,
                           get_index_hltag_json_paths, get_index_json_paths,
                           get_matched_pairs_from_list, get_merged_log_dict, get_merged_snapshot_name,
                           get_raw_and_cal_log_dicts,
                           get_s3_url_from_bucket_and_prefix, get_subtag_dict,
                           get_subtag_json, get_subtag_key_from_path,
                           get_unpacked_prefix_from_ptag, get_zip_log_dict,
                           group_s3_paths_by_hierarchy_tree,
                           is_json_file_in_s3_ptag, is_json_file_in_s3_valid,
                           json_file_in_s3_to_dict, split_s3url_to_bucket_key,
                           unzip_files_by_streaming, upload_files_to_s3)
from utilities.log_constants import _DEFAULT_LOGGER_NAME
from utilities.project_constants import _CFH_1A_MERGE_BATCH_SIZE, _DB_STEP_SIZE, _TRANSMISSION_INDEX_KEY
from utilities.db import (get_archive_ready_raw_data_staging_paths,
                          get_hierarchical_ptag_paths,
                          get_indexed_cal_data_paths,
                          get_indexed_extracted_data_s3_locs,
                          get_indexed_hlog_tag_paths,
                          get_indexed_integrity_check_report_data_paths,
                          get_indexed_ptag_paths, get_indexed_raw_data_paths,
                          get_indexed_raw_data_staging_paths,
                          get_indexed_zip_data_paths,
                          get_or_generate_ptag_db_obj,
                          get_unpack_ready_raw_data_staging_paths,
                          update_raw_data_staging_table_with_unpacked)
from utilities.models import UnpackPriorityBatchJob
from utilities.validation import get_dict_md5


class CatDataIngestType(enum.Enum):
    PtagDataIngest = "ptag"
    RawLogDataIngest = "raw_log"
    CalLogDataIngest = "cal_log"
    SensorDataIngest = "sensor_data"
    SceneFrameDataIngest = "scene_frame"
    SummaryDataIngest = "summary"


@inject
def get_final_subtag_json(s3_client, ptag_db_objs: List[PTag], log_path: str, subtag_list: List[str]):
    try:
        # get subtag json
        subtag_json = None
        child_slice_index = log_path.rfind("/")
        child_dir = log_path[:child_slice_index]

        subtags = []
        if subtag_list is not None:
            for subtag_path in subtag_list:
                subtag_slice_index = subtag_path.rfind("/")
                subtag_path_dir = subtag_path[:subtag_slice_index]

                if subtag_path_dir in child_dir:
                    subtags.append(subtag_path)

        if subtags is not None:
            subtag_json = get_subtag_json(s3_client, subtags)

        # compose final tag json
        additional_info_dict = {}
        final_tag_json = {}
        if ptag_db_objs is not None:
            ptag_db_objs.sort(key=operator.attrgetter("path"))
            for ptag_db_obj in ptag_db_objs:
                ptag_json = json.loads(ptag_db_obj.content)
                if ptag_json.get("additional_info"):
                    additional_info_dict.update(ptag_json.get("additional_info"))
                final_tag_json.update(json.loads(ptag_db_obj.content))
        if subtag_json is not None:
            if subtag_json.get("additional_info"):
                additional_info_dict.update(subtag_json.get("additional_info"))
            final_tag_json.update(subtag_json)

        final_tag_json["additional_info"] = additional_info_dict

        return final_tag_json
    except AttributeError as e:
        print("error: {} - {} - {} - {}".format(ptag_db_objs, log_path, subtag_list, e))
        raise


# TODO this needs its own dag, it is not being used anywhere
@inject
@task()
def rename_extracted_data_paths(
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    """change extracted data paths

    Args:

    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    target_str = conf.get("target_str")
    rename_str = conf.get("rename_str")
    total = int(conf.get("total", 1000))
    if not target_str or not rename_str:
        raise Exception("target_str and rename_str required: {} - {}".format(target_str, rename_str))

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    # get extracted data with target_str
    extracted_datas = extracted_data_service.get_extracted_data_like_s3_path(target_str, total)

    airflow_logger.info("Renaming {} extracted data entries file_path and s3_loc".format(len(extracted_datas)))

    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        try:
            for extracted_data in extracted_datas:
                file_path = extracted_data.file_path
                s3_loc = extracted_data.s3_loc
                extracted_data_service.update_extracted_data(
                    extracted_data.id,
                    file_path.replace(target_str, rename_str),
                    s3_loc.replace(target_str, rename_str),
                )
        except:
            session.rollback()
            raise


@inject
def ingest_cal_and_raw_logs(
    ingest_type: CatDataIngestType,
    data_ingest_ctx: DataIngestContext,
    ptag_memo_dict: Dict,
    log_dict: Dict,
    indexed_log_paths: List[str],
    indexed_log_update_candidates: Dict,
    subtag_dict: Dict,
    skip_index: bool = False,
):
    session = data_ingest_ctx._session
    airflow_logger = data_ingest_ctx._logger

    if not (ingest_type == CatDataIngestType.CalLogDataIngest or ingest_type == CatDataIngestType.RawLogDataIngest):
        raise ValueError(f"got unsupported CatDataIngestType {ingest_type}")
    if skip_index:
        return 0
    if not session:
        airflow_logger.warning(f"got no session for {ingest_type.value}")
        session = Context().get_session()

    airflow_logger.info(f"start {ingest_type.value} ingest")

    airflow_logger.debug(f"log_dict is {log_dict}")

    s3_client = data_ingest_ctx._s3_client
    cal_data_service: CalDataService = data_ingest_ctx._cal_data_service
    raw_data_service: RawDataService = data_ingest_ctx._raw_data_service
    ptag_service: PTagService = data_ingest_ctx._ptag_service
    test_run: bool = data_ingest_ctx._test_run

    assert s3_client is not None
    assert cal_data_service is not None
    assert raw_data_service is not None
    assert ptag_service is not None

    log_ingest_count = 0
    log_limit_count = 0
    log_skip_count = 0
    for log_name in log_dict:
        for log in log_dict.get(log_name):
            log_path = log.get("log_path")
            ptag_path = log.get("ptag_path")
            airflow_logger.debug(f"log path: {log_path}")
            airflow_logger.debug(f"ptag path: {ptag_path}")
            if log_path in indexed_log_paths:
                airflow_logger.debug(f"skipped {ingest_type.value}: {log_path}")
                log_skip_count += 1
                continue

            if log_limit_count >= _DB_STEP_SIZE:
                log_limit_count = 0
                airflow_logger.info(
                    f"reached commit limit, commiting before next data ingest total ingested: {log_ingest_count}"
                )
                session.commit()

            # get ptag
            ptag_db_obj_list = []
            ptag_id: Optional[int] = None
            if ptag_path is not None:
                airflow_logger.debug(f"checking for {ptag_path}")
                child_ptag_db_obj = get_or_generate_ptag_db_obj(
                    s3_client, ptag_path, ptag_memo_dict, session, test_run, ptag_service
                )
                if child_ptag_db_obj:
                    ptag_id = child_ptag_db_obj.id
                    ptag_db_obj_list.append(child_ptag_db_obj)
                    parent_ptag_paths = get_hierarchical_ptag_paths(ptag_path, ptag_service)
                    airflow_logger.debug(f"parent_ptag_paths: {parent_ptag_paths}")
                    for parent_ptag_path in parent_ptag_paths:
                        ptag_db_obj = get_or_generate_ptag_db_obj(
                            s3_client, parent_ptag_path, ptag_memo_dict, session, test_run, ptag_service
                        )
                        if not ptag_db_obj:
                            continue
                        ptag_db_obj_list.append(ptag_db_obj)

            # compose final tag json
            final_tag_json = get_final_subtag_json(
                s3_client, ptag_db_obj_list, log_path, subtag_dict.get(get_subtag_key_from_path(log_path))
            )

            if ingest_type is CatDataIngestType.RawLogDataIngest:
                # index HLOG or Rosbag, if already exists but not fully indexed, pull from db and update anything new
                log_data = raw_data_service.get_raw_data_by_file_path(log_path)
            elif ingest_type is CatDataIngestType.CalLogDataIngest:
                # index cal log, if already exists but not fully indexed, pull from db and update anything new
                log_data = cal_data_service.get_cal_data_by_file_path(log_path)
            else:
                raise ValueError(f"got unsupported CatDataIngestType {ingest_type} during check")

            indexed_log_update_candidates_key_list = list(indexed_log_update_candidates.keys())

            if len(indexed_log_update_candidates) <= 5:
                airflow_logger.debug(f"indexed log update candidates: {indexed_log_update_candidates}")
                airflow_logger.debug(f"keys: {indexed_log_update_candidates_key_list}")

            # if does not exist, make new
            is_update = False
            skip_session_add = False
            if log_path in indexed_log_update_candidates_key_list:
                airflow_logger.debug(f"checking update candidate {log_path}")
                is_update = True
                log_data = indexed_log_update_candidates.get(log_path)
                ingest_log_name = "unknown ingest_log_name"
                if ingest_type is CatDataIngestType.RawLogDataIngest:
                    ingest_log_name = log_data.base_log
                if ingest_type is CatDataIngestType.CalLogDataIngest:
                    ingest_log_name = log_data.cal_base_log
                if log_data.ptag_id == ptag_id and ingest_log_name == log_name and log_data.tag_json == final_tag_json:
                    if ingest_type is CatDataIngestType.RawLogDataIngest and log_data.raw_data_type == log["log_type"]:
                        airflow_logger.debug(f"skipping session add for {log_path}, no update")
                        skip_session_add = True
                    if ingest_type is CatDataIngestType.CalLogDataIngest:
                        airflow_logger.debug(f"skipping session add for {log_path}, no update")
                        skip_session_add = True
                else:
                    log_data = session.merge(log_data)
                    if ingest_type is CatDataIngestType.RawLogDataIngest:
                        log_data.frames = 0
                        log_data.base_log = log_name
                        log_data.raw_data_type = log["log_type"]
                    if ingest_type is CatDataIngestType.CalLogDataIngest:
                        log_data.method = "Test_cal_method"
                        log_data.cal_base_log = log_name
                    log_data.ptag_id = ptag_id
                    log_data.tag_json = final_tag_json
            else:
                airflow_logger.debug(f"{log_path} not in update candidates")
                if not log_data and ingest_type is CatDataIngestType.RawLogDataIngest:
                    log_data = RawData(
                        log_path,
                        log_path,
                        0,
                        ptag_id,
                        base_log=log_name,
                        raw_data_type=log["log_type"],
                        tag_json=final_tag_json,
                    )
                if not log_data and ingest_type is CatDataIngestType.CalLogDataIngest:
                    log_data = CalData(
                        log_path, "Test_cal_method", ptag_id, cal_base_log=log_name, tag_json=final_tag_json
                    )

            if not skip_session_add and not test_run:
                try:
                    session.add(log_data)
                except sa_exc.InvalidRequestError as e:
                    airflow_logger.error(e)
                    insp = inspect(log_data)
                    airflow_logger.error(f"function session obj {session}, object session obj {insp.session}")
                    airflow_logger.error(f"object session id {log_data._sa_instance_state.session_id}")
                    raise sa_exc.InvalidRequestError(e)
                log_ingest_count += 1
                log_limit_count += 1

            airflow_logger.debug(f"is_update: {is_update} indexed: {log} with ptag_id: {ptag_id}")

    session.commit()

    airflow_logger.info(f"finish {ingest_type.value} with {log_skip_count} skipped, {log_ingest_count} ingested")


@inject
def ingest_zip_data(
    data_ingest_ctx: DataIngestContext,
    ptag_memo_dict: Dict,
    zip_log_dict: Dict,
    indexed_zip_data_paths: List[str],
    indexed_zip_data_update_candidates: Dict,
    subtag_dict: Dict,
    skip_index: bool = False,
):
    """
    Ingests zip data from S3 into the database.

    Args:
        data_ingest_ctx (DataIngestContext): Context object containing session, logger, and services.
        ptag_memo_dict (Dict): Memoization dictionary for PTag objects.
        zip_log_dict (Dict): Dictionary containing zip log paths and their metadata.
        indexed_zip_data_paths (List[str]): List of already indexed zip data paths.
        indexed_zip_data_update_candidates (Dict): Dictionary of zip data paths that need updates.
        subtag_dict (Dict): Dictionary of subtags for the zip data.
        skip_index (bool): Flag to skip indexing if set to True.

    Returns:
        None
    """
    session = data_ingest_ctx._session
    airflow_logger = data_ingest_ctx._logger

    if skip_index:
        return 0
    if not session:
        airflow_logger.warning("got no session for zip_data ingest")
        session = Context().get_session()

    airflow_logger.info("start zip_data ingest")
    airflow_logger.info(f"zip_log_dict: {zip_log_dict}")

    s3_client = data_ingest_ctx._s3_client
    ptag_service: PTagService = data_ingest_ctx._ptag_service
    zip_data_service: ZipDataService = data_ingest_ctx._zip_data_service
    test_run: bool = data_ingest_ctx._test_run

    if ptag_memo_dict is None:
        ptag_memo_dict = {}

    assert s3_client is not None
    assert ptag_service is not None
    assert zip_data_service is not None

    log_ingest_count = 0
    log_limit_count = 0
    log_skip_count = 0
    for log_name in zip_log_dict:
        for log in zip_log_dict.get(log_name):
            log_path = log.get("log_path")
            ptag_path = log.get("ptag_path")
            airflow_logger.debug(f"log path: {log_path}")
            airflow_logger.debug(f"ptag path: {ptag_path}")
            if log_path in indexed_zip_data_paths:
                airflow_logger.debug(f"skipped zip_data: {log_path}")
                log_skip_count += 1
                continue

            if log_limit_count >= _DB_STEP_SIZE:
                log_limit_count = 0
                airflow_logger.info(
                    f"reached commit limit, commiting before next data ingest total ingested: {log_ingest_count}"
                )
                session.commit()

            # get ptag
            ptag_db_obj_list = []
            ptag_id: Optional[int] = None
            if ptag_path is not None:
                airflow_logger.debug(f"checking for {ptag_path}")
                child_ptag_db_obj = get_or_generate_ptag_db_obj(
                    s3_client, ptag_path, ptag_memo_dict, session, test_run, ptag_service
                )
                if child_ptag_db_obj:
                    ptag_id = child_ptag_db_obj.id
                    ptag_db_obj_list.append(child_ptag_db_obj)
                    parent_ptag_paths = get_hierarchical_ptag_paths(ptag_path, ptag_service)
                    airflow_logger.debug(f"parent_ptag_paths: {parent_ptag_paths}")
                    for parent_ptag_path in parent_ptag_paths:
                        ptag_db_obj = get_or_generate_ptag_db_obj(
                            s3_client, parent_ptag_path, ptag_memo_dict, session, test_run, ptag_service
                        )
                        if not ptag_db_obj:
                            continue
                        ptag_db_obj_list.append(ptag_db_obj)

            # compose final tag json
            final_tag_json = get_final_subtag_json(
                s3_client, ptag_db_obj_list, log_path, subtag_dict.get(get_subtag_key_from_path(log_path))
            )

            # Only index zip_data
            log_data = zip_data_service.get_zip_data_by_file_path(log_path)

            is_update = False
            skip_session_add = False
            if log_path in indexed_zip_data_update_candidates:
                airflow_logger.debug(f"checking update candidate {log_path}")
                is_update = True
                log_data = indexed_zip_data_update_candidates.get(log_path)
                ingest_log_name = getattr(log_data, "zip_base_log", log_path)
                if (
                    log_data.ptag_id == ptag_id
                    and ingest_log_name == log_path
                    and getattr(log_data, "tag_json", None) == final_tag_json
                ):
                    airflow_logger.debug(f"skipping session add for {log_path}, no update")
                    skip_session_add = True
                else:
                    log_data = session.merge(log_data)
                    log_data.file_path = log_path
                    log_data.ptag_id = ptag_id
                    if hasattr(log_data, "tag_json"):
                        log_data.tag_json = final_tag_json
            else:
                airflow_logger.debug(f"{log_path} not in update candidates")
                if not log_data:
                    log_data = ZipData(log_path, unzipped_file_location=None, ptag_id=ptag_id)

            if not skip_session_add and not test_run:
                try:
                    session.add(log_data)
                except sa_exc.InvalidRequestError as e:
                    airflow_logger.error(e)
                    insp = inspect(log_data)
                    airflow_logger.error(f"function session obj {session}, object session obj {insp.session}")
                    airflow_logger.error(f"object session id {log_data._sa_instance_state.session_id}")
                    raise sa_exc.InvalidRequestError(e)
                log_ingest_count += 1
                log_limit_count += 1

            airflow_logger.debug(f"is_update: {is_update} indexed: {log_path} with ptag_id: {ptag_id}")

    session.commit()
    airflow_logger.info(f"finish zip_data with {log_skip_count} skipped, {log_ingest_count} ingested")


def ingest_sensor_data_and_scene_frame_data(
    ingest_type: CatDataIngestType,
    data_ingest_ctx: DataIngestContext,
    data_file_paths: List[str],
    indexed_data_dict: Dict,
    indexed_data_update_candidates: Dict,
    skip_index: bool = False,
    skip_linking: bool = False,
):
    session = data_ingest_ctx._session
    airflow_logger = data_ingest_ctx._logger

    if not (ingest_type == CatDataIngestType.SensorDataIngest or ingest_type == CatDataIngestType.SceneFrameDataIngest):
        raise ValueError(f"got unsupported CatDataIngestType {ingest_type}")
    if skip_index:
        return 0
    if not session:
        session = Context().get_session()

    airflow_logger.info(f"start {ingest_type.value} ingest")

    s3_client = data_ingest_ctx._s3_client
    cal_data_service: CalDataService = data_ingest_ctx._cal_data_service
    extracted_data_service: ExtractedDataService = data_ingest_ctx._extraced_data_service
    ptag_service: PTagService = data_ingest_ctx._ptag_service
    raw_data_service: RawDataService = data_ingest_ctx._raw_data_service
    test_run: bool = data_ingest_ctx._test_run

    assert s3_client is not None
    assert cal_data_service is not None
    assert extracted_data_service is not None
    assert ptag_service is not None
    assert raw_data_service is not None

    data_ingest_count = 0
    data_limit_count = 0
    data_skip_count = 0
    for data_file_path in data_file_paths:
        if ingest_type == CatDataIngestType.SensorDataIngest and data_file_path in indexed_data_dict:
            # skip already indexed file paths
            airflow_logger.debug(f"skipped file path: {data_file_path}")
            data_skip_count += 1
            continue

        elif ingest_type == CatDataIngestType.SceneFrameDataIngest and data_file_path in indexed_data_dict:
            # skip already indexed file paths
            airflow_logger.debug(f"skipped file path: {data_file_path}")
            data_skip_count += 1
            continue

        if data_limit_count >= _DB_STEP_SIZE:
            data_limit_count = 0
            airflow_logger.info(
                f"reached commit limit, commiting before next data ingest total ingested: {data_ingest_count}"
            )
            session.commit()

        # get ptag, raw data, and cal data
        ptag_db_obj: Optional[PTag] = None
        raw_data: Optional[RawData] = None
        cal_data: Optional[CalData] = None

        ptag_id: Optional[int] = None
        raw_data_id: Optional[int] = None
        cal_data_id: Optional[int] = None

        log_or_snippet_name = get_base_log_or_snippet_name_from_s3_path(data_file_path)
        if log_or_snippet_name:
            ptag_db_obj = ptag_service.get_ptag_by_base_log_or_snippet_name(log_or_snippet_name)
            raw_data = raw_data_service.get_raw_data_by_file_path_like_op_hlog_only(log_or_snippet_name)
            cal_data = cal_data_service.get_cal_data_by_file_path_like_op_hlog_prefer(log_or_snippet_name)
            if ptag_db_obj is not None:
                ptag_id = ptag_db_obj.id
            if raw_data is not None:
                raw_data_id = raw_data.id
            if cal_data is not None:
                cal_data_id = cal_data.id

        is_update = False
        skip_session_add = False
        if data_file_path in indexed_data_update_candidates:
            is_update = True
            extracted_data: ExtractedData = indexed_data_update_candidates.get(data_file_path)
            if (
                extracted_data.raw_data_id == raw_data_id
                and extracted_data.cal_data_id == cal_data_id
                and extracted_data.ptag_id == ptag_id
            ):
                airflow_logger.debug(f"skipping session add for {data_file_path}, no update")
                skip_session_add = True
            else:
                extracted_data = session.merge(extracted_data)
                extracted_data.raw_data_id = raw_data_id
                extracted_data.cal_data_id = cal_data_id
                extracted_data.ptag_id = ptag_id
        else:
            extracted_data: ExtractedData = ExtractedData(
                data_file_path, data_file_path, raw_data_id, cal_data_id, ptag_id
            )

        if not skip_session_add and not test_run:
            try:
                session.add(extracted_data)
            except sa_exc.InvalidRequestError as e:
                airflow_logger.error(e)
                insp = inspect(extracted_data)
                airflow_logger.error(f"function session obj {session}, object session obj {insp.session}")
                airflow_logger.error(f"object session id {extracted_data._sa_instance_state.session_id}")
                raise sa_exc.InvalidRequestError(e)
            data_ingest_count += 1
            data_limit_count += 1

        airflow_logger.debug(
            f"is_update: {is_update} indexed: {data_file_path} raw data id: {raw_data_id} cal data id: {cal_data_id} ptag id: {ptag_id}"
        )

        if skip_linking or ingest_type == CatDataIngestType.SensorDataIngest:
            continue

        # commit so that linking the scene frame works properly
        session.commit()

        # link pcd to scene frame

        # link image scene frames
        scene_frame_bucket, scene_frame_key = split_s3url_to_bucket_key(data_file_path)
        json = json_file_in_s3_to_dict(s3_client, scene_frame_bucket, scene_frame_key)
        if not json:
            raise ValueError(f"could not get json scene_frame_key: {scene_frame_key}")
        images = json.get("images")
        if not images:
            airflow_logger.warning(f"could not find images in scene frame at: {data_file_path}")
            continue
        for image in images:
            image_s3_path = image.get("image_url")
            if not image_s3_path:
                airflow_logger.warning(f"could not link image {image_s3_path} to scene frame at: {data_file_path}")
                continue
            image_extracted_data = extracted_data_service.get_extracted_data_by_s3_path(image_s3_path)
            if not image_extracted_data:
                airflow_logger.warning(
                    f"could not link image {image_s3_path} to scene frame at: {data_file_path} not found in table"
                )
                continue
            image_scene_frame = SceneFrame(extracted_data.id, image_extracted_data.id)

            if not test_run:
                session.add(image_scene_frame)

            airflow_logger.debug(
                f"is_update: {is_update} linked: {data_file_path} raw data id: {raw_data_id} cal data id: {cal_data_id} ptag id: {ptag_id}"
            )

    airflow_logger.info(f"finish {ingest_type.value} with {data_skip_count} skipped, {data_ingest_count} ingested")


@inject
@task()
def clean_annotations_in_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    preset_prefix_path: str = "",
):
    """clean all annotations in s3

    Args:
        s3_key (str): [description]
        s3_bucket (str): [description]
        s3_secret (str): [description]
        preset_prefix_path (str, optional): prefix path to filter extracted data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path

    # get all paths from s3
    s3_client = create_s3_client(s3_key, s3_secret)
    file_paths = get_file_paths(s3_client, s3_bucket, prefix_path, set(["json"]))

    for file_path in file_paths:
        airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
        airflow_logger.info("Processing: {}".format(file_path))
        bucket, file_key = split_s3url_to_bucket_key(file_path)
        # could not json loads, correct json to double quotes
        if not is_json_file_in_s3_valid(s3_client, s3_bucket, file_key):
            convert_json_to_valid(s3_client, s3_bucket, file_key)


@inject
@task()
def crawl_subtags_by_buckets(
    s3_key: str,
    s3_secret: str,
    preset_prefix_paths: List[str] = [],
    project_buckets: List[str] = [],
    ptag_service: PTagService = Provide[Container.ptag_service],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    cal_data_service: CalDataService = Provide[Container.cal_data_service],
):
    """crawl subtags for raw and cal data"""
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_paths = conf.get("prefix_paths", [])
    if prefix_paths == []:
        prefix_paths = preset_prefix_paths
    test_run: bool = conf.get("test_run", False)
    blacklist_pattern = conf.get("blacklist_pattern", "")

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    s3_client = create_s3_client(s3_key, s3_secret)

    # while loop starts here

    airflow_logger.info(f"{project_buckets} with prefixes: {prefix_paths}")

    for project_bucket in project_buckets:
        for prefix_path in prefix_paths:
            airflow_logger.info(f"processing bucket: {project_bucket} with prefix: {prefix_path}")

            json_paths = get_file_paths(s3_client, project_bucket, prefix_path, set(["json"]), blacklist_pattern)
            ptag_paths, subtag_paths, _, _ = get_index_json_paths(s3_client, project_bucket, json_paths, test_run)

            airflow_logger.info(f"ptag paths found: {len(ptag_paths)}")
            airflow_logger.info(f"subtag paths found: {len(subtag_paths)}")

            # get indexed ptags already in cdcs
            _, indexed_ptag_memo_dict = get_indexed_ptag_paths(ptag_paths, ptag_service)

            # get indexed raw data paths already in cdcs
            indexed_raw_data_paths = raw_data_service.get_all_raw_data_paths(project_bucket, prefix_path)
            airflow_logger.info(f"indexed raw log paths len: {len(indexed_raw_data_paths)}")

            # get indexed cal logs already in cdcs
            indexed_cal_data_paths = cal_data_service.get_all_cal_data_paths(project_bucket, prefix_path)
            airflow_logger.info(f"indexed cal log paths len: {len(indexed_cal_data_paths)}")

            raw_log_dict, cal_log_dict, cal_data_list, raw_data_list = get_raw_and_cal_log_dicts(
                indexed_raw_data_paths + indexed_cal_data_paths, ptag_paths
            )
            airflow_logger.info("raw logs found: {} - {}".format(len(raw_log_dict), len(raw_data_list)))
            airflow_logger.info("cal logs found: {} - {}".format(len(cal_log_dict), len(cal_data_list)))

            subtag_dict = get_subtag_dict(subtag_paths)
            airflow_logger.info("subtags found: {}".format(len(subtag_dict)))

            with Context().new_session_scope() as session:
                # open up a new session because we want to commit(persist) the data to the database for every file path
                # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)

                # update raw logs
                for base_log_name in raw_log_dict:
                    for raw_log in raw_log_dict[base_log_name]:

                        # get ptags
                        ptag_db_obj_list = []
                        if raw_log["ptag_path"] is not None:
                            child_ptag_db_obj = get_or_generate_ptag_db_obj(
                                s3_client, raw_log["ptag_path"], indexed_ptag_memo_dict, session, test_run, ptag_service
                            )
                            if child_ptag_db_obj:
                                ptag_db_obj_list.append(child_ptag_db_obj)
                                parent_ptag_paths = get_hierarchical_ptag_paths(raw_log["ptag_path"], ptag_service)
                                for parent_ptag_path in parent_ptag_paths:
                                    ptag_db_obj = get_or_generate_ptag_db_obj(
                                        s3_client,
                                        parent_ptag_path,
                                        indexed_ptag_memo_dict,
                                        session,
                                        test_run,
                                        ptag_service,
                                    )
                                    if not ptag_db_obj:
                                        continue
                                    ptag_db_obj_list.append(ptag_db_obj)

                        # compose final tag json
                        final_tag_json = get_final_subtag_json(
                            s3_client,
                            ptag_db_obj_list,
                            raw_log["raw_log_path"],
                            subtag_dict.get(get_subtag_key_from_path(raw_log["raw_log_path"])),
                        )

                        log_data = raw_data_service.get_raw_data_by_file_path(raw_log["raw_log_path"])
                        log_data.tag_json = final_tag_json

                        session.add(log_data)

                # update cal data
                for base_log_name in cal_log_dict:
                    for cal_log in cal_log_dict[base_log_name]:

                        # get ptag
                        ptag_db_obj_list = []
                        if cal_log["ptag_path"] is not None:
                            child_ptag_db_obj = get_or_generate_ptag_db_obj(
                                s3_client, cal_log["ptag_path"], indexed_ptag_memo_dict, session, test_run, ptag_service
                            )
                            if child_ptag_db_obj:
                                ptag_db_obj_list.append(child_ptag_db_obj)
                                parent_ptag_paths = get_hierarchical_ptag_paths(cal_log["ptag_path"], ptag_service)
                                for parent_ptag_path in parent_ptag_paths:
                                    ptag_db_obj = get_or_generate_ptag_db_obj(
                                        s3_client,
                                        parent_ptag_path,
                                        indexed_ptag_memo_dict,
                                        session,
                                        test_run,
                                        ptag_service,
                                    )
                                    if not ptag_db_obj:
                                        continue
                                    ptag_db_obj_list.append(ptag_db_obj)

                        # compose final tag json
                        final_tag_json = get_final_subtag_json(
                            s3_client,
                            ptag_db_obj_list,
                            cal_log["cal_log_path"],
                            subtag_dict.get(get_subtag_key_from_path(cal_log["cal_log_path"])),
                        )

                        log_data = cal_data_service.get_cal_data_by_file_path(cal_log["cal_log_path"])
                        log_data.tag_json = final_tag_json

                        session.add(log_data)

@inject
@task()
def index_data_from_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    preset_prefix_path: str = "",
    analytics_reports_service: AnalyticsReportsService = Provide[Container.analytics_reports_service],
    integrity_check_report_service: IntegrityCheckReportService = Provide[Container.integrity_check_report_service],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
    ptag_service: PTagService = Provide[Container.ptag_service],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    cal_data_service: CalDataService = Provide[Container.cal_data_service],
    zip_data_service: ZipDataService = Provide[Container.zip_data_service],
):
    """index data from s3 task to both raw and extracted data

    Args:
        s3_key (str): [description]
        s3_bucket (str): [description]
        s3_secret (str): [description]
        preset_prefix_path (str, optional): prefix path to filter data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    ti: TaskInstance = ctx["ti"]

    airflow_logger = ti.log

    # To implement a priority-based fallback system for configuration values
    prefix_path: str = (
        conf.get("prefix_path_unpacked") or  # 1st priority: Check for unpacked path passed in automated Dag config
        conf.get("prefix_path") or           # 2nd priority: Check for standard path passed in regular index Dag config
        preset_prefix_path                   # 3rd priority: Use default preset value
    )

    test_run: bool = conf.get("test_run", False)
    blacklist_pattern: str = conf.get("blacklist_pattern", "")
    skip_ptag_index: bool = conf.get("skip_ptag_index", False)
    skip_cal_log_index: bool = conf.get("skip_cal_log_index", False)
    skip_raw_log_index: bool = conf.get("skip_raw_log_index", False)
    skip_scene_frame_index: bool = conf.get("skip_scene_frame_index", False)
    skip_scene_frame_linking: bool = conf.get("skip_scene_frame_linking", True)
    skip_sensor_data_index: bool = conf.get("skip_sensor_data_index", False)
    log_level_debug: bool = conf.get("log_level_debug", False)

    if prefix_path.startswith("s3://"):
        prefix_bucket, prefix_path = split_s3url_to_bucket_key(prefix_path)
        if prefix_bucket != s3_bucket:
            airflow_logger.info("Exiting index dag as user provided config bucket does not match the current project bucket")
            return

    user_config_bucket = conf.get("bucket")
    if user_config_bucket and user_config_bucket != s3_bucket:
        airflow_logger.info("Exiting index DAG as user provided config bucket does not match the current project bucket")
        return
    
    # Use the provided s3_bucket if config bucket is None/empty, otherwise use config bucket
    s3_bucket = user_config_bucket if user_config_bucket else s3_bucket
    if not s3_bucket:
        raise ValueError("s3_bucket not provided in argument or DAG config. Exiting")

    # when doing a test_run, set debug log
    if test_run:
        log_level_debug = True

    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    airflow_logger.info(f"index_data_from_s3: {s3_bucket} with prefix: {prefix_path}")
    airflow_logger.debug(f"conf: {conf}")

    s3_client = create_s3_client(s3_key, s3_secret)

    # NOTE: For unpack priority DAG, we calculate the unpack prefix from the ptag
    if "ptag_path" in conf:
        ptag_path: str = conf.get("ptag_path")
        prefix_path = get_unpacked_prefix_from_ptag(s3_client, s3_bucket, ptag_path)

    # init optional skipped vars
    ptag_paths: List[str] = []
    subtag_paths: List[str] = []
    summary_paths: List[str] = []
    scene_frame_data_paths: List[str] = []
    indexed_ptag_paths: List[str] = []
    indexed_ptag_memo_dict: Dict = {}
    log_paths: List[str] = []
    indexed_raw_data_paths: List[str] = []
    indexed_raw_data_update_candidates: Dict = {}
    indexed_cal_data_paths: List[str] = []
    indexed_cal_data_update_candidates: Dict = {}
    sensor_data_file_paths: List[str] = []
    indexed_sensor_data_dict: Dict = {}
    indexed_extracted_data_update_candidates: Dict = {}
    integrity_check_report_paths: List[str] = []
    retrieved_ingerity_check_report_dicts: Dict = {}

    # get ptag and analytics summary json files from s3
    if not (skip_ptag_index and skip_scene_frame_index):
        json_paths = get_file_paths(s3_client, s3_bucket, prefix_path, set(["json"]), blacklist_pattern)
        airflow_logger.info("json paths found: {}".format(len(json_paths)))
        ptag_paths, subtag_paths, summary_paths, scene_frame_data_paths, integrity_check_report_paths = (
            get_index_json_paths(s3_client, s3_bucket, json_paths, test_run=test_run)
        )

    airflow_logger.info("ptag paths found: {}".format(len(ptag_paths)))
    airflow_logger.info("subtag paths found: {}".format(len(subtag_paths)))
    airflow_logger.info("analytics paths found: {}".format(len(summary_paths)))
    airflow_logger.info("scene frame data paths found: {}".format(len(scene_frame_data_paths)))
    airflow_logger.info("integrity check report paths found: {}".format(len(integrity_check_report_paths)))

    airflow_logger.debug(f"ptag_paths: {ptag_paths}")
    airflow_logger.debug(f"subtag_paths: {subtag_paths}")
    airflow_logger.debug(f"summary_paths: {summary_paths}")
    airflow_logger.debug(f"scene_frame_data_paths: {scene_frame_data_paths}")
    airflow_logger.debug(f"integrity_check_report_paths: {integrity_check_report_paths}")

    # get indexed ptag paths already in cdcs
    if not skip_ptag_index:
        indexed_ptag_paths, indexed_ptag_memo_dict = get_indexed_ptag_paths(ptag_paths, ptag_service)

    airflow_logger.info("indexed ptag paths len: {}".format(len(indexed_ptag_paths)))

    # log file ext for rosbag and hlog
    base_log_file_ext = set(["bag", "hlog"])

    # get paths for HLOG and Rosbag paths from s3
    if not (skip_cal_log_index and skip_raw_log_index):
        log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, base_log_file_ext, blacklist_pattern)
    airflow_logger.info("log_paths found: {}".format(len(log_paths)))

    raw_log_dict, cal_log_dict, cal_data_list, raw_data_list = get_raw_and_cal_log_dicts(log_paths, ptag_paths)

    airflow_logger.info("raw logs found: {} - {}".format(len(raw_log_dict), len(raw_data_list)))
    airflow_logger.info("cal logs found: {} - {}".format(len(cal_log_dict), len(cal_data_list)))

    airflow_logger.debug("first 3 raw log paths {}".format(raw_data_list[:3]))

    # get indexed raw data paths already in cdcs
    if not skip_raw_log_index:
        indexed_raw_data_paths, indexed_raw_data_update_candidates = get_indexed_raw_data_paths(
            raw_data_list, raw_data_service
        )
    airflow_logger.info(
        f"indexed raw log paths len: {len(indexed_raw_data_paths)} update candidates len: {len(indexed_raw_data_update_candidates)}"
    )
    if len(indexed_raw_data_paths) <= 5:
        airflow_logger.debug(f"indexed raw data paths {indexed_raw_data_paths}")
    if len(indexed_raw_data_update_candidates) <= 5:
        airflow_logger.debug(f"indexed raw data update candidates {indexed_raw_data_update_candidates}")

    # get indexed cal logs already in cdcs
    if not skip_cal_log_index:
        indexed_cal_data_paths, indexed_cal_data_update_candidates = get_indexed_cal_data_paths(
            cal_data_list, cal_data_service
        )
    airflow_logger.info(
        f"indexed cal log paths len: {len(indexed_cal_data_paths)} update candidates len: {len(indexed_cal_data_update_candidates)}"
    )

    # get sensor data paths from s3 extracted and raw image and laser data
    if not skip_sensor_data_index:
        sensor_data_file_paths = get_file_paths(
            s3_client, s3_bucket, prefix_path, set(["jpg", "png", "pcd", "csv"]), blacklist_pattern
        )
    airflow_logger.info("sensor data paths found: {}".format(len(sensor_data_file_paths)))

    # get indexed sensor data file paths already in cdcs
    if not skip_sensor_data_index:
        indexed_sensor_data_dict, indexed_extracted_data_update_candidates = get_indexed_extracted_data_s3_locs(
            sensor_data_file_paths, extracted_data_service
        )
    airflow_logger.info(
        f"indexed sensor data dict len: {len(indexed_sensor_data_dict)} update candidates len: {len(indexed_extracted_data_update_candidates)}"
    )

    # get indexed scene frame data paths already in cdcs
    indexed_scene_frame_extracted_data_dict, indexed_scene_frame_update_candidates = get_indexed_extracted_data_s3_locs(
        scene_frame_data_paths, extracted_data_service
    )
    airflow_logger.info(
        f"indexed scene frame extracted data paths len: {len(indexed_scene_frame_extracted_data_dict)} update candidates len: {len(indexed_scene_frame_update_candidates)}"
    )

    # get indexed integrity check reports data paths already in cdcs
    indexed_integrity_check_report_data_paths = get_indexed_integrity_check_report_data_paths(
        integrity_check_report_paths, integrity_check_report_service
    )
    airflow_logger.info(
        "indexed integrity check report data paths len: {}".format(len(indexed_integrity_check_report_data_paths))
    )

    # Process zip files in S3
    zip_file_ext = {"zip"}

    # Retrieve all zip file paths from S3 under the specified prefix
    zip_log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, zip_file_ext)
    airflow_logger.info(f"Found {len(zip_log_paths)} zip files in S3.")

    # Get the zip log paths and their corresponding ptag paths
    zip_log_dict, zip_data_list = get_zip_log_dict(zip_log_paths, ptag_paths)

    # Retrieve already indexed zip data paths from the database
    indexed_zip_data_paths, indexed_zip_data_update_candidates = get_indexed_zip_data_paths(
        zip_data_list, zip_data_service
    )
    airflow_logger.info(
        f"indexed zip data paths len: {len(indexed_zip_data_paths)} and update candidates len: {len(indexed_zip_data_update_candidates)}"
    )

    # if test_run flag, only report indexing step
    if test_run:
        return

    subtag_dict = get_subtag_dict(subtag_paths)

    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        airflow_logger.debug(f"session obj {session}")

        # generate ingest context
        data_ingest_ctx = DataIngestContext
        data_ingest_ctx._session = session
        data_ingest_ctx._s3_client = s3_client
        data_ingest_ctx._logger = airflow_logger
        data_ingest_ctx._cal_data_service = cal_data_service
        data_ingest_ctx._raw_data_service = raw_data_service
        data_ingest_ctx._extraced_data_service = extracted_data_service
        data_ingest_ctx._ptag_service = ptag_service
        data_ingest_ctx._zip_data_service = zip_data_service
        data_ingest_ctx._test_run = test_run

        airflow_logger.info(f"data ingest with ingest_context: {data_ingest_ctx}")

        airflow_logger.info("start ptag ingest")

        # index ptags
        ptag_skip_count = 0
        for ptag_path in ptag_paths:
            if skip_ptag_index or ptag_path in indexed_ptag_paths:
                ptag_skip_count += 1
                airflow_logger.debug(f"skipped indexing ptag path: {ptag_path}")
                continue
            ptag_db_obj = get_or_generate_ptag_db_obj(
                data_ingest_ctx._s3_client, ptag_path, indexed_ptag_memo_dict, session, test_run, ptag_service
            )
            if ptag_db_obj is None:
                airflow_logger.warning(f"could not get or gen ptag obj from {ptag_path}")
        session.commit()

        airflow_logger.info(f"finish ptag ingest with {ptag_skip_count} skipped")

        # ingest raw data
        ingest_cal_and_raw_logs(
            ingest_type=CatDataIngestType.RawLogDataIngest,
            data_ingest_ctx=data_ingest_ctx,
            ptag_memo_dict=indexed_ptag_memo_dict,
            log_dict=raw_log_dict,
            indexed_log_paths=indexed_raw_data_paths,
            indexed_log_update_candidates=indexed_raw_data_update_candidates,
            subtag_dict=subtag_dict,
            skip_index=skip_raw_log_index,
        )

        # ingest cal data
        ingest_cal_and_raw_logs(
            ingest_type=CatDataIngestType.CalLogDataIngest,
            data_ingest_ctx=data_ingest_ctx,
            ptag_memo_dict=indexed_ptag_memo_dict,
            log_dict=cal_log_dict,
            indexed_log_paths=indexed_cal_data_paths,
            indexed_log_update_candidates=indexed_cal_data_update_candidates,
            subtag_dict=subtag_dict,
            skip_index=skip_cal_log_index,
        )

        # ingest extracted sensor data file paths
        ingest_sensor_data_and_scene_frame_data(
            ingest_type=CatDataIngestType.SensorDataIngest,
            data_ingest_ctx=data_ingest_ctx,
            data_file_paths=sensor_data_file_paths,
            indexed_data_dict=indexed_sensor_data_dict,
            indexed_data_update_candidates=indexed_extracted_data_update_candidates,
            skip_index=skip_sensor_data_index,
            skip_linking=True,
        )

        # index scene frame data file paths
        ingest_sensor_data_and_scene_frame_data(
            ingest_type=CatDataIngestType.SceneFrameDataIngest,
            data_ingest_ctx=data_ingest_ctx,
            data_file_paths=scene_frame_data_paths,
            indexed_data_dict=indexed_scene_frame_extracted_data_dict,
            indexed_data_update_candidates=indexed_scene_frame_update_candidates,
            skip_index=skip_scene_frame_index,
            skip_linking=skip_scene_frame_linking,
        )

        # index zip data file paths
        ingest_zip_data(
            data_ingest_ctx=data_ingest_ctx,
            ptag_memo_dict=indexed_ptag_memo_dict,
            zip_log_dict=zip_log_dict,
            indexed_zip_data_paths=indexed_zip_data_paths,
            indexed_zip_data_update_candidates=indexed_zip_data_update_candidates,
            subtag_dict=subtag_dict,
            skip_index=False,
        )

        airflow_logger.info("start analytics reports data ingest")

        # match analytics reports summary to ptag, then add to db
        for summary_path in summary_paths:
            summary_bucket, summary_key = split_s3url_to_bucket_key(summary_path)
            summary_dict = json_file_in_s3_to_dict(s3_client, summary_bucket, summary_key)
            if not summary_dict:
                raise ValueError(f"could not get json summary_key: {summary_key}")
            summary_md5 = get_dict_md5(summary_dict)
            report = analytics_reports_service.get_report_by_md5(summary_md5)
            # if report already exists, skip
            if report is not None:
                continue

            # link extracted data with ptag if matches
            ptag_db_obj = None

            log_or_snippet_name = get_base_log_or_snippet_name_from_s3_path(summary_path)
            if log_or_snippet_name is None:
                airflow_logger.warning("skipping could not get log or snippet name from: {}".format(summary_path))
                continue
            ptag_db_obj = ptag_service.get_ptag_by_base_log_or_snippet_name(log_or_snippet_name)

            # get ptag id to link to analytics report
            if ptag_db_obj is not None:
                ptag_id = ptag_db_obj.id
            else:
                ptag_id = None

            # get raw data
            raw_data = None
            raw_data_id = None
            if log_or_snippet_name:
                raw_data = raw_data_service.get_raw_data_by_file_path_like_op(log_or_snippet_name)
                if raw_data is not None:
                    raw_data_id = raw_data.id

            # add new report to db
            report_db_obj = AnalyticsReports(
                report_name=summary_dict.get("name"),
                md5=summary_md5,
                bag_duration=summary_dict.get("bag_duration"),
                bag_file_size_in_kb=summary_dict.get("bag_file_size_in_kb"),
                distance_travelled=summary_dict.get("distance_travelled"),
                raw_data_id=raw_data_id,
            )

            session.add(report_db_obj)

            airflow_logger.debug(f"report: {summary_path} ptag id: {ptag_id}")

            # add new report details to db
            report_data = analytics_reports_service.get_report_by_md5(summary_md5)
            if report_data is None:
                airflow_logger.warning("report_data none for report md5sum: {}".format(summary_md5))
                continue

            # get summary details from summary results
            sensors = summary_dict.get("results")
            if sensors is None:
                airflow_logger.warning("summary dict no results: {}".format(summary_dict))
                continue

            for sensor, values in sensors.items():
                sensor_obj = AnalyticsReportsDetails(
                    sensor_name=sensor,
                    bag_id=report_data.id,
                    ptag_id=ptag_id,
                    start_time=values.get("start_time"),
                    sensor_nickname=values.get("nickname"),
                    actual_msgs=values.get("actual_msgs"),
                    expected_msgs=values.get("expected_msgs"),
                    duration_in_seconds=values.get("duration_in_seconds"),
                    missing_sequence_count=values.get("missing_sequence_count"),
                    min_time_diff_in_nano_secs=values.get("min_time_diff_in_nano_secs"),
                    max_time_diff_in_nano_secs=values.get("max_time_diff_in_nano_secs"),
                    average_time_diff_in_nano_secs=values.get("average_time_diff_in_nano_secs"),
                    expected_freq_hz=values.get("expected_freq_hz"),
                    actual_freq_hz=values.get("actual_freq_hz"),
                    extracted_frame_rate=values.get("extracted_frame_rate"),
                )
                session.add(sensor_obj)

                airflow_logger.debug(f"details: {sensor} report data id: {report_data.id}")
        session.commit()

        integrity_log_skip_count = 0
        integrity_log_ingest_count = 0
        for integrity_check_path in integrity_check_report_paths:
            # if report already indexed, skip
            if integrity_check_path in indexed_integrity_check_report_data_paths:
                integrity_log_skip_count += 1
                continue

            integrity_bucket, integrity_key = split_s3url_to_bucket_key(integrity_check_path)
            integrity_dict = json_file_in_s3_to_dict(s3_client, integrity_bucket, integrity_key)
            if not integrity_dict:
                raise ValueError(f"could not get json integrity_key: {integrity_key}")
            integrity_md5: Optional[Dict] = get_dict_md5(integrity_dict)
            retrieved_ingerity_check_report_dicts[integrity_check_path] = integrity_dict
            # get raw data
            log_or_snippet_name = get_base_log_or_snippet_name_from_s3_path(integrity_check_path)
            raw_data = None
            raw_data_id = None
            if log_or_snippet_name:
                TransmissionTimeIndexPath = log_or_snippet_name + "/TransmissionTimeIndex00000.hlog"
                raw_data = raw_data_service.get_raw_data_by_file_path_like_op(TransmissionTimeIndexPath)
                if raw_data is not None:
                    raw_data_id = raw_data.id

            if not log_or_snippet_name:
                continue

            # get integrity report details
            try:
                if not integrity_dict.get("report"):
                    airflow_logger.error("integrity dict does not contain report key")
                    continue
                hlogs = integrity_dict["report"].get("hlogs")
                if not hlogs or len(hlogs) == 0:
                    airflow_logger.info(f"integrity report {integrity_check_path} has no hlogs and is not valid")
                    integrity_log_skip_count += 1
                    continue
                distance_traveled = 0.0
                log_duration = 0.0
                pcd_occlusions: bool = False
                report_version = integrity_dict["report"].get("version")
                for hlog_idx in hlogs:
                    if (
                        report_version == _INTEGRITY_REPORT_VERSION_1_1_0
                        or report_version == _INTEGRITY_REPORT_VERSION_1_2_0
                    ):
                        distance_traveled_m = hlog_idx["hlog"].get("distanceTraveled_m")
                        if distance_traveled_m:
                            distance_traveled = distance_traveled + float(distance_traveled_m)
                        log_duration_s = hlog_idx["hlog"].get("logDuration_s")
                        if log_duration_s:
                            log_duration = log_duration + float(log_duration_s)
                        if hlog_idx["hlog"].get("lidarPointCloudOccludedDetected") or hlog_idx["hlog"].get(
                            "radarPointCloudOccludedDetected"
                        ):
                            pcd_occlusions = True
                    # older versions
                    elif (
                        report_version == _INTEGRITY_REPORT_VERSION_1_0_0
                        or report_version == _INTEGRITY_REPORT_VERSION_1_0_2
                    ):
                        distance_traveled_m = hlog_idx["hlog"].get("distanceTraveled_m")
                        if distance_traveled_m:
                            distance_traveled = distance_traveled + float(distance_traveled_m)
                        log_duration_hlog = hlog_idx["hlog"].get("logDuration")
                        if log_duration_hlog:
                            log_duration = log_duration + float(log_duration_hlog)
                        if hlog_idx["hlog"].get("lidarPointCloudOccludedDetected"):
                            pcd_occlusions = True
                    else:
                        # try failsafe read first
                        if hlog_idx["hlog"].get("lidarPointCloudOccludedDetected") or hlog_idx["hlog"].get(
                            "radarPointCloudOccludedDetected"
                        ):
                            pcd_occlusions = True
                        # otherwise raise error
                        raise ValueError(f"could not match integrity check report version: {report_version}")

                # add new integrity check report to db
                integrity_report_db_obj = IntegrityCheckReport(
                    report_path=integrity_check_path,
                    md5=integrity_md5,
                    hlog_duration=log_duration,
                    distance_travelled=distance_traveled,
                    pcd_occlusions_detected=pcd_occlusions,
                    num_split_files_used=integrity_dict["report"].get("totalHlogsScanned"),
                    raw_data_id=raw_data_id,
                )
                session.add(integrity_report_db_obj)
                session.commit()

                integrity_log_ingest_count += 1
                airflow_logger.info(f"added report: {integrity_check_path} with raw data id: {raw_data_id}")

                for hlog_idx in hlogs:
                    if hlog_idx["hlog"].get("missingChannels") is not None:
                        # check hlog for missing channels
                        for missing_channel_name in hlog_idx["hlog"]["missingChannels"]:
                            missing_channel_obj = IntegrityCheckReportMissingChannel(
                                missing_channel_name=missing_channel_name,
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(missing_channel_obj)

                    # check hlog for tf mismatch
                    if hlog_idx["hlog"].get("tfDynamicFrameIdMissingConnectionToTFStatic") is not None:
                        for tf_mismatch_channel_name in hlog_idx["hlog"]["tfDynamicFrameIdMissingConnectionToTFStatic"]:
                            tf_mismatch_obj = IntegrityCheckReportTFDynamicTFStaticMismatch(
                                tf_dynamic_tf_static_mismatch_channel_name=tf_mismatch_channel_name,
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(tf_mismatch_obj)

                    latency_issue_detected = hlog_idx["hlog"].get("latencyIssuesCount")
                    ouster_cloud_occulded_detected = hlog_idx["hlog"].get("ousterPointCloudOccludedDetected")
                    point_cloud_occulded_detected = hlog_idx["hlog"].get("lidarPointCloudOccludedDetected")
                    radar_cloud_occulded_detected = hlog_idx["hlog"].get("radarPointCloudOccludedDetected")
                    sequence_out_of_order_detected = hlog_idx["hlog"].get("sequenceIdsOutOfOrderDetected")
                    transmission_time_delay_detected = hlog_idx["hlog"].get("transmissionTimeDelayDetected")

                    if not (
                        latency_issue_detected
                        or ouster_cloud_occulded_detected
                        or point_cloud_occulded_detected
                        or radar_cloud_occulded_detected
                        or sequence_out_of_order_detected
                        or transmission_time_delay_detected
                    ):
                        # no hlog level issues found
                        continue

                    # check channels of hlog_idx ONLY if any issues are found
                    for channel_idx in hlog_idx["hlog"].get("channels"):

                        # check channel for latency issues
                        if channel_idx["latencyIssuesCount"] > 0:
                            latency_issues_obj = IntegrityCheckReportLatencyIssue(
                                latency_issue_channel_name=channel_idx.get("channelName"),
                                latency_issue_count=channel_idx.get("latencyIssuesCount"),
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(latency_issues_obj)

                        # check channel for point cloud issues
                        lidar_detected = channel_idx.get("lidarPointCloudOccludedDetected")
                        ouster_detected = channel_idx.get("ousterPointCloudOccludedDetected")
                        radar_detected = channel_idx.get("radarPointCloudOccludedDetected")
                        if lidar_detected or ouster_detected or radar_detected:
                            point_cloud_data_enum: PointCloudDataEnum = PointCloudDataEnum.unknown
                            if lidar_detected:
                                point_cloud_data_enum.lidar
                            elif ouster_detected:
                                point_cloud_data_enum.ouster
                            elif radar_detected:
                                point_cloud_data_enum.radar
                            occluded_channel_obj = IntegrityCheckReportPointCloudOccludedChannel(
                                point_cloud_occluded_channel_name=channel_idx.get("channelName"),
                                point_cloud_data_type=point_cloud_data_enum,
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(occluded_channel_obj)

                        # check channel for seq order
                        if channel_idx.get("sequenceIdsOutOfOrderCount") is not None:
                            if channel_idx["sequenceIdsOutOfOrderCount"] > 0:
                                seq_out_of_order_channel_obj = IntegrityCheckReportSequenceOutOfOrderChannel(
                                    sequence_out_of_order_channel_name=channel_idx.get("channelName"),
                                    sequence_out_of_order_count=channel_idx.get("sequenceIdsOutOfOrderCount"),
                                    split_file_num=hlog_idx["hlog"].get("splitNum"),
                                    integrity_check_report_id=integrity_report_db_obj.id,
                                )
                                session.add(seq_out_of_order_channel_obj)

                        # check channel for missing calibration
                        if channel_idx.get("sensorsNotCalibrated") is not None:
                            sensors_not_calibrated_obj = IntegrityCheckReportNotCalibratedSensor(
                                sensor_not_calibrated_channel_name=channel_idx.get("channelName"),
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(sensors_not_calibrated_obj)

                        # check channel for transmission time delay issues
                        if channel_idx.get("transmissionTimeDelayDetected") is not None:
                            transmission_time_delay_obj = IntegrityCheckReportTransmissionTimeDelay(
                                transmission_time_delay_channel_name=channel_idx.get("channelName"),
                                transmission_time_delay_avg=channel_idx.get("transmissionDelayAvg"),
                                transmission_time_delay_max=channel_idx.get("transmissionDelayMax"),
                                transmission_time_delay_min=channel_idx.get("transmissionDelayMin"),
                                transmission_timestamp_invalid_count=channel_idx.get(
                                    "transmissionTimestampInvalidCount"
                                ),
                                split_file_num=hlog_idx["hlog"].get("splitNum"),
                                integrity_check_report_id=integrity_report_db_obj.id,
                            )
                            session.add(transmission_time_delay_obj)
                    session.commit()
            except:
                raise ValueError("Integrity check report {} generated is not valid".format(integrity_check_path))

        airflow_logger.info(
            f"finished ingesting integrity check reports with {integrity_log_skip_count} skipped, {integrity_log_ingest_count} ingested"
        )

        session.commit()


@inject
@task(task_id="index_staging_data_from_s3")
def index_staging_data_from_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    preset_prefix_path: str = "",
    ptag_service: PTagService = Provide[Container.ptag_service],
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
    zip_data_service: ZipDataService = Provide[Container.zip_data_service],
):
    """index raw log and ptag from s3 staging to tables

    Args:
        s3_key (str): [description]
        s3_bucket (str): [description]
        s3_secret (str): [description]
        preset_prefix_path (str, optional): prefix path to filter extracted data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    ti: TaskInstance = ctx["ti"]
    
    #Instantiate airflow_logger
    airflow_logger = ti.log

    prefix_path = conf.get("prefix_path", preset_prefix_path)
    if not prefix_path:
        raise ValueError("prefix_path not provided in argument or DAG config. Exiting")

    if prefix_path.startswith("s3://"):
        prefix_bucket, prefix_path = split_s3url_to_bucket_key(prefix_path)
        if prefix_bucket != s3_bucket:
            airflow_logger.info("Exiting index dag as user provided config bucket does not match the current project bucket")
            return
    user_config_bucket = conf.get("bucket")
    if user_config_bucket and user_config_bucket != s3_bucket:
        airflow_logger.info(f"Exiting staging index DAG as user provided config bucket: {user_config_bucket} does not match the current project bucket: {s3_bucket}")
        return
    
    # Use the provided s3_bucket if config bucket is None/empty, otherwise use config bucket
    s3_bucket = user_config_bucket if user_config_bucket else s3_bucket
    if not s3_bucket:
        raise ValueError("s3_bucket not provided in argument or DAG config. Exiting")

    test_run = conf.get("test_run", False)
    blacklist_pattern = conf.get("blacklist_pattern", "")
    log_level_debug: bool = conf.get("log_level_debug", False)

    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    airflow_logger.info(f"index_staging_data_from_s3: {s3_bucket} with prefix: {prefix_path}")
    airflow_logger.debug(f"conf: {conf}")

    s3_client = create_s3_client(s3_key, s3_secret)

    # get ptag and analytics summary json files from s3
    json_paths = get_file_paths(s3_client, s3_bucket, prefix_path, set(["json"]), blacklist_pattern)
    ptag_paths, _, summary_paths, _, _ = get_index_json_paths(s3_client, s3_bucket, json_paths, test_run)

    airflow_logger.info("ptag paths found: {}".format(len(ptag_paths)))
    airflow_logger.info("analytics paths found: {}".format(len(summary_paths)))

    airflow_logger.debug(f"ptag_paths: {ptag_paths}")
    airflow_logger.debug(f"summary_paths: {summary_paths}")

    # get indexed ptags already in cdcs
    indexed_ptag_paths, indexed_ptag_memo_dict = get_indexed_ptag_paths(ptag_paths, ptag_service)
    airflow_logger.info("indexed ptag paths len: {}".format(len(indexed_ptag_paths)))

    # log file ext for rosbag and hlog
    base_log_file_ext = set(["bag", "hlog", "zip"])

    # get paths for HLOG, Rosbag, and ZIP paths from s3
    raw_log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, base_log_file_ext, blacklist_pattern)
    airflow_logger.info("staging raw_log_paths found: {}".format(len(raw_log_paths)))

    # split out zip files for zip_data indexing
    zip_log_paths = [p for p in raw_log_paths if p.endswith(".zip")]
    non_zip_log_paths = [p for p in raw_log_paths if not p.endswith(".zip")]

    raw_log_dict, cal_log_dict, cal_data_list, raw_data_list = get_raw_and_cal_log_dicts(
        non_zip_log_paths, ptag_paths, staging_mode=True
    )
    airflow_logger.info("staging raw logs found: {} - {}".format(len(raw_log_dict), len(raw_data_list)))
    airflow_logger.info("staging cal logs found: {} - {}".format(len(cal_log_dict), len(cal_data_list)))

    # get indexed raw data staging already in cdcs
    indexed_raw_data_staging_paths, indexed_raw_data_staging_update_candidates = get_indexed_raw_data_staging_paths(
        raw_log_paths, raw_data_staging_service
    )
    airflow_logger.info("indexed raw data staging paths len: {}".format(len(indexed_raw_data_staging_paths)))
    airflow_logger.info(
        "indexed raw data staging update candidates len: {}".format(len(indexed_raw_data_staging_update_candidates))
    )

    # get zip log dict for zip_data indexing
    zip_log_dict, zip_data_list = get_zip_log_dict(zip_log_paths, ptag_paths)
    indexed_zip_data_paths, indexed_zip_data_update_candidates = get_indexed_zip_data_paths(
        zip_data_list, zip_data_service
    )
    airflow_logger.info(
        f"indexed zip data paths len: {len(indexed_zip_data_paths)} and update candidates len: {len(indexed_zip_data_update_candidates)}"
    )

    if test_run:
        airflow_logger.info("raw_log_dict: {}".format(raw_log_dict))
        airflow_logger.info("indexed raw data staging paths: {}".format(indexed_raw_data_staging_paths))
        airflow_logger.info(
            "indexed raw data staging update candidates: {}".format(indexed_raw_data_staging_update_candidates)
        )
        airflow_logger.info("zip_log_dict: {}".format(zip_log_dict))
        airflow_logger.info("indexed zip data paths: {}".format(indexed_zip_data_paths))
        airflow_logger.info("indexed zip data update candidates: {}".format(indexed_zip_data_update_candidates))

    # FIXME: !!! This level of nesting needs to be refactored
    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        config_ptag_path: str | None = conf.get("ptag_path")
        if config_ptag_path:
            config_ptag_path = config_ptag_path.strip()
            if not config_ptag_path.startswith(f"s3://{s3_bucket}/"):
                config_ptag_path = get_s3_url_from_bucket_and_prefix(s3_bucket, config_ptag_path).rstrip("/")
            airflow_logger.debug(f"PTag path provided in config: {config_ptag_path}")
            ptag_paths.append(config_ptag_path)

        # index ptags
        for ptag_path in ptag_paths:
            if ptag_path in indexed_ptag_paths:
                # skip already indexed ptag paths
                airflow_logger.info("skipped indexing ptag path: {}".format(ptag_path))
                continue

            get_or_generate_ptag_db_obj(s3_client, ptag_path, indexed_ptag_memo_dict, session, test_run, ptag_service)

        # index raw data staging
        for base_log_name in raw_log_dict:
            for raw_staging_log in raw_log_dict[base_log_name]:
                skip_session_add = False
                update_record = False
                if raw_staging_log["log_path"] in indexed_raw_data_staging_paths:
                    # skip already indexed raw data staging
                    airflow_logger.info("skipped raw data staging: {}".format(raw_staging_log["log_path"]))
                    continue

                ptag_db_obj = None
                ptag_db_obj_id = None

                if raw_staging_log["ptag_path"] is not None:
                    # get ptag
                    ptag_db_obj = get_or_generate_ptag_db_obj(
                        s3_client, raw_staging_log["ptag_path"], indexed_ptag_memo_dict, session, test_run, ptag_service
                    )

                if raw_staging_log["log_path"] in indexed_raw_data_staging_update_candidates:
                    airflow_logger.debug(f"in update candidates: {raw_staging_log['log_path']}")
                    log_data = indexed_raw_data_staging_update_candidates.get(raw_staging_log["log_path"])
                    if raw_staging_log["ptag_path"] is not None:
                        if log_data.ptag_id == ptag_db_obj.id:
                            airflow_logger.info(f"skipping session add for {raw_staging_log['log_path']}, no update")
                            skip_session_add = True
                        else:
                            airflow_logger.info(
                                f"updating with ptag from path: {raw_staging_log['ptag_path']}, ptag_db_obj.id = {ptag_db_obj.id}"
                            )
                            log_data.ptag_id = ptag_db_obj.id
                            update_record = True
                    else:
                        airflow_logger.info(f"skipping session add for {raw_staging_log['log_path']}, no ptag_path")
                        skip_session_add = True
                else:
                    airflow_logger.debug(f"not in db: {raw_staging_log['log_path']}")
                    # index HLOG or Rosbag
                    if raw_staging_log["ptag_path"] is not None:
                        log_data = RawDataStaging(
                            raw_staging_log["log_path"],
                            ptag_db_obj.id,
                            raw_data_type=raw_staging_log["log_type"],
                            raw_data_id=None,
                        )
                        log_data.ptag = ptag_db_obj
                        if ptag_db_obj:
                            ptag_db_obj_id = ptag_db_obj.id
                    else:
                        log_data = RawDataStaging(
                            raw_staging_log["log_path"],
                            None,
                            raw_data_type=raw_staging_log["log_type"],
                            raw_data_id=None,
                        )

                if not test_run and not skip_session_add:
                    if update_record:
                        log_data = session.merge(log_data)
                        session.add(log_data)
                        airflow_logger.info(f"updated: {raw_staging_log} with ptag_id: {log_data.ptag_id}")
                    else:
                        session.add(log_data)
                        airflow_logger.info(f"indexed: {raw_staging_log} with ptag_id: {ptag_db_obj_id}")

        # index zip_data
        for base_log_name in zip_log_dict:
            for zip_log in zip_log_dict[base_log_name]:
                skip_session_add = False
                update_record = False
                if zip_log["log_path"] in indexed_zip_data_paths:
                    airflow_logger.info("skipped zip_data: {}".format(zip_log["log_path"]))
                    continue
                ptag_db_obj = None
                ptag_db_obj_id = None

                if zip_log["ptag_path"] is not None:
                    ptag_db_obj = get_or_generate_ptag_db_obj(
                        s3_client, zip_log["ptag_path"], indexed_ptag_memo_dict, session, test_run, ptag_service
                    )

                if zip_log["log_path"] in indexed_zip_data_update_candidates:
                    airflow_logger.debug(f"in update candidates: {zip_log['log_path']}")
                    log_data = indexed_zip_data_update_candidates.get(zip_log["log_path"])
                    if zip_log["ptag_path"] is not None:
                        if log_data.ptag_id == ptag_db_obj.id:
                            airflow_logger.info(f"skipping session add for {zip_log['log_path']}, no update")
                            skip_session_add = True
                        else:
                            airflow_logger.info(
                                f"updating with ptag from path: {zip_log['ptag_path']}, ptag_db_obj.id = {ptag_db_obj.id}"
                            )
                            log_data.ptag_id = ptag_db_obj.id
                            update_record = True
                    else:
                        airflow_logger.info(f"skipping session add for {zip_log['log_path']}, no ptag_path")
                        skip_session_add = True
                else:
                    airflow_logger.debug(f"not in db: {zip_log['log_path']}")
                    if zip_log["ptag_path"] is not None:
                        log_data = ZipData(zip_log["log_path"], unzipped_file_location=None, ptag_id=ptag_db_obj.id)
                        if ptag_db_obj:
                            ptag_db_obj_id = ptag_db_obj.id
                    else:
                        log_data = ZipData(zip_log["log_path"], unzipped_file_location=None, ptag_id=None)

                if not test_run and not skip_session_add:
                    if update_record:
                        log_data = session.merge(log_data)
                        session.add(log_data)
                        airflow_logger.info(f"updated: {zip_log} with ptag_id: {log_data.ptag_id}")
                    else:
                        session.add(log_data)
                        airflow_logger.info(f"indexed: {zip_log} with ptag_id: {ptag_db_obj_id}")


@inject
@task()
def unzip_files_to_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    preset_prefix_path: str = "",
    zip_data_service: ZipDataService = Provide[Container.zip_data_service],
):
    """
    Unzips files from S3 whose unzip_status is 0 and unzipped file_path is NULL in the ZipData table and updates unzipped paths in db.

    Args:
        s3_key (str): AWS S3 key.
        s3_bucket (str): S3 bucket name.
        s3_secret (str): AWS S3 secret.
        preset_prefix_path (str, optional): Prefix path to filter zip files.
        zip_data_service (ZipDataService): Service to interact with ZipData table.
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # Get only zip files that are not yet unzipped
    zip_files_with_prefix = zip_data_service.get_all_zip_data_paths(s3_bucket, prefix_path)
    zip_datasets = zip_data_service.get_zip_data_not_unzipped_yet(zip_files_with_prefix)
    if zip_datasets is None or len(zip_datasets) == 0:
        print("No zip files found to unzip in the database with prefix:", prefix_path)
        return
    zip_data_s3_file_paths = [zip_data.file_path for zip_data in zip_datasets]
    s3_client = create_s3_client(s3_key, s3_secret)

    zip_data_s3_tree = group_s3_paths_by_hierarchy_tree(zip_data_s3_file_paths)
    zip_data_prefix_for_batch_jobs = flatten_tree_for_batch(zip_data_s3_tree)
    print(f"List of zip data prefixes to be unzipped is {json.dumps(zip_data_prefix_for_batch_jobs, indent=2)}")

    response = s3_client.get_bucket_location(Bucket=s3_bucket)
    s3_region = response.get("LocationConstraint")
    print(f"Using S3 region: {s3_region}")

    batch_client = create_batch_client_v2(s3_key, s3_secret, s3_region)
    sts_client = create_sts_client(s3_key, s3_secret, s3_region)

    job_name = get_batch_job_name("UNZIP")

    # Extract aws account information for submitting batch job with job role ARN and execution role ARN
    response = sts_client.get_caller_identity()
    aws_account = response["Account"]

    unzip_job_dict = {"jobId": [], "unzipPrefix": []}
    for unzip_s3_path in zip_data_prefix_for_batch_jobs:
        # Create a batch job for each prefix
        _, unzip_prefix = split_s3url_to_bucket_key(unzip_s3_path)
        response = run_unzip_job_in_batch(
            client=batch_client,
            bucket=s3_bucket,
            unzip_prefix=unzip_prefix,
            job_name=job_name,
            aws_account=aws_account,
            aws_region=s3_region,
        )
        try:
            job_id = response["jobId"]
            unzip_job_dict["jobId"].append(job_id)
            unzip_job_dict["unzipPrefix"].append(unzip_prefix)
        except KeyError as e:
            airflow_logger.error("run_unzip_job_in_batch response key error")
            raise KeyError(e)

    return unzip_job_dict


@inject
@task()
def send_files_to_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    extracted_data_service: ExtractedDataService = Provide[Container.extracted_data_service],
):
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    raw_data_ids = conf.get("raw_data_ids", [])
    print("uploading converted data to s3 with raw data ids:", raw_data_ids)
    raw_datasets = raw_data_service.get_raw_datasets_by_ids(raw_data_ids)
    s3_client = create_s3_client(s3_key, s3_secret)
    results = [
        {
            "id": raw_data.id,
            "converted_file_paths": raw_data.converted_file_path.split(","),
            "team": raw_data.ptag.team,
        }
        for raw_data in raw_datasets
    ]
    for result in results:
        with Context().new_session_scope() as session:
            raw_data_id = result["id"]
            converted_file_paths = result["converted_file_paths"]
            target_dir = result["team"]
            if len(target_dir) == 0:
                target_dir = "Unknown"
            for converted_file_path in converted_file_paths:
                # check if this has already been uploaded
                extracted_data = extracted_data_service.get_extracted_data_by_file_path(converted_file_path)
                if extracted_data is not None:
                    # skip if it is already indexed
                    print(converted_file_path, "has already been indexed")
                    continue
                s3_path_dir = target_dir + "/" + "/".join(ntpath.dirname(converted_file_path).split("/")[4:])
                uploaded_s3_paths = upload_files_to_s3(s3_client, s3_bucket, [converted_file_path], s3_path_dir)
                if len(uploaded_s3_paths) <= 0:
                    raise ValueError(f"Failed to upload file to S3: {converted_file_path}")
                extracted_data = ExtractedData(converted_file_path, uploaded_s3_paths[0], raw_data_id)
                session.add(extracted_data)
                print(
                    converted_file_path,
                    "has been uploaded to S3",
                    uploaded_s3_paths[0],
                )


@inject
@task(task_id="check_unpack_ready_logs")
def check_unpack_ready_logs(
    s3_key: str,
    s3_secret: str,
    preset_prefix_path: str = "",
    aws_s3_bucket: str = "",
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """checks whether the logs are available for unpacking in the given bucket

    Args:
        aws_s3_bucket (str): [description]
        aws_s3_key (str): [description]
        aws_s3_secret (str): [description]
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs

    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    user_config_bucket = conf.get("bucket", "")
    if user_config_bucket != "":
        aws_s3_bucket = user_config_bucket

    if aws_s3_bucket is None:
        raise Exception("Bucket missing in the dag config. Exiting")

    prefix_path = conf.get("prefix_path", "Staging")
    if prefix_path == "":
        prefix_path = preset_prefix_path

    s3_client = create_s3_client(s3_key, s3_secret)

    with Context().new_session_scope() as session:

        airflow_logger.info(f"check db with s3 bucket {aws_s3_bucket} and prefix {prefix_path} ")
        unpacked_path_s3_url = get_s3_url_from_bucket_and_prefix(aws_s3_bucket, prefix_path)
        airflow_logger.info(f"unpacked_path_s3_url: {unpacked_path_s3_url} ")
        raw_data_staging_path_to_be_unpacked = (
            raw_data_staging_service.get_unpack_ready_raw_data_staging_logs_with_prefix(unpacked_path_s3_url)
        )
        staging_paths_in_db = [str(s3_path) for s3_path in raw_data_staging_path_to_be_unpacked]
        unpack_ready_prefix_list: List[str] = []
        unpack_ready_prefix_list = get_common_staging_prefixes(staging_paths_in_db)
        airflow_logger.info(f"raw_data_staging_path_to_be_unpacked: {unpack_ready_prefix_list}")
        airflow_logger.info(f"length of raw_data_staging_path_to_be_unpacked: {len(unpack_ready_prefix_list)}")

    return unpack_ready_prefix_list


@inject
@task(task_id="check_unpack_ready_logs")
def check_unpack_ready_logs_v2(
    prefix_path: str = "",
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    ti: TaskInstance = ctx["ti"]
    logger = ti.log
    logger.setLevel(logging.DEBUG)

    bucket = conf.get("bucket", "")
    if not bucket:
        raise ValueError("Bucket missing in the DAG config. Exiting")

    prefix_path = conf.get("prefix_path", prefix_path)
    if not prefix_path:
        raise ValueError("Prefix path not passed in and not present in DAG config. Exiting")

    with Context().new_session_scope():
        logger.info(f"Check unpack ready logs in {bucket=} and {prefix_path=} ")
        prefix_path_s3_url = get_s3_url_from_bucket_and_prefix(bucket, prefix_path)
        raw_log_paths: list[Any] = (
            raw_data_staging_service.get_unpack_ready_raw_data_staging_logs_with_prefix(prefix_path_s3_url)
        )
        raw_log_paths = [log.file_path for log in raw_log_paths]
        logger.debug(f"{raw_log_paths=}")
        unpack_ready_prefixes = get_common_staging_prefixes(raw_log_paths)
        logger.info(f"Length of unpack ready prefixes: {len(unpack_ready_prefixes)}")
        logger.debug(f"{unpack_ready_prefixes=}")
        return unpack_ready_prefixes


@inject
@task(task_id="create_filesystem_id")
def create_filesystem(
    aws_s3_key: str,
    aws_s3_secret: str,
    aws_batch_region: str,
    staging_prefix_list: List[str] = [],
    aws_s3_bucket: str = "",
    file_system_dict: Optional[Dict] = None,
):
    """creates fsx file system

    Args:
        aws_s3_key (str): [description]
        aws_s3_secret (str): [description]
        aws_batch_region (str): [description].
        aws_s3_bucket (str): [description]
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    user_config_bucket = conf.get("bucket", "")
    workflow_type = conf.get("workflow_type", "")
    file_system_id = conf.get("FileSystemID", "")
    file_system_mountpath = conf.get("FileSystemMountPath", "")
    
    # If file_system_dict is provided and already has FileSystemID, return it (for test or override)
    if file_system_id and file_system_mountpath:
        airflow_logger.info("Using provided file_system_dict for FSx filesystem.")
        file_system_dict = {
            "FileSystemID": file_system_id,
            "FileSystemMountPath": file_system_mountpath
        }
        # push xcom value
        context = get_current_context()
        ti = context["ti"]
        ti.xcom_push("fsx_file_system_id", file_system_id)
        
        return file_system_dict

    if user_config_bucket != "":
        aws_s3_bucket = user_config_bucket

    if aws_s3_bucket is None:
        raise Exception(f"Bucket missing in the dag config. Exiting")

    fsx_config_path = conf.get("fsx_config_path", "Configs/fsx-configs/fsx_config.json")

    airflow_logger.info(f"aws_s3_bucket: {aws_s3_bucket}")
    airflow_logger.info(f"fsx_config_path: {fsx_config_path}")

    s3_client = create_s3_client(aws_s3_key, aws_s3_secret)
    fsx_client = create_fsx_client(aws_s3_key, aws_s3_secret, aws_batch_region)
    try:
        result = s3_client.get_object(Bucket=aws_s3_bucket, Key=fsx_config_path)
        config = result["Body"].read().decode()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print(f"could not get file: {fsx_config_path} in bucket {aws_s3_bucket}")
            raise ClientError(e)
    except KeyError as e:
        print(f"could not get key Body from result: {e}")
        raise KeyError(e)
    fsx_config = json.loads(config)
    file_size_in_GB = 0
    fsx_memory_capacity = 0

    if workflow_type is not None and workflow_type == WorkflowType.EXTRACT.value:
        extraction_path = conf.get("prefix_path", "")
        if aws_s3_bucket != "" and extraction_path != "":
            file_size_in_GB += calculate_s3_prefix_size_gb(s3_client, aws_s3_bucket, extraction_path)
            fsx_memory_capacity = math.ceil((4 * file_size_in_GB) / 2400) * 2400
            airflow_logger.info(f"Total assigned fsx filesystem memory in GB is: {fsx_memory_capacity}")
    else:
        if not staging_prefix_list:
            raise Exception("No logs in staging/prefix path ready for unpacking, abandoning file system creation")

        for staging_prefix in staging_prefix_list:
            _, key = split_s3url_to_bucket_key(staging_prefix)
            file_size_in_GB += calculate_s3_prefix_size_gb(s3_client, aws_s3_bucket, key)

        if len(staging_prefix_list) > 0:
            airflow_logger.info(f"Total size of staging prefixes in GB is: {file_size_in_GB}")
            fsx_memory_capacity = math.ceil((4 * file_size_in_GB) / 2400) * 2400
            airflow_logger.info(f"Total assigned fsx filesystem memory in GB is: {fsx_memory_capacity}")
        else:
            fsx_memory_capacity = 0

    aws_batch_filesystem_dict = create_fsx_filesystem(fsx_client, fsx_config, aws_s3_bucket, fsx_memory_capacity)

    # push xcom value
    context = get_current_context()
    ti = context["ti"]
    ti.xcom_push("fsx_file_system_id", aws_batch_filesystem_dict["FileSystemID"])

    return aws_batch_filesystem_dict


@inject
@task(task_id="create_filesystem_v2")
def create_filesystem_v2(
    aws_s3_key: str,
    aws_s3_secret: str,
    src_bucket: str,
    dest_bucket: str,
    unmerged_cfh_1a_pairs: Optional[List[Tuple[str, str]]] = None,
    fsx_config_path: str = "Configs/fsx-configs/fsx_config.json",
    file_system_dict: Optional[Dict] = None,
):
    """
    Creates FSx file system for merging CFH and 1A logs.

    Args:
        aws_s3_key (str): AWS S3 access key.
        aws_s3_secret (str): AWS S3 secret key.
        src_bucket (str): Source S3 bucket name.
        dest_bucket (str): Destination S3 bucket name.
        unmerged_cfh_1a_pairs (Optional[List[Tuple[str, str]]]): List of (cfh_key, one_a_key) pairs.
        fsx_config_path (str): Path to FSx config JSON in S3.
        file_system_dict (Optional[Dict]): Optional dict to override or store FSx info.
    Returns:
        Dict: AWS Batch filesystem dictionary.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    file_system_id = conf.get("FileSystemID", "")
    file_system_mountpath = conf.get("FileSystemMountPath", "")
  
    if not unmerged_cfh_1a_pairs:
        raise ValueError("No logs in unmerged_cfh_1a_pairs ready for merging, abandoning file system creation")
    
    # If file_system_dict is provided and already has FileSystemID, return it (for test or override)
    if file_system_id and file_system_mountpath:
        airflow_logger.info("Using provided file_system_dict for FSx filesystem.")
        file_system_dict = {
            "FileSystemID": file_system_id,
            "FileSystemMountPath": file_system_mountpath
        }
        # push xcom value
        context = get_current_context()
        ti = context["ti"]
        ti.xcom_push("fsx_file_system_id", file_system_id)
        
        return file_system_dict

      # Allow fsx_config_path override from conf
    fsx_config_path = conf.get("fsx_config_path", fsx_config_path)
    airflow_logger.info(f"fsx_config_path: {fsx_config_path}")

    s3_client = create_s3_client(aws_s3_key, aws_s3_secret)

    # Try to load fsx_config from src_bucket, fallback to dest_bucket if not found
    config = None
    for bucket in [src_bucket, dest_bucket]:
        try:
            result = s3_client.get_object(Bucket=bucket, Key=fsx_config_path)
            config = result["Body"].read().decode()
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                airflow_logger.warning(f"could not get file: {fsx_config_path} in bucket {bucket}")
                continue
            else:
                raise
        except KeyError as e:
            airflow_logger.error(f"could not get key Body from result: {e}")
            raise
    if config is None:
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, f"Could not find {fsx_config_path} in either {src_bucket} or {dest_bucket}")

    fsx_config = json.loads(config)

    response = s3_client.get_bucket_location(Bucket=src_bucket)
    s3_region = response.get("LocationConstraint")
    airflow_logger.info(f"Using S3 region: {s3_region}")

    fsx_client = create_fsx_client(aws_s3_key, aws_s3_secret, s3_region)

    file_size_in_gb = 0
    for cfh_key, one_a_key in unmerged_cfh_1a_pairs:
        file_size_in_gb += calculate_s3_prefix_size_gb(s3_client, src_bucket, cfh_key)
        file_size_in_gb += calculate_s3_prefix_size_gb(s3_client, dest_bucket, one_a_key)

    airflow_logger.info(f"Total size of unmerged CFH-1A pairs in GB is: {file_size_in_gb}")

    fsx_memory_capacity = 0
    if file_size_in_gb > 0:
        fsx_memory_capacity = math.ceil((4 * file_size_in_gb) / 2400) * 2400
    airflow_logger.info(f"Total assigned FSx filesystem memory in GB is: {fsx_memory_capacity}")

    aws_batch_filesystem_dict = create_fsx_filesystem_v2(
        fsx_client, fsx_config, src_bucket, dest_bucket, fsx_memory_capacity
    )

    # push xcom value
    context = get_current_context()
    ti = context["ti"]
    ti.xcom_push("fsx_file_system_id", aws_batch_filesystem_dict["FileSystemID"])

    return aws_batch_filesystem_dict


@inject
@task()
def create_filesystem_v3(
    key: str, secret: str, region: str, prefixes: list[str], *, sleep_interval: int = 10
) -> dict[Literal["FileSystemID", "FileSystemMountPath"], str]:
    # pull run time configs
    ctx = get_current_context()
    ti: TaskInstance = ctx["ti"]
    conf = ctx["dag_run"].conf

    logger = ti.log
    logger.setLevel(logging.DEBUG)

    logger.debug(f"{conf=}")

    bucket = conf.get("bucket")
    if not bucket:
        raise ValueError("Cannot create FSx filesystem without bucket in DAG config. Exiting")

    fsx_name = conf.get("fsx_name")

    file_system_id: str | None = conf.get("FileSystemID")
    file_system_mountpath: str | None = conf.get("FileSystemMountPath")

    # Short-circuit FSx creation if FileSystemID and FileSystemMountPath are provided
    if file_system_id and file_system_mountpath:
        logger.info("Using FSx provided in DAG config")
        file_system_dict: dict[Literal["FileSystemID", "FileSystemMountPath"], str] = {
            "FileSystemID": file_system_id,
            "FileSystemMountPath": file_system_mountpath,
        }

        # push xcom value
        ti.xcom_push("fsx", file_system_dict)
        ti.xcom_push("fsx_file_system_id", file_system_id)

        return file_system_dict

    if not prefixes:
        raise ValueError("No prefix path provided or no logs ready for unpack. Cannot proceed with FSx creation")

    logger.info(f"Creating FSx filesystem for {len(prefixes)} prefixes")
    logger.debug(f"{prefixes=}")

    fsx_config_key = conf.get("fsx_config_path")
    if not fsx_config_key:
        fsx_config_key = "Configs/fsx-configs/fsx_config.json"
        logger.warning(f"No FSx config path provided in DAG config. Using default path: {fsx_config_key}")

    s3_client = create_s3_client(key, secret)
    fsx_client = create_fsx_client(key, secret, region)

    try:
        fsx_config: dict[str, Any] = s3_client.get_object(Bucket=bucket, Key=fsx_config_key)
    except ClientError as e:
        raise ValueError(f"FSx config path ({fsx_config_key}) is not valid: {e}") from e

    try:
        fsx_config: dict[str, Any] = json.loads(fsx_config["Body"].read().decode("utf-8"))
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"FSx config path ({fsx_config_key}) is not valid: {e}") from e

    total_file_size = 0
    for prefix in prefixes:
        # Handle prefixes coming in as S3 URI or as prefix
        if prefix.startswith("s3://"):
            _, file_key = split_s3url_to_bucket_key(prefix)
        else:
            file_key = prefix
        total_file_size += calculate_s3_prefix_size_gb(s3_client, bucket, file_key)

    if total_file_size == 0:
        total_file_size = 1

    def get_fsx_memory_capacity(file_size: float, multiplier: int = 4, chunk_size: int = 1_200) -> int:
        num_chunks = math.ceil((multiplier * file_size) / chunk_size)
        return num_chunks * chunk_size

    fsx_memory_capacity = get_fsx_memory_capacity(total_file_size)

    logger.info(f"Total size of all staging prefixes: {total_file_size}")
    logger.info(f"Assigned FSx filesystem memory: {fsx_memory_capacity}")

    file_system_dict = create_fsx_filesystem(
        fsx_client,
        fsx_config,
        bucket,
        fsx_memory_capacity,
        fsx_name=fsx_name,
        sleep_interval=sleep_interval,
        logger=logger,
    )

    # push xcom value
    ti.xcom_push("fsx", file_system_dict)
    ti.xcom_push("fsx_file_system_id", file_system_dict["FileSystemID"])

    return file_system_dict


@inject
@task()
def compute_s3_staging_and_unpacked_prefixes(
    aws_s3_key: str,
    aws_s3_secret: str,
    staging_prefix_list: List[str],
):
    """copies raw hlogs and ptags from s3 staging to s3 unpacked dir if already not unpacked

    Args:
        s3_key (str): s3 key for Trex user to access s3 and rds
        s3_secret (str): s3 secret access for Trex user to access s3 and rds
        raw_data_staging_prefix_list (List[str]): List of raw data staging s3 prefix paths.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    user_config_bucket = conf.get("bucket", "")
    if user_config_bucket != "":
        s3_bucket = user_config_bucket

    s3_client = create_s3_client(aws_s3_key, aws_s3_secret)
    unpacked_prefix_list = []
    for staging_prefix in staging_prefix_list:
        airflow_logger.info(f"run_unpacking_pipeline_job on {staging_prefix}")
        # get ptag paths in staging prefix
        json_paths = get_file_paths(s3_client, s3_bucket, staging_prefix, set(["json"]))
        ptag_path = ""
        for json_path in json_paths:
            _, tag_file_key = split_s3url_to_bucket_key(json_path)
            if not is_json_file_in_s3_valid(s3_client, s3_bucket, tag_file_key):
                airflow_logger.warning("invalid json file in s3: {}".format(tag_file_key))
                continue
            elif is_json_file_in_s3_ptag(s3_client, s3_bucket, tag_file_key):
                ptag_path = tag_file_key
                break

        if not ptag_path:
            airflow_logger.info("Ptag is missing, staging prefix {} is skipped : ".format(staging_prefix))
            continue

        airflow_logger.info("Staging prefix to be unpacked :{}".format(staging_prefix))
        unpacked_prefix = get_unpacked_prefix_from_ptag(s3_client, s3_bucket, ptag_path)
        if not unpacked_prefix:
            airflow_logger.warning(f"unpacked_prefix: {unpacked_prefix} from ptag_path: {ptag_path}")
        if unpacked_prefix not in unpacked_prefix_list:
            unpacked_prefix_list.append(unpacked_prefix)

    return dict(zip(staging_prefix_list, unpacked_prefix_list))


@inject
@task()
def unpack_aws_data_from_s3_staging(
    s3_key: str,
    s3_secret: str,
    s3_bucket: str,
    aws_batch_region: str,
    aws_batch_filesystem_dict: Dict,
    preset_prefix_path: str = "",
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """copies raw hlogs and ptags from s3 staging to s3 unpacked dir if already not unpacked

    Args:
        s3_key (str): s3 key for Trex user to access s3 and rds
        s3_secret (str): s3 secret access for Trex user to access s3 and rds
        s3_bucket (str): [description]
        aws_batch_region (str): [description]
        preset_prefix_path (str, optional): prefix path to filter raw hlog data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "Staging")
    if prefix_path == "":
        prefix_path = preset_prefix_path
    test_run = conf.get("test_run", False)
    test_container = conf.get("test_container", False)
    integrity_config_path = conf.get("integrity_config_path", "")
    ros_conversion_config_path = conf.get("ros_conversion_config_path", "")
    if integrity_config_path == "":
        integrity_config_path = "Configs/log-integrity-configs/LogIntegrityConfig.json"
        airflow_logger.warning(
            f"integrity config path not set, passing the default config path: {integrity_config_path}"
        )
    if ros_conversion_config_path == "":
        airflow_logger.info(
            f"ros conversion config path not set, rosbag conversion step shall be skipped"
        )
    unpack_type = conf.get("unpack_type", "")
    # unpack types ["all", "snapshot", "complete"]
    if not unpack_type:
        unpack_type = "all"
    user_config_bucket = conf.get("bucket", "")
    if user_config_bucket != "":
        s3_bucket = user_config_bucket

    if s3_bucket is None:
        raise Exception(f"Bucket missing in the dag config. Exiting")

    batch_job_name = get_batch_job_name("Fsx-UNPACKING")

    airflow_logger.info("unpack_aws_data_from_s3_staging: {} with conf: {}".format(s3_bucket, conf))
    airflow_logger.info(f"test run mode: {test_run} test container mode: {test_container}")

    s3_client = create_s3_client(s3_key, s3_secret)
    batch_client = create_batch_client_v2(s3_key, s3_secret, aws_batch_region)
    sts_client = create_sts_client(s3_key, s3_secret, aws_batch_region)

    # Extract aws account information for submitting batch job with job role ARN and execution role ARN
    response = sts_client.get_caller_identity()
    aws_account = response["Account"]

    # log file ext for hlog
    base_log_file_ext = set(["hlog"])

    # get paths for HLOGs from s3
    raw_log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, base_log_file_ext)
    airflow_logger.info("Length of staging raw_log_paths found in s3: {}".format(len(raw_log_paths)))

    # get raw data s3 staging file paths ready to be unpacked
    unpack_ready_raw_data_staging_paths = get_unpack_ready_raw_data_staging_paths(
        raw_log_paths, raw_data_staging_service
    )
    s3_locs_to_be_unpacked = set()
    for raw_data_staging_path in unpack_ready_raw_data_staging_paths:
        _, key = split_s3url_to_bucket_key(raw_data_staging_path)
        staging_base_dir = os.path.split(key)[0]
        s3_locs_to_be_unpacked.add(staging_base_dir)

    airflow_logger.info(
        "Length of raw data s3 staging file paths ready to be unpacked in db: {}".format(len(s3_locs_to_be_unpacked))
    )

    # if test_run flag, only report unpacking step
    if test_run:
        return

    # submit job to unpack pipeline
    unpacked_job_dict = {"jobId": [], "StagingPrefix": [], "FileSystemID": [], "JobStatus": []}
    for staging_prefix in s3_locs_to_be_unpacked:
        airflow_logger.info(f"run_unpacking_pipeline_job on {staging_prefix}")
        # get ptag paths in staging prefix
        json_paths = get_file_paths(s3_client, s3_bucket, staging_prefix, set(["json"]))
        ptag_path = ""
        for json_path in json_paths:
            _, tag_file_key = split_s3url_to_bucket_key(json_path)
            if not is_json_file_in_s3_valid(s3_client, s3_bucket, tag_file_key):
                airflow_logger.warning("invalid json file in s3: {}".format(tag_file_key))
                continue
            elif is_json_file_in_s3_ptag(s3_client, s3_bucket, tag_file_key):
                ptag_path = tag_file_key
                break

        if not ptag_path:
            airflow_logger.info("Ptag is missing, staging prefix {} is skipped : ".format(staging_prefix))
            continue

        airflow_logger.info("Staging prefix to be unpacked :{}".format(staging_prefix))
        unpacked_prefix = get_unpacked_prefix_from_ptag(s3_client, s3_bucket, ptag_path)
        if not unpacked_prefix:
            airflow_logger.warning(f"unpacked_prefix: {unpacked_prefix} from ptag_path: {ptag_path}")

        airflow_logger.info("Unpacked prefix extracted from ptag is {}: ".format(unpacked_prefix))
        response = run_unpacking_pipeline_job_in_batch(
            batch_client,
            s3_bucket,
            staging_prefix,
            unpacked_prefix,
            batch_job_name,
            aws_account,
            aws_batch_region,
            aws_batch_filesystem_dict["FileSystemID"],
            aws_batch_filesystem_dict["FileSystemMountPath"],
            integrity_config_path,
            ros_conversion_config_path,
            unpack_type,
            test_container=test_container,
        )

        try:
            job_id = response["jobId"]
            unpacked_job_dict["jobId"].append(job_id)
            unpacked_job_dict["StagingPrefix"].append(staging_prefix)
            if aws_batch_filesystem_dict.get("FileSystemID") not in unpacked_job_dict["FileSystemID"]:
                unpacked_job_dict["FileSystemID"].append(aws_batch_filesystem_dict["FileSystemID"])
        except KeyError as e:
            airflow_logger.error("run_unpacking_pipeline_job_in_batch response key error")
            raise KeyError(e)

    if len(unpacked_job_dict["FileSystemID"]) == 0:
        unpacked_job_dict["FileSystemID"].append(aws_batch_filesystem_dict["FileSystemID"])

    return unpacked_job_dict


@inject
@task()
def update_unpacked_status_in_db(
    unpacked_job_status_dict: Dict,
    aws_s3_bucket: str,
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_hook: AwsBatchClientHook,
    aws_batch_region: str,
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
) -> dict[str, Any] | None:
    """updates raw data staging table with unpacked column if successfully unpacked

    Args:
        unpacked_job_status_dict ({"jobId": [], "StagingPrefix": []}): [description]
        aws_batch_key (str): [description]
        aws_batch_secret (str): [description]
        aws_batch_token (str): [description]
        aws_batch_region (str): [description].
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    ti: TaskInstance = ctx["ti"]

    aws_s3_bucket = conf.get("bucket", aws_s3_bucket)
    if not aws_s3_bucket:
        raise ValueError("Bucket missing in the DAG config. Exiting")

    logger = ti.log
    logger.setLevel(logging.DEBUG)

    # Guard clause - early return for invalid input
    if not unpacked_job_status_dict or not unpacked_job_status_dict.get("jobId", []):
        logger.warning("Either unpacked job status dict is empty or contains no job IDs.")
        return None

    batch_client = create_batch_client_v2(aws_batch_key, aws_batch_secret, aws_batch_region)
    fsx_client = create_fsx_client(aws_batch_key, aws_batch_secret, aws_batch_region)

    update_db_status_dict = {"FileSystemID": "", "UnpackedRawDataId": []}
    poll_job_status_to_release_data(aws_batch_hook, unpacked_job_status_dict, batch_client, fsx_client)

    all_unpacked_ids: list[int] = []

    with Context().new_session_scope():
        for dict_idx in range(len(unpacked_job_status_dict["jobId"])):
            status_response = batch_client.describe_jobs(jobs=unpacked_job_status_dict["jobId"])
            job = status_response["jobs"][dict_idx]
            job_description = aws_batch_hook.get_job_description(job["jobId"])
            job_status = job_description.get("status")

            logger.info(f"job_description: {job_description}")
            unpacked_job_status_dict["JobStatus"].append(job_status)

            if job_status == "SUCCEEDED":
                # get raw data s3 staging file paths ready to be unpacked
                staging_prefix = unpacked_job_status_dict["StagingPrefix"][dict_idx]
                logger.debug(f"Staging prefix to update db: {staging_prefix}")
                staging_paths = raw_data_staging_service.get_all_raw_data_staging_paths(
                    aws_s3_bucket, staging_prefix
                )
                unpacked_ids = update_raw_data_staging_table_with_unpacked(
                    staging_paths, raw_data_staging_service
                )
                all_unpacked_ids.extend(unpacked_ids)

                logger.debug(f"For {staging_prefix=}: {staging_paths=}")
                logger.debug(f"For {staging_prefix=}: {unpacked_ids=}")

                logger.info(f"Raw data staging ids of the unpacked logs are: {unpacked_ids}")

        update_db_status_dict["FileSystemID"] = unpacked_job_status_dict["FileSystemID"]
        update_db_status_dict["UnpackedRawDataId"] = all_unpacked_ids

        if ("FAILED" in unpacked_job_status_dict["JobStatus"]) or ("RUNNING" in unpacked_job_status_dict["JobStatus"]):
            raise RuntimeError(f"Some of the Jobs may have failed job description: {unpacked_job_status_dict}")

    return update_db_status_dict


@inject
@task()
def update_unpacked_status_in_db_v2(
    unpacked_job_status_dict: dict[Literal["FileSystemID", "StagingPrefix", "JobStatus", "jobId"], Any],
    *,
    key: str,
    secret: str,
    region: str,
    aws_batch_hook: AwsBatchClientHook,
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
) -> dict[Literal["FileSystemID", "UnpackedRawDataId"], Any] | None:
    """Update raw data staging table with unpacked column, if successfully unpacked.

    Args:
        key (str): S3/Batch access key
        secret (str): S3/Batch secret
        region (str): AWS region
        aws_batch_hook (AwsBatchClientHook): Airflow hook to interact with AWS Batch
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    ti: TaskInstance = ctx["ti"]

    bucket = conf.get("bucket")
    if not bucket:
        raise ValueError("Bucket missing in the DAG config. Exiting")

    logger = ti.log
    logger.setLevel(logging.DEBUG)

    # Guard clause - early return for invalid input
    if not unpacked_job_status_dict or not unpacked_job_status_dict.get("jobId", []):
        logger.warning("Either unpacked job status dict is empty or contains no job IDs.")
        return None

    ptag_path: str | None = conf.get("ptag_path")
    if not ptag_path:
        raise ValueError(f"Cannot update specific split files without ptag path provided.")

    s3_client = create_s3_client(key, secret)
    batch_client = create_batch_client_v2(key, secret, region)
    fsx_client = create_fsx_client(key, secret, region)

    unpacked_prefix = get_unpacked_prefix_from_ptag(s3_client, bucket=bucket, json_path=ptag_path)

    poll_job_status_to_release_data(aws_batch_hook, unpacked_job_status_dict, batch_client, fsx_client)

    all_unpacked_ids: list[int] = []
    for idx, job_id in enumerate(unpacked_job_status_dict["jobId"]):
        # Only describe the current job
        status_response = batch_client.describe_jobs(jobs=[job_id])

        try:
            job = status_response["jobs"][0]
        except IndexError:
            logger.error(f"Job {job_id} not found in the response. Skipping.")
            continue

        job_description = aws_batch_hook.get_job_description(job["jobId"])
        job_status = job_description.get("status")

        logger.info(f"For {job_id=}: {job_description=}")
        unpacked_job_status_dict["JobStatus"].append(job_status)

        if job_status != "SUCCEEDED":
            continue

        # get raw data s3 staging file paths ready to be unpacked
        staging_prefix = unpacked_job_status_dict["StagingPrefix"][idx]

        unpacked_ids: list[int] = []

        split_files_key = f"{unpacked_prefix}/split_files.json"
        try:
            s3_client.head_object(Bucket=bucket, Key=split_files_key)

            split_files = json_file_in_s3_to_dict(s3_client, bucket=bucket, obj_key=split_files_key)
            if not isinstance(split_files, list):
                raise ValueError(f"Expected split files to be a list, got {type(split_files)} for {split_files_key=}")

            # If we have individual split files available for a job ID, we only mark those split files as Unpacked
            for split_file in split_files:
                if staging_prefix.endswith("/"):
                    staging_prefix = staging_prefix[:-1]  # Remove trailing slash if present

                split_file_key = f"{staging_prefix}/{split_file}"
                split_file_path = get_s3_url_from_bucket_and_prefix(aws_s3_bucket=bucket, prefix_path=split_file_key)
                if split_file_path.endswith("/"):
                    split_file_path = split_file_path[:-1] # Remove trailing slash if present

                # Provide DB context or the DB will not be updated
                with Context().new_session_scope():
                    unpacked_id = raw_data_staging_service.update_raw_data_staging_with_unpacked(split_file_path)

                unpacked_ids.append(unpacked_id)
        except ClientError:
            logger.warning(
                f"Individual split files not present for {job_id=}. Marking entire {staging_prefix=} as unpacked."
            )
            # make it so that all splits are marked as unpacked
            conf["filter_all_split"] = True

        if conf.get("filter_all_split", False):
            logger.info(f"Unpacking all split files. Marking entire {staging_prefix=} as unpacked")
            logger.debug(f"Staging prefix to update db: {staging_prefix}")
            with Context().new_session_scope():
                staging_paths = raw_data_staging_service.get_all_raw_data_staging_paths(
                    bucket, staging_prefix
                )
                unpacked_ids = update_raw_data_staging_table_with_unpacked(
                    staging_paths, raw_data_staging_service
                )

            logger.debug(f"For {staging_prefix=}: {staging_paths=}")

        logger.info(f"Raw data staging IDs of the unpacked logs are: {unpacked_ids}")
        all_unpacked_ids.extend(unpacked_ids)

    if ("FAILED" in unpacked_job_status_dict["JobStatus"]) or ("RUNNING" in unpacked_job_status_dict["JobStatus"]):
        raise RuntimeError(f"Some jobs might have FAILED: {unpacked_job_status_dict}")

    return {
        "FileSystemID": unpacked_job_status_dict["FileSystemID"], "UnpackedRawDataId": all_unpacked_ids
    }



@inject
@task()
def update_unzip_status_in_db(
    unzip_job_status_dict: Dict,
    aws_s3_bucket: str,
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_hook: str,
    aws_batch_region: str,
    zip_data_service: ZipDataService = Provide[Container.zip_data_service],
):
    """
    Updates ZipData table with unzip_status and unzipped_file_location if successfully unzipped.

    Args:
        unzip_job_status_dict ({"jobId": [], "unzipPrefix": []}): Batch job info.
        aws_batch_key (str): AWS Batch key.
        aws_batch_secret (str): AWS Batch secret.
        aws_batch_hook (str): AWS Batch hook.
        aws_batch_region (str): AWS region.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    # Guard clause - early return for invalid input
    if not unzip_job_status_dict or "jobId" not in unzip_job_status_dict:
        airflow_logger.warning("unzip_job_status_dict is None. No jobs to process.")
        return

    batch_client = create_batch_client_v2(aws_batch_key, aws_batch_secret, aws_batch_region)
    
    # Collect failed jobs instead of raising immediately
    failed_jobs = []
    successful_jobs = []
    
    with Context().new_session_scope() as session:
        for dict_idx in range(len(unzip_job_status_dict["jobId"])):
            job_id = unzip_job_status_dict["jobId"][dict_idx]
            
            try:
                status_response = batch_client.describe_jobs(jobs=[job_id])
                job = status_response["jobs"][0]
                job_description = aws_batch_hook.get_job_description(job["jobId"])
                job_status = job_description.get("status")

                airflow_logger.info(f"job_description: {job_description}")
                unzip_job_status_dict.setdefault("JobStatus", []).append(job_status)
                aws_batch_hook.wait_for_job(job["jobId"])
                
                # process successful jobs, collect failed ones
                if not aws_batch_hook.check_job_success(job["jobId"]):
                    failed_jobs.append({
                        "job_id": job["jobId"],
                        "status": job_status,
                        "prefix": unzip_job_status_dict["unzipPrefix"][dict_idx]
                    })
                    airflow_logger.error(f"Job {job['jobId']} failed with status {job_status}.")
                    continue

                # Process successful job
                unzip_prefix = unzip_job_status_dict["unzipPrefix"][dict_idx]
                zip_data_paths_to_update = zip_data_service.get_all_zip_data_paths(aws_s3_bucket, unzip_prefix)
                airflow_logger.info(f"zip_data_paths_to_update: {zip_data_paths_to_update}")
                
                for zip_path in zip_data_paths_to_update:
                    # Remove extension from unzip_prefix for unzipped_location
                    unzipped_location, _ = os.path.splitext(zip_path)
                    zip_data_id = zip_data_service.update_zip_data_with_unzip_status_and_location(
                        zip_path, unzipped_location
                    )
                    airflow_logger.info(f"Zip file {zip_path} unzipped to {unzipped_location} (id: {zip_data_id})")
                
                successful_jobs.append({
                    "job_id": job["jobId"],
                    "prefix": unzip_prefix,
                })
                
            except Exception as e:
                # Capture any unexpected errors during processing
                failed_jobs.append({
                    "job_id": job_id,
                    "prefix": unzip_job_status_dict["unzipPrefix"][dict_idx],
                })
                airflow_logger.error(f"Error processing job {job_id}: {e}")
                continue

    airflow_logger.info(f"Processing complete. Successful jobs: {len(successful_jobs)}, Failed jobs: {len(failed_jobs)}")
    

@inject
@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_filesystem(
    aws_fsx_key: str,
    aws_fsx_secret: str,
    aws_fsx_region: str,
    update_db_status_dict: Dict,
):
    """deletes fsx file system

    Args:
        aws_fsx_key (str): [description]
        aws_fsx_secret (str): [description]
        aws_fsx_region (str): [description].
        aws_batch_filesystem_dict (Dict): [description].
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    retain_fsx = conf.get("retain_fsx", False)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    fsx_client = create_fsx_client(aws_fsx_key, aws_fsx_secret, aws_fsx_region)

    if retain_fsx:
        airflow_logger.info("Test run mode, skipping FSx filesystem deletion.")
        return

    if (not update_db_status_dict) or (not update_db_status_dict.get("FileSystemID")):
        # pull xcom value
        airflow_logger.info("Batch job failed, fetching file system id using xcom_pull")

        ti: TaskInstance = ctx["ti"]
        aws_file_system_id: str | None = (
            ti.xcom_pull(key="fsx_file_system_id", task_ids="create_filesystem_id") or
            ti.xcom_pull(key="fsx_file_system_id", task_ids="create_filesystem_v2") or
            ti.xcom_pull(key="fsx_file_system_id", task_ids="create_filesystem_v3")
        )
        if not aws_file_system_id:
            airflow_logger.error("Could not find FSx FileSystem ID in XCom")
            raise ValueError("FSx FileSystem ID not found for deletion")

        delete_fsx_filesystem(fsx_client, aws_file_system_id)
        airflow_logger.info("Batch job failed, deleting the Fsx file system {}".format(aws_file_system_id))
    else:
        for aws_file_system_id in update_db_status_dict["FileSystemID"]:
            airflow_logger.info("Deleting Fsx file system with id {} ".format(aws_file_system_id))
            delete_fsx_filesystem(fsx_client, aws_file_system_id)
            airflow_logger.info("Fsx file system {} is succesfully deleted ".format(aws_file_system_id))


@inject
@task()
def extract_aws_data_from_s3_unpacked(
    s3_key: str,
    s3_secret: str,
    aws_batch_region: str,
    aws_batch_filesystem_dict: Dict,
    preset_prefix_path: str = "",
):
    """extracts images, pcds and json files from hlogs from baselogs/snapshots/snippets in unpacked dir

    Args:
        s3_key (str): s3 key for Trex user to access s3 and rds
        s3_secret (str): s3 secret access for Trex user to access s3 and rds
        aws_batch_region (str): [description]
        aws_batch_filesystem (str): [description]
        preset_prefix_path (str, optional): prefix path to filter the TransmissionTimeIndex of hlog data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")

    if prefix_path == "":
        prefix_path = preset_prefix_path
    test_run = conf.get("test_run", False)

    s3_bucket = conf.get("bucket", "")

    extraction_prefix = conf.get("extraction_prefix", "")

    ros_conversion_prefix = conf.get("ros_conversion_prefix", "")

    batch_job_name = get_batch_job_name("Fsx-EXTRACTION")
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    airflow_logger.info("extract_aws_data_from_s3_unpacked: {} with conf: {}".format(s3_bucket, conf))

    s3_client = create_s3_client(s3_key, s3_secret)
    batch_client = create_batch_client_v2(s3_key, s3_secret, aws_batch_region)
    sts_client = create_sts_client(s3_key, s3_secret, aws_batch_region)

    # Extract aws account information for submitting batch job with job role ARN and execution role ARN
    response = sts_client.get_caller_identity()
    aws_account = response["Account"]

    # log file ext for hlog
    base_log_file_ext = set(["hlog"])

    # get paths for HLOGs from s3
    raw_log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, base_log_file_ext)
    airflow_logger.info("Length of raw_log_paths found in s3: {}".format(len(raw_log_paths)))

    # get raw data s3 staging file paths ready to be extracted
    s3_locs_to_be_extracted = []
    for raw_data_path in raw_log_paths:
        bucket, key = split_s3url_to_bucket_key(raw_data_path)
        s3_filename = os.path.split(key)[1]
        matched_s3_loc = get_extraction_index_fullpath(s3_filename)
        if matched_s3_loc and s3_filename != "CommentIndex00000.hlog":
            s3_locs_to_be_extracted.append(key)

    airflow_logger.info(
        "Length of raw data s3 file paths ready to be extracted in db: {}".format(len(s3_locs_to_be_extracted))
    )

    # if test_run flag, only report extracting step
    if test_run:
        return

    # submit job to extract pipeline
    extract_job_dict = {"jobId": [], "UnpackedPrefix": [], "FileSystemID": []}
    for unpacked_prefix in s3_locs_to_be_extracted:
        airflow_logger.info("run_ais_extraction_job")
        airflow_logger.info("unpacked prefix to be extracted :{}".format(unpacked_prefix))
        response = run_extraction_pipeline_job_in_batch(
            batch_client,
            s3_bucket,
            unpacked_prefix,
            extraction_prefix,
            ros_conversion_prefix,
            batch_job_name,
            aws_account,
            aws_batch_region,
            aws_batch_filesystem_dict["FileSystemMountPath"],
        )
        job_id = response["jobId"]
        extract_job_dict["jobId"].append(job_id)
        extract_job_dict["UnpackedPrefix"].append(unpacked_prefix)
        if aws_batch_filesystem_dict.get("FileSystemID") not in extract_job_dict["FileSystemID"]:
            extract_job_dict["FileSystemID"].append(aws_batch_filesystem_dict["FileSystemID"])

    return extract_job_dict


@inject
@task()
def wait_for_extraction_job_completion(
    extract_job_dict: {"jobId": [], "StagingPrefix": [], "FileSystemID": []},
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_hook: str,
    aws_batch_region: str,
):
    """updates raw data staging table with unpacked column if successfully unpacked

    Args:
        unpacked_job_status_dict ({"jobId": [], "StagingPrefix": []}): [description]
        aws_batch_key (str): [description]
        aws_batch_secret (str): [description]
        aws_batch_token (str): [description]
        aws_batch_region (str): [description].
    """
    batch_client = create_batch_client_v2(aws_batch_key, aws_batch_secret, aws_batch_region)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    for dict_idx in range(len(extract_job_dict["jobId"])):
        status_response = batch_client.describe_jobs(jobs=extract_job_dict["jobId"])
        job = status_response["jobs"][dict_idx]
        aws_batch_hook.wait_for_job(job["jobId"])
        aws_batch_hook.check_job_success(job["jobId"])

    fsx_file_system_id = extract_job_dict["FileSystemID"][0]
    airflow_logger.info(
        "Extraction jobs have been completed, filesystem returned for deletion is: {}".format(fsx_file_system_id)
    )

    return fsx_file_system_id


def concurrent_copy_s3_objects(
    s3_client: BaseClient, s3_bucket: str, source_key: str, destination_key: str
) -> Tuple[str, Dict[str, Any]]:
    copy_result = s3_client.copy_object(
        CopySource={"Bucket": s3_bucket, "Key": source_key}, Bucket=s3_bucket, Key=destination_key
    )
    time.sleep(0.1) # add a small delay to avoid throttling
    return source_key, copy_result


def concurrent_delete_s3_objects(
    s3_client: BaseClient, s3_bucket: str, source_key: str, destination_key: str
) -> Tuple[str, Optional[Dict[str, Any]]]:
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    try:
        s3_client.head_object(Bucket=s3_bucket, Key=destination_key)
    except ClientError as e:
        airflow_logger.warning(f"Archive key '{destination_key}' does not exist in Archive: {e}")
        return source_key, None

    response: Dict[str, Any] = s3_client.delete_object(Bucket=s3_bucket, Key=source_key)
    time.sleep(0.1)
    if response.get("DeleteMarker", False):
        airflow_logger.info(f"Successfully deleted {source_key} from bucket {s3_bucket} after archiving")
    else:
        airflow_logger.info(f"Deletion of {source_key} from bucket {s3_bucket} after archiving unsuccessful")

    return source_key, response


@inject
@task()
def migrate_files_in_s3_to_deep_archive(
    s3_key: str,
    s3_secret: str,
    s3_bucket: str,
    s3_region: str,
    preset_prefix_path: str = "Staging/",  # Default to Staging. This task should not run outside of Staging/
    raw_data_staging_service: RawDataStagingService = Provide[Container.raw_data_staging_service],
):
    """Migrates data in s3 staging to deep archive if data already moved to unpacked dir.

    Args:
        s3_key (str): S3 key to create an S3 client
        s3_secret (str): S3 secret to create an S3 client
        s3_bucket (str): S3 bucket to migrate data from
        preset_prefix_path (str, optional): Prefix path to filter where files should be archived from. Defaults to "Staging/"
    """
    del s3_region  # unused

    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf

    prefix_path: str = conf.get("prefix_path", preset_prefix_path)
    if not prefix_path:
        raise ValueError("Cannot run Archive DAG without a prefix path specified")

    # allow mentioning "Directory" instead of "Directory/"
    if not prefix_path.endswith("/"):
        prefix_path = prefix_path + "/"

    if not prefix_path.startswith("Staging/"):
        raise ValueError("Archive DAG is only allowed to archive files from Staging/ directory")

    s3_bucket = conf.get("bucket", s3_bucket)
    if not s3_bucket:
        raise ValueError("Cannot continue without bucket provided. Exiting")

    log_level_debug = conf.get("log_level_debug", False)

    # make max_workers configurable
    max_workers = conf.get("max_workers", 20)
    max_workers = int(max_workers) if max_workers else 20
    max_workers = min(max_workers, 20)  # Limit max_workers to a reasonable number

    logger: logging.Logger = ctx["ti"].log
    if log_level_debug:
        logger.setLevel(logging.DEBUG)

    logger.info(f"Migrate files in S3 to deep archive from {s3_bucket} with {prefix_path=}")
    logger.debug(f"{conf=}")

    s3_client = create_s3_client(s3_key, s3_secret)

    # log file ext for hlog
    base_log_file_extensions = {"hlog"}

    # get paths for HLOGs from s3
    raw_log_paths = get_file_paths(s3_client, s3_bucket, prefix_path, base_log_file_extensions)
    logger.info(f"Length of log paths found in prefix: {len(raw_log_paths)}")

    # get raw data s3 staging file paths ready to be unpacked
    archive_ready_log_paths = get_archive_ready_raw_data_staging_paths(raw_log_paths, raw_data_staging_service)
    logger.info(f"Length of archive ready log paths in prefix: {len(archive_ready_log_paths)}")
    logger.debug(f"{archive_ready_log_paths=}")

    archive_ready_prefixes = get_common_staging_prefixes(archive_ready_log_paths)
    archive_ready_prefixes = [split_s3url_to_bucket_key(prefix)[1] for prefix in archive_ready_prefixes]
    logger.info(f"Length of archive ready prefixes: {len(archive_ready_prefixes)}")
    logger.debug(f"{archive_ready_prefixes=}")

    # Get additional files, from the prefixes which have been unpacked
    archive_ready_paths: set[str] = set()
    for archive_ready_prefix in archive_ready_prefixes:
        archive_ready_paths.update(
            get_file_paths(s3_client, s3_bucket, archive_ready_prefix, extensions=set())
        )

    # Don't include the log paths themselves
    additional_file_paths = archive_ready_paths - set(archive_ready_log_paths)

    logger.info(f"Length of additional files found in unpack ready prefixes: {len(additional_file_paths)}")
    logger.debug(f"{additional_file_paths=}")

    logger.info(f"Length of all archive ready paths: {len(archive_ready_paths)=}")
    logger.debug(f"{archive_ready_paths=}")

    s3_keys_to_be_archived: set[str] = set()
    for raw_data_staging_path in archive_ready_paths:
        _, key = split_s3url_to_bucket_key(raw_data_staging_path)
        if key not in s3_keys_to_be_archived:
            s3_keys_to_be_archived.add(key)

    # If no files to archive, return early
    if not s3_keys_to_be_archived:
        logger.info("No files to archive. Exiting the task.")
        return

    logger.info(f"Length of S3 keys to be archived: {len(s3_keys_to_be_archived)}")
    logger.debug(f"{s3_keys_to_be_archived=}")

    if conf.get("test_run", False):
        logger.info(f"Test run mode. Skipping actual file archival")
        return

    # Copy over files to archive directory
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: list[Future[Any]] = []
        for s3_filepath in s3_keys_to_be_archived:
            archive_destination_s3_filepath = get_archive_destination_from_s3_key(s3_filepath)
            logger.debug(f"Archive destination for {s3_filepath}: {archive_destination_s3_filepath}")
            future = executor.submit(
                concurrent_copy_s3_objects,
                s3_client=s3_client,
                s3_bucket=s3_bucket,
                source_key=s3_filepath,
                destination_key=archive_destination_s3_filepath,
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                s3_filepath, result = future.result()
                logger.debug(f"Successfully copied {s3_filepath=} to archive: {result}")
            except Exception as e:
                logger.error(f"An error occurred while copying {s3_filepath=} to archive directory: {e}")

    # Delete objects in Staging after copying the files to Archive, in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: list[Future[Any]] = []

        for s3_filepath in s3_keys_to_be_archived:
            # Skip over any files that are already in the Archive directory
            if s3_filepath.startswith("Archive/"):
                continue

            archive_destination_s3_filepath = get_archive_destination_from_s3_key(s3_filepath)
            future = executor.submit(
                concurrent_delete_s3_objects,
                s3_client=s3_client,
                s3_bucket=s3_bucket,
                source_key=s3_filepath,
                destination_key=archive_destination_s3_filepath,
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                s3_filepath, result = future.result()
                logger.debug(f"Successfully deleted {s3_filepath=} from staging: {result}")
            except Exception as e:
                logger.error(f"An error occurred during deletion of {s3_filepath=} from Staging directory: {e}")

    # Update raw data staging table with archived column and updated db paths from Staging to Archive
    removed_log_files: set[str] = set()
    with Context().new_session_scope() as session:
        for s3_file_key in s3_keys_to_be_archived:
            if Path(s3_file_key).suffix not in {".hlog", ".bag"}:
                logger.debug(f"Skipping non-log file for DB updation: {s3_file_key}")
                continue

            archive_destination_s3_file_key = get_archive_destination_from_s3_key(s3_file_key)
            try:
                if not s3_client.head_object(Bucket=s3_bucket, Key=archive_destination_s3_file_key):
                    logger.warning(
                        f"archive_key: '{archive_destination_s3_file_key}' does not exist in Archive for {s3_file_key=}. Skipping update."
                    )
                    continue
            except ClientError as e:
                logger.warning(
                    f"Error while querying archive key: {archive_destination_s3_file_key} for {s3_file_key=}: {e}"
                )
                continue

            # Update the raw data staging entry to mark it as archived
            try:
                raw_data_staging_service.update_raw_data_staging_with_archived(s3_file_key, session=session)
                removed_log_files.add(s3_file_key)
            # We raise a ValueError if the file is not found (might've been deleted from the DB manually etc.)
            except ValueError as e:
                logger.warning(f"Error updating raw data staging for {s3_file_key}: {e}")
                continue

            logger.debug(f"Updated entry: {s3_file_key} as Archived in DB")

    # Log the output in log_file.txt in staging
    log_locs = list({Path(loc).parent.as_posix() for loc in removed_log_files})
    for log_loc in log_locs:
        log_text = f"{datetime.now()}: The files in the current location have been successfully unpacked and archived to s3://{s3_bucket}/{get_archive_destination_from_s3_key(log_loc)}"
        log_file_key = f"{log_loc}/log_file.txt"
        s3_client.put_object(Body=log_text, Bucket=s3_bucket, Key=log_file_key)


@inject
@task()
def generate_report_for_unpacked_logs(
    s3_key: str,
    s3_secret: str,
    s3_bucket: str,
    aws_batch_region: str,
    aws_batch_filesystem_dict: Dict,
    aws_batch_hook: str,
):
    """copies raw hlogs and ptags from s3 staging to s3 unpacked dir if already not unpacked

    Args:
        s3_key (str): s3 key for Trex user to access s3 and rds
        s3_secret (str): s3 secret access for Trex user to access s3 and rds
        s3_bucket (str): [description]
        aws_batch_region (str): [description]
        preset_unpacked_path (str, optional): prefix path to filter raw hlog data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    unpacked_path_prefix = conf.get("unpacked_path_prefix", "")
    if unpacked_path_prefix != "":
        integrity_config_path = conf.get("integrity_config_path", "")
        if integrity_config_path == "":
            integrity_config_path = "Configs/log-integrity-configs/LogIntegrityConfig.json"

        user_config_bucket = conf.get("bucket", "")
        if user_config_bucket != "":
            s3_bucket = user_config_bucket

        batch_job_name = get_batch_job_name("REPORTGEN")

        airflow_logger.info("unpack_aws_data_from_s3_staging: {} with conf: {}".format(s3_bucket, conf))

        batch_client = create_batch_client_v2(s3_key, s3_secret, aws_batch_region)
        sts_client = create_sts_client(s3_key, s3_secret, aws_batch_region)

        # Extract aws account information for submitting batch job with job role ARN and execution role ARN
        response = sts_client.get_caller_identity()
        aws_account = response["Account"]

        s3_client = create_s3_client(s3_key, s3_secret)

        paginator = s3_client.get_paginator("list_objects_v2")
        results = paginator.paginate(Bucket=s3_bucket, Prefix=unpacked_path_prefix)

        airflow_logger.info(f"s3_bucket: {s3_bucket} unpacked_path_prefix: {unpacked_path_prefix}")

        folders = set()

        for result in results:
            contents = result.get("Contents")
            if not contents:
                key_count = result.get("KeyCount")
                if key_count == 0:
                    continue
                raise ValueError("get file paths no contents in result: {}".format(result))
            for content in contents:
                folders.add(content["Key"])

        airflow_logger.info(f"folders: {sorted(folders)}")

        unpack_paths = [x for x in folders if re.search("Complete_Log", x)]

        airflow_logger.info(unpack_paths)

        unpack_paths_filtered = [z[: z.rindex("Complete_Log") + 12] + "/" for z in unpack_paths]

        airflow_logger.info(unpack_paths_filtered)
        airflow_logger.info(len(unpack_paths_filtered))

        unpack_paths_filtered_set = set(unpack_paths_filtered)
        unpack_paths_unique_list = list(unpack_paths_filtered_set)

        airflow_logger.info(unpack_paths_unique_list)
        airflow_logger.info(len(unpack_paths_unique_list))

        job_id_list = []

        for unpacked_base_log in unpack_paths_unique_list:
            airflow_logger.info(unpacked_base_log)
            response = run_log_integrity_report_generation_job_in_batch(
                batch_client,
                s3_bucket,
                unpacked_base_log,
                batch_job_name,
                aws_account,
                aws_batch_region,
                aws_batch_filesystem_dict["FileSystemMountPath"],
                integrity_config_path,
            )
            job_id_list.append(response["jobId"])
            aws_batch_hook.wait_for_job(response["jobId"])

        job_status_list = []

        for job_id in job_id_list:
            job_description = aws_batch_hook.get_job_description(job_id)
            job_status = job_description.get("status")
            job_status_list.append(job_status)

        airflow_logger.info(f"job_description: {job_description}")

        if ("FAILED" in job_status_list) or ("RUNNING" in job_status_list):
            raise AirflowException(f"Some of the Jobs may have failed job description: {job_status_list}")

    else:
        airflow_logger.error("unpacked_path = ''")


@inject
@task()
def index_hlog_tag_from_s3(
    s3_key: str,
    s3_bucket: str,
    s3_secret: str,
    s3_region: str,
    aws_batch_hook: str,
    unpacked_job_status_dict: Dict,
    preset_prefix_path: str = "",
    hlog_tag_service: HlogTagService = Provide[Container.hlog_tag_service],
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
):
    """index data from s3 task to both raw and extracted data

    Args:
        s3_key (str): [description]
        s3_bucket (str): [description]
        s3_secret (str): [description]
        preset_prefix_path (str, optional): prefix path to filter data. This will be overridden by runtime config with key prefix_path. Defaults to "".
    """
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf

    prefix_path: str = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path

    test_run: bool = conf.get("test_run", False)
    log_level_debug: bool = conf.get("log_level_debug", False)
    blacklist_pattern = conf.get("blacklist_pattern", "")

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)

    batch_client = create_batch_client_v2(s3_key, s3_secret, s3_region)
    fsx_client = create_fsx_client(s3_key, s3_secret, s3_region)

    poll_aws_job_status(aws_batch_hook, unpacked_job_status_dict, batch_client)

    airflow_logger.info(f"index_data_from_s3: {s3_bucket} with conf: {preset_prefix_path}")

    s3_client = create_s3_client(s3_key, s3_secret)

    hltag_paths: List[str] = []

    # get hltag json files from s3
    json_paths = get_file_paths(s3_client, s3_bucket, prefix_path, set(["json"]), blacklist_pattern)
    hltag_paths = get_index_hltag_json_paths(s3_client, s3_bucket, json_paths)

    airflow_logger.info(f"hltag paths found: {hltag_paths}")

    # get indexed raw data paths already in cdcs
    indexed_hlog_tag_paths = get_indexed_hlog_tag_paths(hltag_paths, hlog_tag_service)
    airflow_logger.info("indexed hlog tag paths len: {}".format(len(indexed_hlog_tag_paths)))

    update_db_status_dict = {"FileSystemID": []}

    # if test_run flag, only report indexing step
    if test_run:
        return

    with Context().new_session_scope() as session:
        # open up a new session because we want to commit(persist) the data to the database for every file path
        # This will take longer time but it can prevent lost of data due to various reasons(we don't have a robust enough pipeline due to reasons such as network errors)
        airflow_logger.debug(f"new function session obj {session}")

        airflow_logger.info("start hltag ingest")

        s3_resource = create_s3_resource(s3_key, s3_secret, s3_region)

        # index hltags
        hltag_skip_count = 0
        for hltag_path in hltag_paths:

            _, key = split_s3url_to_bucket_key(hltag_path)

            base_log_path = get_base_log_path_from_data_path(hltag_path)
            airflow_logger.info(f"base_log_path: {base_log_path} for hltag_path: {hltag_path}")
            raw_data = raw_data_service.get_raw_data_by_file_path_like_op(base_log_path)

            airflow_logger.info("get Tag objects from hltag.json file")
            hltag_raw_data_event_objects = get_hltag_objects_from_json(key, s3_bucket, s3_resource, raw_data)

            if hltag_raw_data_event_objects is None or len(hltag_raw_data_event_objects) == 0:
                continue

            if hltag_path in indexed_hlog_tag_paths:
                hltag_raw_data_db_objects = hlog_tag_service.get_hlog_tags_by_file_path(hltag_path)
                for hltag_raw_data_event_object in hltag_raw_data_event_objects:
                    is_update = False
                    is_skip = False
                    for hltag_raw_data_db_object in hltag_raw_data_db_objects:
                        if hltag_raw_data_db_object != hltag_raw_data_event_object:
                            is_update = True
                            hltag_raw_data_db_object.json = hltag_raw_data_event_object.json
                            airflow_logger.info(
                                f"hltag_raw_data_event_object.raw_data_id: {hltag_raw_data_event_object.raw_data_id} "
                            )
                            if hltag_raw_data_db_object.raw_data_id == None:
                                hltag_raw_data_db_object.raw_data_id = hltag_raw_data_event_object.raw_data_id
                            break
                        elif hltag_raw_data_db_object == hltag_raw_data_event_object:
                            is_skip = True
                            airflow_logger.info(
                                f"{hltag_path} with {hltag_raw_data_event_object.name} already indexed. skipping.."
                            )
                            break

                    if is_update:
                        session.merge(hltag_raw_data_db_object)
                        airflow_logger.info(
                            f"{hltag_path} with {hltag_raw_data_event_object.name} updated with json: {hltag_raw_data_event_object.json}"
                        )
                        continue

                    if is_skip:
                        continue

                    if not is_update and not is_skip:
                        session.add(hltag_raw_data_event_object)
                        continue

            else:
                for hltag_raw_data_event_object in hltag_raw_data_event_objects:
                    session.add(hltag_raw_data_event_object)

        session.commit()

        update_db_status_dict["FileSystemID"] = unpacked_job_status_dict["FileSystemID"]
        airflow_logger.info(f"finish hltag ingest with {hltag_skip_count} skipped")

    return update_db_status_dict


@inject
@task()
def get_hltag_prefix_path(
    s3_key: str,
    s3_secret: str,
    aws_s3_bucket: str = "",
    preset_prefix_path: str = "",
):
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf

    prefix_path: str = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path

    s3_client = create_s3_client(s3_key, s3_secret)

    hltag_paths = get_file_paths(s3_client, aws_s3_bucket, prefix_path, set(["hltags"]))

    return hltag_paths


@inject
@task()
def convert_hlogtag_to_json(
    s3_key: str,
    s3_secret: str,
    s3_region: str,
    s3_bucket: str,
    aws_batch_filesystem_dict: Dict,
    preset_prefix_path: str = "",
):
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")
    if prefix_path == "":
        prefix_path = preset_prefix_path

    airflow_logger.info(f"prefix_path: {prefix_path}")
    test_run = conf.get("test_run", False)
    test_container = conf.get("test_container", False)

    s3_client = create_s3_client(s3_key, s3_secret)
    batch_client = create_batch_client_v2(s3_key, s3_secret, s3_region)
    sts_client = create_sts_client(s3_key, s3_secret, s3_region)

    # Extract aws account information for submitting batch job with job role ARN and execution role ARN
    response = sts_client.get_caller_identity()
    aws_account = response["Account"]

    unpacked_job_dict = {"jobId": [], "StagingPrefix": [], "FileSystemID": [], "JobStatus": []}
    hlatg_paths = get_file_paths(s3_client, s3_bucket, prefix_path, set(["hltags"]))

    airflow_logger.info(f"hlatg_paths: {hlatg_paths}")

    for hltag_path in hlatg_paths:
        batch_job_name = get_batch_job_name("HLTAGTOJSON")

        prefix_key = hltag_path.replace("s3://", "").split("/", 1)[1]

        base_log_dir = prefix_key.rsplit("/", 1)[0]

        response = run_hltag_to_json_job_in_batch(
            batch_client,
            s3_bucket,
            base_log_dir,
            batch_job_name,
            aws_account,
            s3_region,
            aws_batch_filesystem_dict["FileSystemID"],
            aws_batch_filesystem_dict["FileSystemMountPath"],
            test_container=test_container,
        )

        airflow_logger.info(f"batch response: {response}")

        unpacked_job_dict["jobId"].append(response["jobId"])

        if aws_batch_filesystem_dict.get("FileSystemID") not in unpacked_job_dict["FileSystemID"]:
            unpacked_job_dict["FileSystemID"].append(aws_batch_filesystem_dict["FileSystemID"])

        unpacked_job_dict["StagingPrefix"].append(hltag_path.rsplit("/", 1)[0])

    return unpacked_job_dict


@inject
@task
def associate_logs_in_s3(    
    s3_key: str,
    s3_secret: str,
    src_bucket: str,
    dest_bucket: str,
    preset_prefix_path: str = "",
    raw_data_service: RawDataService = Provide[Container.raw_data_service],
    associate_cfh_1a_data_service: AssociateCfh1ADataService = Provide[Container.associate_cfh_1a_data_service]
):
    """
    Associates logs in S3 between source and destination buckets.

    Args:
        s3_key (str): AWS S3 access key.
        s3_secret (str): AWS S3 secret key.
        src_bucket (str): Source S3 bucket name (e.g., for CFH logs).
        dest_bucket (str): Destination S3 bucket name (e.g., for 1A logs).
        preset_prefix_path (str, optional): S3 prefix path to filter logs. Defaults to "".
        raw_data_service (RawDataService): Service for raw data operations.
        associate_cfh_1a_data_service (AssociateCfh1ADataService): Service for associating CFH and 1A data.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    # pull run time configs
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path: str = conf.get("prefix_path", preset_prefix_path)
    if "s3://" in prefix_path:
        _, prefix_path = split_s3url_to_bucket_key(prefix_path)
    raw_data_paths_cfh = raw_data_service.get_raw_data_snapshots_like(src_bucket, prefix_path)
    raw_data_paths_1ahs = raw_data_service.get_raw_data_snapshots_like(dest_bucket, prefix_path)
    matched_cfh_1a_pairs = get_matched_pairs_from_list(raw_data_paths_cfh, raw_data_paths_1ahs)
    airflow_logger.info(f"matched_cfh_1a_pairs: {matched_cfh_1a_pairs}")

    # Fetch already registered pairs to avoid duplicates
    associate_cfh_1a_paths_list = associate_cfh_1a_data_service.get_all_indexed_cfh_1a_matched_data()
    existing_pairs = set(
        (s3_path.file_path_cfh, s3_path.file_path_1a)
        for s3_path in associate_cfh_1a_paths_list
    )

    with Context().new_session_scope() as session:
        inserted_objs = []
        for matched_pair in matched_cfh_1a_pairs:
            cfh_raw_data_path = matched_pair[0]
            one_a_raw_data_path = matched_pair[1]
            if (cfh_raw_data_path, one_a_raw_data_path) in existing_pairs:
                airflow_logger.info(f"Skipping already registered pair: {cfh_raw_data_path}, {one_a_raw_data_path}")
                continue
            assoc_obj = AssociateCfh1AData(cfh_raw_data_path, one_a_raw_data_path)
            session.add(assoc_obj)
            inserted_objs.append(assoc_obj)
        session.commit()
        inserted_ids = [obj.id for obj in inserted_objs]
        if not inserted_ids:
            airflow_logger.warning("No new associations were inserted. Exiting safely.")
    return inserted_ids


@inject
@task(task_id="get_unmerged_cfh_1a_pairs")
def get_unmerged_cfh_1a_pairs(
    associate_cfh_1a_ids: List[int] = [],
    preset_prefix_path: str = "",
    associate_cfh_1a_data_service: AssociateCfh1ADataService = Provide[Container.associate_cfh_1a_data_service]
):
    """
    Retrieves CFH and 1A pairs from the AssociateCfh1AData table based on associate_cfh_1a_ids.

    Returns:
        List[Tuple[str, str]]: List of (cfh_key, one_a_key) pairs.
    """
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path: str = conf.get("prefix_path", preset_prefix_path) 
    if prefix_path.startswith("s3://"):
        _, prefix_path = split_s3url_to_bucket_key(prefix_path)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    # If associate_cfh_1a_ids is provided, fetch only those records
    if associate_cfh_1a_ids:
        associate_datas = associate_cfh_1a_data_service.get_data_by_ids(associate_cfh_1a_ids)
    else:
        # fallback to not yet merged if no ids provided
        associate_datas = associate_cfh_1a_data_service.get_data_not_yet_merged(prefix_path)

    cfh_1a_pairs = [
        (
            split_s3url_to_bucket_key(associate_data.file_path_cfh)[1],
            split_s3url_to_bucket_key(associate_data.file_path_1a)[1]
        )
        for associate_data in associate_datas
    ]
    cfh_1a_ids = [associate_data.id for associate_data in associate_datas]

    airflow_logger.info(f"CFH-1A pairs: {cfh_1a_pairs}")
    airflow_logger.info(f"CFH-1A IDs: {cfh_1a_ids}")

    # push xcom value for pairs
    context = get_current_context()
    ti = context["ti"]
    ti.xcom_push("unmerged_cfh_1a_pairs", cfh_1a_pairs)
    ti.xcom_push("unmerged_cfh_1a_ids", cfh_1a_ids)

    return cfh_1a_pairs
    

@inject
@task
def merge_cfh_1a_logs_in_s3(
    s3_key: str,
    s3_secret: str,
    src_bucket: str,
    dest_bucket: str,
    file_system_dict: Dict
):
    """
    Launches AWS Batch jobs to merge CFH and 1A logs in S3 using provided file system.

    Args:
        s3_key (str): AWS S3 access key.
        s3_secret (str): AWS S3 secret key.
        src_bucket (str): Source S3 bucket name (CFH logs).
        dest_bucket (str): Destination S3 bucket name (1A logs).
        file_system_dict (Dict): FSx file system info with FileSystemID and FileSystemMountPath.
    Returns:
        List[str]: List of AWS Batch job IDs submitted.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path: str = conf.get("prefix_path", "")
    if prefix_path.startswith("s3://"):
        _, prefix_path = split_s3url_to_bucket_key(prefix_path)

    batch_job_name = get_batch_job_name("CFH1AMERGEJOB")

    # Create AWS clients
    s3_client = create_s3_client(s3_key, s3_secret)
    s3_region = s3_client.get_bucket_location(Bucket=src_bucket).get("LocationConstraint")
    airflow_logger.info(f"Using S3 region: {s3_region}")
    batch_client = create_batch_client_v2(s3_key, s3_secret, s3_region)
    sts_client = create_sts_client(s3_key, s3_secret, s3_region)
    aws_account = sts_client.get_caller_identity()["Account"]

    ti = ctx["ti"]
    unmerged_cfh_1a_pairs = ti.xcom_pull(key="unmerged_cfh_1a_pairs", task_ids="get_unmerged_cfh_1a_pairs")
    if not unmerged_cfh_1a_pairs:
        airflow_logger.warning("No unmerged CFH-1A pairs found for merging. Exiting safely.")
        return []

    job_ids = []
    total_pairs = len(unmerged_cfh_1a_pairs)
    batch_size = _CFH_1A_MERGE_BATCH_SIZE

    for i in range(0, total_pairs, batch_size):
        pairs_batch = unmerged_cfh_1a_pairs[i:i + batch_size]
        # Submit a batch job to merge each batch of CFH-1A pairs
        response = run_cfh_1a_merge_job_in_batch(
            batch_client,
            src_bucket,
            dest_bucket,
            pairs_batch,
            batch_job_name,
            aws_account,
            file_system_dict.get("FileSystemID"),
            file_system_dict.get("FileSystemMountPath"),
            s3_region,
        )
        if not response or "jobId" not in response:
            airflow_logger.error(f"Failed to submit merge job for pairs {i} to {i + len(pairs_batch) - 1}: {response}")
            continue
        job_id = response.get("jobId")
        airflow_logger.info(f"Started merge batch job {job_id} for pairs {i} to {i + len(pairs_batch) - 1}")
        job_ids.append(job_id)

    return job_ids


@inject
@task()
def update_merged_status_in_db(
    job_ids: List[str],
    aws_batch_key: str,
    aws_batch_secret: str,
    aws_batch_region: str,
    aws_batch_hook,
    associate_cfh_1a_data_service: AssociateCfh1ADataService = Provide[Container.associate_cfh_1a_data_service],
):
    """
    Updates the merged status for CFH-1A pairs in the database after merge jobs complete.

    Args:
        job_ids (List[str]): List of AWS Batch job IDs.
        aws_batch_key (str): AWS Batch access key.
        aws_batch_secret (str): AWS Batch secret key.
        aws_batch_region (str): AWS region.
        aws_batch_hook: AWS Batch hook object.
        associate_cfh_1a_data_service (AssociateCfh1ADataService): Service for updating merge status.
    Returns:
        Dict: Dictionary with updated association IDs and filesystem ID.
    """
    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    batch_client = create_batch_client_v2(aws_batch_key, aws_batch_secret, aws_batch_region)
    context = get_current_context()
    ti = context["ti"]
    unmerged_cfh_1a_ids = ti.xcom_pull(key="unmerged_cfh_1a_ids", task_ids="get_unmerged_cfh_1a_pairs")
    aws_file_system_id = ti.xcom_pull(key="fsx_file_system_id", task_ids="create_filesystem_v2")
    associate_data_ids_list = []
    update_db_status_dict = {'FileSystemID': '', 'associate_data_ids_updated': []}

    # Wait for all jobs and check their status
    for job_id in job_ids:
        status_response = batch_client.describe_jobs(jobs=[job_id])
        job = status_response["jobs"][0]
        job_description = aws_batch_hook.get_job_description(job["jobId"])
        job_status = job_description.get("status")
        airflow_logger.info(f"job_description: {job_description}")
        aws_batch_hook.wait_for_job(job["jobId"])
        if not aws_batch_hook.check_job_success(job["jobId"]):
            airflow_logger.error(f"Job {job['jobId']} failed with status {job_status}.")
            raise RuntimeError(f"Job {job['jobId']} failed with status {job_status}.")

    # If all jobs succeeded, update DB
    with Context().new_session_scope() as session:
        for unmerged_cfh_1a_id in unmerged_cfh_1a_ids:
            associate_data_id = associate_cfh_1a_data_service.update_data_with_merge_status(unmerged_cfh_1a_id)
            if associate_data_id:
                associate_data_ids_list.append(associate_data_id)
            else:
                airflow_logger.warning(f"Failed to update merged status for ID: {unmerged_cfh_1a_id}")
    update_db_status_dict["FileSystemID"] = [aws_file_system_id]
    update_db_status_dict["associate_data_ids_updated"] = associate_data_ids_list
    airflow_logger.info(f"Successfully updated merged status for IDs: {associate_data_ids_list}")

    return update_db_status_dict


@inject
@task()
def index_merged_data_in_s3(
    s3_key: str,
    s3_secret: str,
    s3_bucket: str,
    update_db_status_dict: Dict,
    ptag_service: PTagService = Provide[Container.ptag_service],
    associate_cfh_1a_data_service: AssociateCfh1ADataService = Provide[Container.associate_cfh_1a_data_service],
    merge_cfh_1a_data_service: MergeCfh1ADataService = Provide[Container.merge_cfh_1a_data_service]
):
    """
    Index merged CFH-1A data in S3 and update the database if TransmissionTimeIndex00000.hlog is present
    and no .hlock file exists in the merged directory.

    Returns:
        List[int]: List of indexed merged IDs.
    """
    ctx = get_current_context()
    conf = ctx["dag_run"].conf
    prefix_path = conf.get("prefix_path", "")
    test_run = conf.get("test_run", False)
    log_level_debug = conf.get("log_level_debug", False)

    airflow_logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
    if log_level_debug:
        airflow_logger.setLevel(logging.DEBUG)
    airflow_logger.info(f"index_merged_data_in_s3: {s3_bucket} with prefix: {prefix_path}")
    airflow_logger.debug(f"conf: {conf}")

    s3_client = create_s3_client(s3_key, s3_secret)

    associate_data_ids_updated = update_db_status_dict.get("associate_data_ids_updated", [])
    if not associate_data_ids_updated:
        airflow_logger.warning("No association IDs to index. Exiting safely.")
        return []

    merge_log_paths = []
    associate_ids = []
    ptag_paths = set()

    for associate_id in associate_data_ids_updated:
        associate_obj = associate_cfh_1a_data_service.get_data_by_id(associate_id)
        if not associate_obj:
            airflow_logger.warning(f"Associate object with ID {associate_id} not found.")
            continue
        
        # Initialize variables
        merged_dir = ""
        merged_file_prefix = ""
        merged_file_path_to_index = ""

        # Compute merged snapshot name and merged directory
        merged_snapshot_name = get_merged_snapshot_name(associate_obj.file_path_cfh, associate_obj.file_path_1a)
        s3_bucket_name, one_ahs_key = split_s3url_to_bucket_key(f"s3://{associate_obj.file_path_1a}")
        merged_base_dir = os.path.dirname(one_ahs_key).replace("Unpacked", "Merged").rstrip("/")
        if merged_snapshot_name:
            merged_dir = os.path.join(merged_base_dir, merged_snapshot_name).replace("//", "/")
        merged_file_prefix = f"{merged_dir}/{_TRANSMISSION_INDEX_KEY}"
        merged_file_path_to_index = f"s3://{s3_bucket_name}/{merged_file_prefix}"

        # Find ptag files in merged dir
        json_files = get_file_paths(s3_client, s3_bucket_name, merged_dir, {"json"})
        for json_path in json_files:
            if is_json_file_in_s3_ptag(s3_client, s3_bucket_name, split_s3url_to_bucket_key(json_path)[1]):
                ptag_paths.add(json_path)

        # List all objects in the merged directory and check for required files
        hlog_found = False
        hlock_found = False
        try:
            response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=merged_dir)
            for obj in response.get("Contents", []):
                if _TRANSMISSION_INDEX_KEY in obj.get("Key", ""):
                    hlog_found = True
                if obj.get("Key", "").endswith(".hlock"):
                    hlock_found = True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "404":
                airflow_logger.warning(f"TransmissionTimeIndex00000.hlog not found in merged path: s3://{s3_bucket_name}/{merged_dir}")
            else:
                airflow_logger.error(f"Error checking for TransmissionTimeIndex00000.hlog: {e}")

        if hlog_found and not hlock_found:
            merge_log_paths.append(merged_file_path_to_index)
            associate_ids.append(associate_id)
        else:
            if not hlog_found:
                airflow_logger.warning(f"TransmissionTimeIndex00000.hlog not found in merged path: s3://{s3_bucket_name}/{merged_dir}")
            if hlock_found:
                airflow_logger.warning(f".hlock file found in merged dir {merged_dir}")

    # Build merge log dict for merged_data indexing
    merge_log_dict = get_merged_log_dict(merge_log_paths, associate_ids, list(ptag_paths))

    indexed_merged_ids = []
    indexed_ptag_memo_dict = {}

    with Context().new_session_scope() as session:
        for _, merge_logs in merge_log_dict.items():
            for merge_log in merge_logs:
                log_entry = merge_log["log_entry"]
                associate_id = merge_log["associate_id"]
                merged_file_path = log_entry["log_path"]
                ptag_path = log_entry["ptag_path"]

                ptag_db_obj_id = None
                if ptag_path:
                    ptag_db_obj = get_or_generate_ptag_db_obj(
                        s3_client, ptag_path, indexed_ptag_memo_dict, session, test_run, ptag_service
                    )
                    if ptag_db_obj:
                        ptag_db_obj_id = ptag_db_obj.id

                merged_data_obj = merge_cfh_1a_data_service.get_merged_data_by_associate_id_and_path(
                    associate_id, merged_file_path
                )
                if merged_data_obj:
                    if ptag_db_obj_id and merged_data_obj.ptag_id != ptag_db_obj_id:
                        merged_data_obj.ptag_id = ptag_db_obj_id
                        session.merge(merged_data_obj)
                        airflow_logger.info(f"Updated ptag_id for MergedData {merged_file_path}")
                else:
                    merged_data_obj = MergedData(
                        merged_file_path, ptag_id=ptag_db_obj_id, associate_cfh_1a_data_id=associate_id
                    )
                    session.add(merged_data_obj)
                    session.commit()
                    airflow_logger.info(f"Indexed new merged data: {merged_file_path} with associate_id: {associate_id} and ptag_id: {ptag_db_obj_id}")

                if merged_data_obj.id not in indexed_merged_ids:
                    indexed_merged_ids.append(merged_data_obj.id)
        session.commit()

    airflow_logger.info(f"Indexed Merged IDs: {indexed_merged_ids}")
    return indexed_merged_ids


@task()
def unpack_priority_snapshots(
    key: str,
    secret: str,
    region: str,
    staging_prefixes: list[str],
    fsx: dict[Literal["FileSystemID", "FileSystemMountPath"], str],
) -> dict[Literal["FileSystemID", "StagingPrefix", "JobStatus", "jobId"], Any]:
    ctx = get_current_context()
    ti: TaskInstance = ctx["ti"]

    conf = ctx["dag_run"].conf
    logger = ti.log
    logger.setLevel(logging.DEBUG)

    bucket: str | None = conf.get("bucket")
    if not bucket:
        raise ValueError("Cannot submit unpack priority job without bucket name")

    ptag_path: str | None = conf.get("ptag_path")
    if not ptag_path:
        raise ValueError("Cannot submit unpack priority job without ptag path")

    if not fsx:
        logger.warning("FSx dict not passed in. Attempting to pull from XCom")
        fsx = ti.xcom_pull(task_ids="create_filesystem_v3", key="fsx")
        if not fsx:
            raise ValueError("FSx dict not found in XCom and not passed in. Exiting.")

    s3_client = create_s3_client(key, secret)
    batch_client = create_batch_client_v2(key, secret, region)
    sts_client = create_sts_client(key, secret, region)
    aws_account = sts_client.get_caller_identity()["Account"]

    snapshots_list: str | None = conf.get("snapshots_list")
    date: str | None = conf.get("date")
    month: str | None = conf.get("month")
    filter_new: bool = conf.get("filter_new", False)
    filter_all: bool = conf.get("filter_all", False)
    filter_all_split: bool = conf.get("filter_all_split", False)

    # Override mutually exclusive parameters
    if filter_all_split:
        filter_all = False
        filter_new = False
        month = None
        date = None
        snapshots_list = None

    if filter_all:
        filter_new = False
        month = None
        date = None
        snapshots_list = None

    if filter_new:
        month = None
        date = None
        snapshots_list = None

    if date is not None and month is not None:
        raise ValueError("Both date and month cannot be provided together. Please provide only one.")

    if date:
        try:
            dt.datetime.strptime(date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Date must be in format YYYY-mm-dd") from e

    if month:
        try:
            dt.datetime.strptime(month, "%Y-%m")
        except ValueError as e:
            raise ValueError(f"Date must be in format YYYY-mm") from e

    logger.debug(f"{filter_all_split=}; {filter_all=}; {filter_new=}")
    logger.debug(f"{date=}; {month=}; {snapshots_list=}")

    if not (filter_all_split or filter_all or filter_new or date or month or snapshots_list):
        raise ValueError(
            "At least one of filter_all_split, filter_all, filter_new, date, month or snapshots_list must be provided."
        )

    job_name = get_batch_job_name("FSx-UNPACKING-PRIORITY-SNAPSHOTS")
    if job_name is None:
        raise RuntimeError(f"Failed to get job name!")

    unpack_result: dict[
        Literal["FileSystemID", "StagingPrefix", "JobStatus", "jobId"],
        Any
    ]  = {
        "jobId": [],
        "StagingPrefix": [],
        "FileSystemID": [fsx["FileSystemID"]],
        "JobStatus": [],
    }
    for staging_prefix in staging_prefixes:
        _, staging_key = split_s3url_to_bucket_key(staging_prefix)

        unpacked_prefix = get_unpacked_prefix_from_ptag(s3_client, bucket, ptag_path)
        if not unpacked_prefix:
            logger.warning(f"Could not get unpacked prefix from ptag for {staging_prefix=}. Skipping")
            continue

        job = UnpackPriorityBatchJob(
            job_name=job_name,
            staging_prefix=staging_key,
            unpacked_prefix=unpacked_prefix,
            ptag_path=ptag_path,
            snapshots_list=snapshots_list,
            bucket=bucket,
            aws_account=aws_account,
            **fsx,
        )

        if filter_all_split:
            job.filter_all_split = "true"

        if filter_all:
            job.filter_all = "true"

        if filter_new:
            job.filter_new = "true"

        if date:
            job.date = date

        if month:
            job.month = month

        if snapshots_list:
            job.snapshots_list = snapshots_list

        response: dict[str, Any] = run_unpack_priority_snapshots_job_in_batch(batch_client, job)
        logger.debug(f"For {staging_key=}: {response=}")

        try:
            job_id = response["jobId"]
        except KeyError as e:
            raise ValueError(f"Response from Batch client does not contain jobId for {staging_prefix=}") from e

        logger.debug(f"For {staging_key=}: {job_id=}")
        unpack_result["jobId"].append(job_id)
        unpack_result["StagingPrefix"].append(staging_key)

    return unpack_result
