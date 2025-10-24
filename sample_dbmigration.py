# %%
from sqlalchemy.orm import with_expression
import db.models
from db.context import Context
import csv
from db.models import Modality, PTag, RawData, ExtractedData
import json
import sys
csv.field_size_limit(sys.maxsize)

connection_str = os.getenv(
            "SQLALCHEMY_DATABASE_URI",
            "mysql+pymysql://root:ossdb@DATABASE_SERVER_NAME/cdcs",
        )
ctx = Context()
ctx.init_session(connection_str)
ctx.create_tables()
# %% ptag
file_name = "/home/haol/projects/elt-pipeline/db_versions/old_db_csv/data_collection_kit.csv"
with open(file_name) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=";")
    with Context().session_scope():
        for row in csv_reader:
            ptag_json = row['content'].replace("\'", "\"").replace(
                "\"s ", "_s ").replace("True", 'true').replace("False", 'false')
            ptag_dict = json.loads(ptag_json)
            ptag = PTag(row['filepath'], row['ptag_version'], row['md5'], row['country'], row['state'], row['city'], row['project'],
                        row['team'], row['customer'], row['product_group'], row['machine_model'], row['application_type'], row['content'])
            ptag.id = row['id']
            Context().session.add(ptag)
            for modalities_dict in ptag_dict['modalities']:
                modalities = modalities_dict['modalities']
                for item in modalities:
                    modality = Modality(item.get('modality_type', 'unknown'), item.get(
                        'sensor_type', 'unknown'), item['modality_loc1'], item['modality_loc2'], item['modality_loc3'], ptag.id)
                    modality.ptag = ptag
                    Context().session.add(modality)
# %% indexed path

# %% source data
indexed_source_paths = RawData.get(select_fields=[RawData.file_path]).all()
indexed_source = set([result[0] for result in indexed_source_paths])
file_name = '/home/haol/projects/elt-pipeline/db_versions/old_db_csv/source_data.csv'

with open(file_name) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=";")
    for row in csv_reader:
        source_path = row['source_filepath']
        if source_path not in indexed_source:
            print("indexing", source_path)
            with Context().session_scope():
                converted_data_path = row.get('converted_data_path', '')
                raw = RawData(source_path, converted_data_path, row.get(
                    'frames', 0), row['data_collection_kit_id'])
                Context().session.add(raw)
                s3_locs_str = row['s3_loc']
                if s3_locs_str != '0':
                    if converted_data_path.lower().endswith('avi'):
                        s3_locs = s3_locs_str.split(',')
                        for s3_loc in s3_locs:
                            extracted_data = ExtractedData(
                                s3_loc, s3_loc, raw.id)
                            extracted_data.raw_data = raw
                            Context().session.add(extracted_data)
                    else:
                        extracted_data = ExtractedData(
                            s3_locs_str, s3_locs_str, raw.id)
                        extracted_data.raw_data = raw
                        Context().session.add(extracted_data)
                indexed_source.add(source_path)
    print("done")
# %% temp_labeled_data
file_name = '/home/haol/projects/elt-pipeline/db_versions/old_db_csv/temp_labeled_data.csv'
with open(file_name) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=";")
    for row in csv_reader:
        source_path = row['file_path']
        with Context().session_scope():
            raw = RawData(source_path, source_path, row.get('frames', 0), None)
            Context().session.add(raw)
            s3_locs_str = row['s3_loc']
            if s3_locs_str != '0':
                s3_locs = s3_locs_str.split(',')
                for s3_loc in s3_locs:
                    extracted_data = ExtractedData(s3_loc, s3_loc, raw.id)
                    extracted_data.raw_data = raw
                    Context().session.add(extracted_data)
print("done")
# %%
print("hello")
# %%
