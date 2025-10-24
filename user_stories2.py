User Story 1 – Schema & Table Creation

Title: Create new metadata schema and tables for flexible log attribute tagging

Description:
As a data engineer, I want to design and implement new database tables (log_metadata_header, log_metadata_attr) parallel to existing PTAG tables, so that log attributes can be stored and queried flexibly without impacting existing data structures.

Acceptance Criteria:

Metadata header table contains one row per ECM log file, linked to raw_data.id.

Attributes table stores flattened key–value pairs per log.

Alembic migration script successfully creates both tables.

No changes or downtime to PTAG or existing ingestion tables.

Technical Tasks:

Update models.py to include new ORM classes for both tables.

Add Alembic migration script for schema creation.

Verify FK constraints and index performance on raw_data_id.

Unit test schema creation and model insert/query operations.

Dependencies:

Requires existing PTAG and raw_data schema access.

Alembic setup working on devbox.

Risks:

Migration failures on older DB versions or missing privileges.

User Story 2 – DAG for Metadata Ingest

Title: Implement Airflow DAG to ingest and upsert metadata JSON into new tables

Description:
As a pipeline developer, I want to automate metadata ingestion through Airflow DAGs that read JSON manifests placed with ECM logs, so that metadata can be processed and stored automatically.

Acceptance Criteria:

DAG detects new JSON metadata files matching ECM log filenames.

DAG validates incoming JSON schema (key-value format).

Inserts/updates data into log_metadata_header and log_metadata_attr using idempotent logic.

Handles re-upload scenarios gracefully (upsert, no duplicates).

Technical Tasks:

Develop Airflow DAG (PythonOperator) using metadata_repo functions.

Add JSON validation and schema enforcement.

Configure DAG scheduling and file sensor for JSON files.

Add Airflow test run with mock metadata file.

Dependencies:

Database connectivity to Airflow environment.

Availability of sample metadata JSON for testing.

Risks:

File schema mismatch leading to failed ingestion.

User Story 3 – API Endpoint for Metadata Query

Title: Add API endpoint for querying logs by metadata attributes

Description:
As an analyst or developer, I want to query ECM logs based on metadata attributes through a REST API so that I can filter and retrieve logs dynamically without using SQL queries.

Acceptance Criteria:

/log-metadata/search?key=&value= endpoint is functional.

Supports query filters on key-value attributes.

Returns matching logs with their PTAG/log reference.

Handles missing or invalid query parameters gracefully.

Technical Tasks:

Implement new Flask Blueprint (log_metadata_api.py).

Add route handler for search with SQLAlchemy joins.

Integrate with existing app factory and register blueprint.

Write unit tests for endpoint responses (200, 400, empty).

Dependencies:

Schema from User Story 1 must be deployed.

metadata_repo.py functions (upsert, search_by_kv) must be implemented.

Risks:

Cross-table joins may degrade performance on large datasets.

User Story 4 – Validation & Backward Compatibility

Title: Validate new metadata system and ensure compatibility with PTAG workflows

Description:
As a solution architect, I want to ensure that the new metadata system coexists with the current PTAG implementation, so that downstream workflows and dashboards continue functioning without disruption.

Acceptance Criteria:

PTAG APIs and dashboards remain unaffected after deployment.

Feature flag or config toggle enables switching between old and new metadata.

New API responses include both PTAG and metadata context where applicable.

Documentation updated for usage and migration readiness.

Technical Tasks:

Introduce environment-level feature flag to toggle metadata flow.

Conduct regression tests on PTAG ingestion and queries.

Validate unified API behavior in staging environment.

Update “How to Use” guide and migration notes.

Dependencies:

Previous stories (schema, DAG, API) completed.

Risks:

Schema or API conflicts during integration testing.
