Error 1146 = Alembic tried to apply (or compare against) a migration that modifies a table (associate_cfh_1a_data) that doesn’t exist in your dev DB.


“Target database is not up to date” = Your DB’s Alembic state is behind (or not tracked at all), so --autogenerate refuses to proceed.


This happens because --autogenerate diff-scans all models vs the live DB. Since your dev DB isn’t aligned to the repo’s migration history, the scan encounters older changes (like that MODIFY merge_status) and blows up before it can even write your new revision.

Fast path to finish just your two new tables
Given the devbox constraints and enterprise env.py you can’t edit, don’t use --autogenerate. Create a manual revision that only creates your two tables, then run it.
Step 0 — sanity: see Alembic state
# does the DB have an alembic_version row?
alembic current -v



If it prints “None” or errors, your DB isn’t tracked by Alembic yet.


Step 1 — create an empty manual revision
alembic revision -m "add log_metadata_header and log_metadata_attr"

This writes a new file under db_versions/versions/XXXX_add_log_metadata...py.
Step 2 — paste the DDL into that revision
Open that new file and put only these ops in upgrade() (and drops in downgrade()):
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.create_table(
        "log_metadata_header",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, index=True, nullable=False, unique=True),
        sa.Column("raw_data_id", sa.Integer(), sa.ForeignKey("raw_data.id"), index=True, nullable=False),
        sa.Column("log_uid", sa.String(length=255), index=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_json", sa.JSON(), nullable=True),
        sa.UniqueConstraint("raw_data_id", name="_log_metadata_header_raw_data_uc"),
        mysql_charset="utf8mb4"
    )

    op.create_table(
        "log_metadata_attr",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, index=True, nullable=False, unique=True),
        sa.Column("header_id", sa.Integer(), sa.ForeignKey("log_metadata_header.id", ondelete="CASCADE"), index=True, nullable=False),
        sa.Column("attr_key", sa.String(length=255), index=True, nullable=False),
        sa.Column("attr_value", sa.String(length=1024), index=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("header_id", "attr_key", "attr_value", name="_log_metadata_attr_uc"),
        mysql_charset="utf8mb4"
    )

def downgrade():
    op.drop_table("log_metadata_attr")
    op.drop_table("log_metadata_header")


Note: we’re not touching any other tables—so no risk of tripping on legacy diffs.

Step 3 — if your DB is not tracked: stamp it once
If alembic current showed “None” (no tracking), you must “tell Alembic where we are” without applying old migrations:
# mark the DB as being at the current *previous* head
# (i.e., before your new revision). Find that with:
alembic heads
# Suppose it prints: a1b2c3d4 (develop head)
# Then stamp to that revision:
alembic stamp a1b2c3d4

If you don’t care about historical diffs in dev and just need to run your new one, you can also:
alembic stamp head

…but only before you create your new revision. If you already created yours, stamp to the previous head (not your new one), then upgrade.
Step 4 — apply your migration only
alembic upgrade head

This should create only log_metadata_header and log_metadata_attr.

Why this fixes your error


We bypass --autogenerate (which tries to diff the world and trips over old, missing tables).


We install a manual, surgical revision that creates only the two new tables.


We “stamp” once so Alembic stops complaining that the DB isn’t up to date.



If you still prefer --autogenerate later
Once your dev DB is aligned to the repo’s head (via proper upgrade or an agreed stamp), --autogenerate will work again for future changes. For now, the manual approach is the least risky.

After migrations succeed — next steps (quick recap)


(Optional) Seed a couple rows to prove write path works:
python scripts/seed_metadata.py



Smoke search in Python REPL:
from db.context import Context
from utilities.metadata_repo import search_by_kv
s = Context().get_session()
print(search_by_kv(s, key="weather", value="snow", limit=10))



Run the DAG (manual trigger) to ingest a JSON from your inbox dir.


Wire the Flask route (already in your PR) and hit:
GET /logs/metadata?key=weather&value=snow




Stand-up blurb (super short)


Blocker earlier was Alembic --autogenerate failing because the dev DB wasn’t aligned to migration history (missing legacy tables + “target not up to date”).


Switched to a manual Alembic revision that creates only our two new tables (log_metadata_header, log_metadata_attr).


Successfully connected with SQLALCHEMY_DATABASE_URI, stamped dev DB once, and ran alembic upgrade head.


Next: seed a few rows, verify query path, and run the Airflow DAG ingest on sample JSON.

