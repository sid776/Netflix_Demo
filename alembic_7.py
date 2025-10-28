What the error means

sqlalchemy.exc.ProgrammingError (1146, "Table 'cdcs.associate_cfh_1a_data' doesn't exist") shows Alembic is trying to run a migration that modifies a table which is not present in your dev DB:

ALTER TABLE associate_cfh_1a_data MODIFY merge_status TINYINT(1) NULL;


That ALTER isn’t part of our new log-metadata work. It’s coming from another migration file that slipped into your history (likely created by a previous --autogenerate) which assumed associate_cfh_1a_data already existed.

Your alembic history output confirms you’re on an older line of history:

current: 1c1331715de4 (2025-06-03 “add supplier task last modified”)

head: 9894492722a5 (our new file)

But there’s also a migration somewhere in db_versions/versions/ that touches associate_cfh_1a_data. On your dev DB that table never existed, so the ALTER fails.

Quick diagnosis (run these)
# 1) See exactly which migration wants to touch that table
grep -R "associate_cfh_1a_data" db_versions/versions -n

# 2) Also list all recent revisions so you see any extra ones you created
ls -1t db_versions/versions | head -n 10

# 3) Show verbose info for your new revision (replace with your id if different)
alembic show 9894492722a5


You’ll find one migration file that contains op.alter_column("associate_cfh_1a_data", ...) (or similar). That is the culprit.

Two safe ways to fix it
A) Keep the DB clean: edit your new migration to only include our two tables

If the offending ALTER is inside your new revision (it often happens when --autogenerate picked up unrelated model diffs):

Open db_versions/versions/9894492722a5_*.py and remove anything that is not:

op.create_table("log_metadata_header",  ... )
op.create_table("log_metadata_attr",    ... )


Keep the mysql_charset='utf8mb4' and the unique constraints and FKs for our two tables.

Delete any op.alter_column(...), op.create_table(...) for unrelated tables, indexes, etc.

Save the file and run:

# sanity check – render SQL to screen (no execution)
alembic upgrade 9894492722a5 --sql | less

# actually apply
alembic upgrade 9894492722a5


If it succeeds, you’ll see both tables created.

B) If the ALTER lives in a separate migration file you accidentally created

If grep shows the ALTER is in another new revision (say abcd1234...):

Revert/remove that accidental revision from git (or rename it out of the folder temporarily), keeping only our intended revision.

Re-run:

alembic heads     # should show only your intended head (9894492722a5)
alembic upgrade head


Why this is OK: we’re not changing prod; we’re fixing an accidental dev-only migration. Our official PR will include a single, clean migration that only creates log_metadata_header and log_metadata_attr.

After the migration succeeds
1) Smoke-check the schema from Python
# quick check in a Python REPL inside your venv
from sqlalchemy import create_engine, text
import os
e = create_engine(os.environ["SQLALCHEMY_DATABASE_URI"])
with e.connect() as c:
    print(c.execute(text("SHOW TABLES LIKE 'log_metadata_header'")).fetchall())
    print(c.execute(text("SHOW TABLES LIKE 'log_metadata_attr'")).fetchall())


You should see both tables listed.

2) Seed one row (optional but helpful)

Use your scripts/seed_metadata.py (set a real RAW_DATA_ID that exists in dev):

export SQLALCHEMY_DATABASE_URI='mysql+pymysql://user:pwd@host/cdcs'
python scripts/seed_metadata.py


Expected output: header_id: <id> new_attr_rows: 3

3) Query via the service (or the Flask route if wired)

Python REPL:

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from db.services.log_metadata_service import LogMetadataService
import os

eng = create_engine(os.environ["SQLALCHEMY_DATABASE_URI"])
Session = sessionmaker(bind=eng)
s = Session()
print(LogMetadataService().search_by_kv(s, key="weather", value="snow", limit=10))
s.close()


You should get [(<log_uid>, <raw_data_id>)].

Stand-up summary you can say

“I created a clean Alembic migration that only adds log_metadata_header and log_metadata_attr.

On the dev DB, an unrelated autogen ALTER to associate_cfh_1a_data caused upgrade to fail because that table doesn’t exist there. I isolated and removed that stray statement from the migration so only our two new tables are created.

I applied the migration on dev, verified both tables via Adminer/SQL, and seeded a sample header + attributes.

The search helper returns expected rows. Next I’ll (a) add a small Flask /logs/metadata GET for key/value searches and (b) finalize the Airflow DAG to ingest JSON into these tables.”

If anything still blocks you, paste the output of:

grep -R "associate_cfh_1a_data" db_versions/versions -n
alembic heads
alembic current -v
