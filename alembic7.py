1) Sanity-check state
# in repo root, venv active, DB URI exported
echo "$SQLALCHEMY_DATABASE_URI"
alembic current -v        # shows the DB’s current revision
alembic heads             # shows the latest revision(s) in your repo
alembic history -v | tail -n 20


If current shows a specific revision (not None), the DB is tracked — perfect.

If you see multiple heads, run alembic merge later; for now just upgrade to the head you care about.

2) Make sure your new revision has the DDL

Open the new file under db_versions/versions/…add_log_metadata_header_and_log_attr.py and confirm upgrade() contains only the two op.create_table(...) blocks and downgrade() drops them. (From your screenshot, it looks correct.)

3) Apply your revision
# apply all pending migrations up to latest
alembic upgrade head

If you get:

“Target database is not up to date.”
That means there are earlier repo revisions your DB hasn’t applied yet. Either:

Apply them: alembic upgrade head (preferred if they’re valid for dev), or

If dev DB shouldn’t replay history, “accept current as baseline” and only track from here:

# find the revision just BEFORE your new file
alembic heads           # note previous head (call it PREV)
alembic stamp PREV      # mark DB as being at PREV without running scripts
alembic upgrade head    # now apply only your new revision


“Can’t locate revision …”
You’re pointed at the wrong migrations folder. Ensure alembic.ini has script_location = db_versions and you’re running from repo root.

4) Verify tables

Use any of:

mysql -h <host> -u <user> -p <db>
SHOW TABLES LIKE 'log_metadata_%';
DESC log_metadata_header;
DESC log_metadata_attr;


or Adminer per wiki.

5) Quick seed & query (optional but recommended)
python scripts/seed_metadata.py
python - <<'PY'
from db.context import Context
from utilities.metadata_repo import search_by_kv
s=Context().get_session()
print(search_by_kv(s, "weather","snow", 10))
s.close()
PY

6) Commit & push migration
git add db_versions/versions/<your_new_file>.py
git commit -m "DB: create log_metadata_header & log_metadata_attr"
git push origin <your-branch>

Stand-up one-liner

“Created a manual Alembic revision for two new metadata tables and applied it to dev. Verified tables exist. Next: seed sample rows, confirm search works, and trigger the Airflow ingest DAG on a sample JSON.”
