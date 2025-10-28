"""add log_metadata_header and log_metadata_attr

Revision ID: 9894492722a5
Revises: bcd9dce626d2
Create Date: 2025-10-28 11:25:38.276432

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9894492722a5'
down_revision = 'bcd9dce626d2'
branch_labels = None
depends_on = None


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
