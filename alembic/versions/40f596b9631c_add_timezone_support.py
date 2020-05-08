"""add timezone support

Revision ID: 40f596b9631c
Revises: b5fadcf5a383
Create Date: 2019-12-03 13:54:56.253382

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '40f596b9631c'
down_revision = 'b5fadcf5a383'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "museum_packages", "object_modified_date",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True)
    )
    op.alter_column(
        "museum_packages", "created_date",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True)
    )
    op.alter_column(
        "museum_objects", "created_date",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True)
    )
    op.alter_column(
        "museum_objects", "modified_date",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True)
    )


def downgrade():
    op.alter_column(
        "museum_packages", "object_modified_date",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime()
    )
    op.alter_column(
        "museum_packages", "created_date",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime()
    )
    op.alter_column(
        "museum_objects", "created_date",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime()
    )
    op.alter_column(
        "museum_objects", "modified_date",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime()
    )
