"""add MuseumObject.freeze_source

Revision ID: 346370256182
Revises: 40e6c047955b
Create Date: 2019-11-13 10:07:17.533468

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '346370256182'
down_revision = '40e6c047955b'
branch_labels = None
depends_on = None


def upgrade():
    freeze_source = postgresql.ENUM('USER', 'AUTOMATIC', name='freezesource')
    freeze_source.create(op.get_bind())

    op.add_column('museum_objects', sa.Column('freeze_source', freeze_source, nullable=True))


def downgrade():
    op.drop_column('museum_objects', 'freeze_source')

    freeze_source = postgresql.ENUM('USER', 'AUTOMATIC', name='freezesource')
    freeze_source.drop(op.get_bind())
