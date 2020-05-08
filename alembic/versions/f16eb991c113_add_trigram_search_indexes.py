"""add trigram search indexes

Revision ID: f16eb991c113
Revises: a22e6a4b5597
Create Date: 2020-02-03 14:40:17.831864

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f16eb991c113'
down_revision = 'a22e6a4b5597'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.create_index('ix_museum_objects_freeze_reason_trgm_gin', 'museum_objects', ['freeze_reason'], unique=False, postgresql_ops={'freeze_reason': 'gin_trgm_ops'}, postgresql_using='gin')
    op.create_index('ix_museum_objects_title_trgm_gin', 'museum_objects', ['title'], unique=False, postgresql_ops={'title': 'gin_trgm_ops'}, postgresql_using='gin')


def downgrade():
    op.drop_index('ix_museum_objects_title_trgm_gin', table_name='museum_objects')
    op.drop_index('ix_museum_objects_freeze_reason_trgm_gin', table_name='museum_objects')
