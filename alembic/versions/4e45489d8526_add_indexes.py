"""add indexes

Revision ID: 4e45489d8526
Revises: 54edccdf9dd8
Create Date: 2020-02-24 15:23:40.955913

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4e45489d8526'
down_revision = '54edccdf9dd8'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f('ix_museum_objects_latest_package_id'), 'museum_objects', ['latest_package_id'], unique=False)
    op.create_index(op.f('ix_museum_packages_created_date'), 'museum_packages', ['created_date'], unique=False)
    op.create_index(op.f('ix_museum_packages_museum_object_id'), 'museum_packages', ['museum_object_id'], unique=False)
    op.create_index('ix_museum_packages_sip_filename_trgm_gin', 'museum_packages', ['sip_filename'], unique=False, postgresql_ops={'sip_filename': 'gin_trgm_ops'}, postgresql_using='gin')


def downgrade():
    op.drop_index('ix_museum_packages_sip_filename_trgm_gin', table_name='museum_packages')
    op.drop_index(op.f('ix_museum_packages_museum_object_id'), table_name='museum_packages')
    op.drop_index(op.f('ix_museum_packages_created_date'), table_name='museum_packages')
    op.drop_index(op.f('ix_museum_objects_latest_package_id'), table_name='museum_objects')
