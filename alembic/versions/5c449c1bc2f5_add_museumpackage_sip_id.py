"""Add MuseumPackage.sip_id

Revision ID: 5c449c1bc2f5
Revises: 40f596b9631c
Create Date: 2019-12-04 09:58:43.525122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5c449c1bc2f5'
down_revision = '40f596b9631c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('museum_packages', sa.Column('sip_id', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('museum_packages', 'sip_id')
