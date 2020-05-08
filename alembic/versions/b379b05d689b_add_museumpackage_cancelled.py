"""add MuseumPackage.cancelled

Revision ID: b379b05d689b
Revises: 4e45489d8526
Create Date: 2020-02-26 10:49:39.441002

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b379b05d689b'
down_revision = '4e45489d8526'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('museum_packages', sa.Column('cancelled', sa.Boolean(), server_default='f', nullable=True))


def downgrade():
    op.drop_column('museum_packages', 'cancelled')
