"""add SyncStatus

Revision ID: 156f33fadc35
Revises: aa4d673fc82c
Create Date: 2020-04-08 15:11:55.432938

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '156f33fadc35'
down_revision = 'aa4d673fc82c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('sync_statuses',
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('start_sync_date', sa.DateTime(timezone=True), nullable=True),
    sa.Column('prev_start_sync_date', sa.DateTime(timezone=True), nullable=True),
    sa.Column('offset', sa.BigInteger(), server_default='0', nullable=True),
    sa.PrimaryKeyConstraint('name', name=op.f('pk_sync_statuses'))
    )


def downgrade():
    op.drop_table('sync_statuses')
