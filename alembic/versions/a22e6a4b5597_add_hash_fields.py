"""add hash fields

Revision ID: a22e6a4b5597
Revises: b7bf3f1119f5
Create Date: 2020-01-17 10:01:52.576673

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a22e6a4b5597'
down_revision = 'b7bf3f1119f5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('museum_attachments', sa.Column('metadata_hash', sa.String(length=64), nullable=True))
    op.add_column('museum_objects', sa.Column('attachment_metadata_hash', sa.String(length=64), nullable=True))
    op.add_column('museum_objects', sa.Column('metadata_hash', sa.String(length=64), nullable=True))
    op.add_column('museum_packages', sa.Column('attachment_metadata_hash', sa.String(length=64), nullable=True))
    op.add_column('museum_packages', sa.Column('metadata_hash', sa.String(length=64), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('museum_packages', 'metadata_hash')
    op.drop_column('museum_packages', 'attachment_metadata_hash')
    op.drop_column('museum_objects', 'metadata_hash')
    op.drop_column('museum_objects', 'attachment_metadata_hash')
    op.drop_column('museum_attachments', 'metadata_hash')
    # ### end Alembic commands ###
