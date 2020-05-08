"""add association table indexes

Revision ID: 54edccdf9dd8
Revises: f16eb991c113
Create Date: 2020-02-10 16:17:31.790240

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '54edccdf9dd8'
down_revision = 'f16eb991c113'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f('ix_object_attachment_association_museum_attachment_id'), 'object_attachment_association', ['museum_attachment_id'], unique=False)
    op.create_index(op.f('ix_object_attachment_association_museum_object_id'), 'object_attachment_association', ['museum_object_id'], unique=False)
    op.create_unique_constraint(op.f('uq_object_attachment_association_museum_object_id'), 'object_attachment_association', ['museum_object_id', 'museum_attachment_id'])


def downgrade():
    op.drop_constraint(op.f('uq_object_attachment_association_museum_object_id'), 'object_attachment_association', type_='unique')
    op.drop_index(op.f('ix_object_attachment_association_museum_object_id'), table_name='object_attachment_association')
    op.drop_index(op.f('ix_object_attachment_association_museum_attachment_id'), table_name='object_attachment_association')
