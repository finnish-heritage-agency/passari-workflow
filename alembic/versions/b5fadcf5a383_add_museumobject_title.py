"""Add MuseumObject.title

Revision ID: b5fadcf5a383
Revises: 346370256182
Create Date: 2019-11-29 10:06:39.018797

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5fadcf5a383'
down_revision = '346370256182'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('museum_objects', sa.Column('title', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('museum_objects', 'title')
