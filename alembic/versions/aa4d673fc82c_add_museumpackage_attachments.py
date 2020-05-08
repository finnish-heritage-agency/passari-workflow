"""add MuseumPackage.attachments

Revision ID: aa4d673fc82c
Revises: b379b05d689b
Create Date: 2020-02-27 12:55:27.812507

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa4d673fc82c'
down_revision = 'b379b05d689b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'package_attachment_association',
        sa.Column('museum_package_id', sa.BigInteger(), nullable=True),
        sa.Column('museum_attachment_id', sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(['museum_attachment_id'], ['museum_attachments.id'], name=op.f('fk_package_attachment_association_museum_attachment_id_museum_attachments')),
        sa.ForeignKeyConstraint(['museum_package_id'], ['museum_packages.id'], name=op.f('fk_package_attachment_association_museum_package_id_museum_packages')),
        sa.UniqueConstraint('museum_package_id', 'museum_attachment_id', name=op.f('uq_package_attachment_association_museum_package_id'))
    )
    op.create_index(op.f('ix_package_attachment_association_museum_attachment_id'), 'package_attachment_association', ['museum_attachment_id'], unique=False)
    op.create_index(op.f('ix_package_attachment_association_museum_package_id'), 'package_attachment_association', ['museum_package_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_package_attachment_association_museum_package_id'), table_name='package_attachment_association')
    op.drop_index(op.f('ix_package_attachment_association_museum_attachment_id'), table_name='package_attachment_association')
    op.drop_table('package_attachment_association')
