"""initial

Revision ID: 109dd4123e16
Revises: 
Create Date: 2019-11-11 13:14:26.706301

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '109dd4123e16'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('museum_objects',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('preserved', sa.Boolean(), nullable=True),
        sa.Column('created_date', sa.DateTime(), nullable=True),
        sa.Column('modified_date', sa.DateTime(), nullable=True),
        sa.Column('latest_package_id', sa.BigInteger(), nullable=True),
        # Deferred to prevent 'relation doesn't exist' error
        # sa.ForeignKeyConstraint(['latest_package_id'], ['museum_packages.id'], name='fk_museum_object_latest_package'),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_museum_objects'))
    )
    op.create_table('museum_packages',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('sip_filename', sa.String(length=255), nullable=True),
        sa.Column('object_modified_date', sa.DateTime(), nullable=True),
        sa.Column('created_date', sa.DateTime(), nullable=True),
        sa.Column('downloaded', sa.Boolean(), nullable=True),
        sa.Column('packaged', sa.Boolean(), nullable=True),
        sa.Column('uploaded', sa.Boolean(), nullable=True),
        sa.Column('rejected', sa.Boolean(), nullable=True),
        sa.Column('preserved', sa.Boolean(), nullable=True),
        sa.Column('museum_object_id', sa.BigInteger(), nullable=True),
        # Deferred to prevent 'relation doesn't exist' error
        # sa.ForeignKeyConstraint(['museum_object_id'], ['museum_objects.id'], name='fk_museum_package_museum_object'),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_museum_packages'))
    )
    op.create_foreign_key(
        constraint_name="fk_museum_object_latest_package",
        source_table="museum_objects",
        referent_table="museum_packages",
        local_cols=["latest_package_id"],
        remote_cols=["id"]
    )
    op.create_foreign_key(
        constraint_name="fk_museum_package_museum_object",
        source_table="museum_packages",
        referent_table="museum_objects",
        local_cols=["museum_object_id"],
        remote_cols=["id"]
    )
    op.create_index(op.f('ix_museum_packages_sip_filename'), 'museum_packages', ['sip_filename'], unique=True)


def downgrade():
    op.drop_index(op.f('ix_museum_packages_sip_filename'), table_name='museum_packages')
    op.execute("DROP TABLE museum_packages CASCADE")
    op.execute("DROP TABLE museum_objects CASCADE")
