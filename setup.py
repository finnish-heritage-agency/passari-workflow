from setuptools import setup, find_packages


DESCRIPTION = (
    "MuseumPlus digital preservation workflow"
)
LONG_DESCRIPTION = DESCRIPTION
AUTHOR = "Janne Pulkkinen"
AUTHOR_EMAIL = "janne.pulkkinen@museovirasto.fi"


setup(
    name="passari_workflow",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=find_packages("src"),
    include_package_data=True,
    package_dir={"passari_workflow": "src/passari_workflow"},
    install_requires=[
        "click>=7", "click<8",
        "toml",
        "SQLAlchemy",
        "psycopg2",
        "rq>=1",
        "python-redis-lock",
        "alembic",
        "requests"
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6"
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "sync-objects = passari_workflow.scripts.sync_objects:cli",
            "sync-attachments = "
            "passari_workflow.scripts.sync_attachments:cli",
            "sync-hashes = "
            "passari_workflow.scripts.sync_hashes:cli",
            "create-pas-db = passari_workflow.scripts.create_pas_db:cli",
            "enqueue-objects = passari_workflow.scripts.enqueue_objects:cli",
            "sync-processed-sips = "
            "passari_workflow.scripts.sync_processed_sips:cli",
            "reenqueue-object = "
            "passari_workflow.scripts.reenqueue_object:cli",
            "freeze-objects = "
            "passari_workflow.scripts.freeze_objects:cli",
            "unfreeze-objects = "
            "passari_workflow.scripts.unfreeze_objects:cli",
            "pas-shell = "
            "passari_workflow.scripts.pas_shell:cli",
            "reset-workflow = "
            "passari_workflow.scripts.reset_workflow:cli",
            "dip-tool = "
            "passari_workflow.scripts.dip_tool:cli"
        ]
    },
    use_scm_version=True,
    setup_requires=["setuptools_scm"]
)
