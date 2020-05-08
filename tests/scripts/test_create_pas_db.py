import pytest
from passari_workflow.scripts.create_pas_db import \
    cli as create_pas_db_cli


@pytest.fixture(scope="function")
def create_pas_db(cli):
    def func():
        return cli(create_pas_db_cli, [])

    return func


def test_create_pas_db(session, create_pas_db):
    result = create_pas_db()

    assert "Done" in result.stdout
