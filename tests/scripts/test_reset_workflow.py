import pytest
from passari_workflow.db.models import MuseumObject
from passari_workflow.scripts.reset_workflow import \
    cli as reset_workflow_cli


@pytest.fixture(scope="function")
def reset_workflow(cli, monkeypatch, museum_packages_dir, redis):
    def func(args, **kwargs):
        return cli(reset_workflow_cli, args, **kwargs)

    monkeypatch.setattr(
        "passari_workflow.scripts.reset_workflow.PACKAGE_DIR",
        str(museum_packages_dir)
    )

    return func


def test_reset_workflow(
        redis, session, reset_workflow, museum_object_factory,
        museum_package_factory, museum_packages_dir):
    """
    Reset workflow and ensure dangling packages are removed
    """
    # Objects A and B will be reset, object C will remain
    object_a = museum_object_factory(id=10)
    package_a = museum_package_factory(downloaded=True, museum_object=object_a)
    object_a.latest_package = package_a

    object_b = museum_object_factory(id=20)
    package_b = museum_package_factory(
        downloaded=True, packaged=True, museum_object=object_b
    )
    object_b.latest_package = package_b

    object_c = museum_object_factory(id=30)
    package_c = museum_package_factory(
        downloaded=True, packaged=True, uploaded=True, museum_object=object_c
    )
    object_c.latest_package = package_c

    (museum_packages_dir / "10" / "sip").mkdir(parents=True)
    (museum_packages_dir / "20" / "sip").mkdir(parents=True)
    (museum_packages_dir / "30" / "sip").mkdir(parents=True)

    session.commit()

    # 2 objects were reset
    result = reset_workflow(["--perform-reset"])
    assert "Found 2 dangling objects" in result.stdout

    object_a = session.query(MuseumObject).get(10)
    object_b = session.query(MuseumObject).get(20)
    object_c = session.query(MuseumObject).get(30)

    assert not object_a.latest_package
    assert not object_b.latest_package
    assert object_c.latest_package

    # Package directories were deleted
    assert not (museum_packages_dir / "10").is_dir()
    assert not (museum_packages_dir / "20").is_dir()
    assert (museum_packages_dir / "30").is_dir()


def test_reset_workflow_help(reset_workflow):
    """
    Test that running the command without parameters only prints the help text
    """
    result = reset_workflow([])

    assert result.stdout.startswith("Usage: ")
