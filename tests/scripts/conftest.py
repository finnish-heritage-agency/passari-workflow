import pytest

from click.testing import CliRunner


@pytest.fixture(scope="function")
def cli(session):
    def func(cmd, args, success=True):
        runner = CliRunner()
        result = runner.invoke(cmd, args, catch_exceptions=False)

        # Expire any existing DB state in the test functions. This ensures
        # we can retrieve the same values both in the test functions and the
        # scripts that we're running.
        session.expire_all()

        exit_code = result.exit_code
        if success:
            assert exit_code == 0, (
                f"Expected command to succeed, got exit code {exit_code} "
                f"instead"
            )
        else:
            assert exit_code != 0, (
                f"Expected command to fail, got exit code 0 instead"
            )

        return result

    return func
