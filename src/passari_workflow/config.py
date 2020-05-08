import datetime
import os
from pathlib import Path

import click

import toml


def get_config(app_name, config_name, default_config):
    """
    Try retrieving the configuration file content from the following sources
    in the following order:
    1. <APP_NAME>_CONFIG_PATH env var, if provided
    2. '/etc/<app_name>/<config_name>' path
    3. Local configuration directory as determined by `click.get_app_dir()`

    In addition, the default config will be written to source 3 in case no
    config sources are available.
    """
    env_name = f"{app_name.upper().replace('-', '_')}_CONFIG_PATH"
    if os.environ.get(env_name):
        return Path(os.environ[env_name]).read_text()

    system_path = Path("/etc") / app_name / config_name
    if system_path.is_file():
        return system_path.read_text()

    local_path = Path(click.get_app_dir(app_name)) / config_name
    if local_path.is_file():
        return local_path.read_text()

    local_path.parent.mkdir(exist_ok=True, parents=True)
    local_path.write_text(default_config)
    return default_config


DEFAULT_CONFIG = f"""
[logging]
# different logging levels:
# 50 = critical
# 40 = error
# 30 = warning
# 20 = info
# 10 = debug
level=10

[db]
# PostgreSQL server credentials
user=''
password=''
host='127.0.0.1'
port='5432'
name='passari'

[redis]
# Redis server credentials
host='127.0.0.1'
port='6379'
password=''

[package]
# Directory used for packages under processing.
# It is recommended to use a high performance and high capacity storage
# for this directory, as the workflow cannot know the size of individual
# files before downloading them.
# This should *not* be located in the same filesystem as the Redis and
# PostgreSQL, as the filesystem can otherwise get full at random due to the
# aforementioned reason.
package_dir=''

# Directory used for storing preservation reports for each processed SIP.
# These files are not read automatically by the workflow and are accessible
# only through the web UI, meaning it is recommended to use a storage
# designed for infrequent reads.
archive_dir=''

# Delay before a new package will enter preservation.
# Default is 30 days (2592000 seconds)
preservation_delay=2592000

# Delay before a preserved package will be updated if changed.
# Default is 30 days (2592000 seconds)
update_delay=2592000
"""[1:]

USER_CONFIG_DIR = click.get_app_dir("passari-workflow")
CONFIG = toml.loads(
    get_config("passari-workflow", "config.toml", DEFAULT_CONFIG)
)

PACKAGE_DIR = CONFIG["package"]["package_dir"]
ARCHIVE_DIR = CONFIG["package"]["archive_dir"]

PRESERVATION_DELAY = datetime.timedelta(
    seconds=int(CONFIG["package"].get("preservation_delay", 2592000))
)
UPDATE_DELAY = datetime.timedelta(
    seconds=int(CONFIG["package"].get("update_delay", 2592000))
)
