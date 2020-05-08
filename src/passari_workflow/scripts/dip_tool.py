"""
Command-line tool to search and download preserved packages from the DPRES
service.

This script is provided for convenience, and isn't used in the workflow at the
moment.
"""
import pathlib
import time

import click
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from passari.config import CONFIG

HOST = f"https://{CONFIG['ssh']['host']}"
BASE_URL = f"{HOST}/api/2.0/urn:uuid:{CONFIG['mets']['contract_id']}"


session = requests.Session()
session.verify = False  # Test environment uses self-signed cert

urllib3.disable_warnings(category=InsecureRequestWarning)


@click.group()
def cli():
    pass


@cli.command(help="Download a preserved package from the DPRES service")
@click.option("--output", type=click.Path(file_okay=True), default=None)
@click.argument("aip_id")
def download(output, aip_id):
    if not output:
        output = str((pathlib.Path(".").resolve() / aip_id).with_suffix(".zip"))
    # Start AIP creation
    response = session.post(
        f"{BASE_URL}/preserved/{aip_id}/disseminate",
        params={"format": "zip"}
    )
    response.raise_for_status()

    data = response.json()["data"]
    print("DIP scheduled for creation, polling until the DIP is ready.")

    poll_url = f"{HOST}{data['disseminated']}"
    download_url = None

    while True:
        response = session.get(poll_url)
        data = response.json()["data"]

        if data["complete"] == "true":
            download_url = f"{HOST}{data['actions']['download']}"
            break
        print(".", end="", flush=True)
        time.sleep(3)

    # AIP complete, download it
    print("\nDownloading...")

    response = session.get(download_url, stream=True)

    with open(output, "wb") as file_:
        for chunk in response.iter_content(chunk_size=128):
            file_.write(chunk)

    print("Done!")


@cli.command(help="List and search for preserved packages in the DPRES service")
@click.option("--page", default=1, type=int)
@click.option("--limit", default=50, type=int)
@click.option("--query", default=None, type=str)
def list_pkgs(page, limit, query):
    params = {
        "page": page,
        "limit": limit
    }

    if query:
        params["q"] = query

    response = session.get(
        f"{BASE_URL}/search",
        params=params,
        timeout=10
    )
    response.raise_for_status()

    data = response.json()["data"]

    for result in data["results"]:
        print(f"{result['id']}")


if __name__ == "__main__":
    cli()
