from passari_workflow.db import DBSession

from sqlalchemy import create_engine

from urllib.parse import quote_plus

from passari_workflow.config import CONFIG


def get_connection_uri():
    """
    Get the connection URI used to connect to the database
    """
    user = CONFIG["db"]["user"]
    password = CONFIG["db"].get("password", "")
    host = CONFIG["db"]["host"]
    name = CONFIG["db"]["name"]
    port = CONFIG["db"]["port"]

    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{name}"


def connect_db():
    """
    Connect to the database, ensuring that functions that return database
    connections work.
    """
    engine = create_engine(get_connection_uri())
    DBSession.configure(bind=engine)

    return engine
