from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker


DBSession = sessionmaker()


@contextmanager
def scoped_session():
    session = DBSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
