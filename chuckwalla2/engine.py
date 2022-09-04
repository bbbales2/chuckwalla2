import configparser
import os
import sqlalchemy.engine
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from chuckwalla2.secrets import get_secrets


def get_engine(production : bool = True) -> sqlalchemy.engine.Engine:
    if production:
        secrets = get_secrets()

        engine = create_engine(f"mysql+pymysql://{secrets.username}:{secrets.password}@{secrets.host}:{secrets.port}/default")

        if not database_exists(engine.url):
            create_database(engine.url)
    else:
        engine = create_engine("sqlite:///chuckwalla2-storage.db")
    return engine
