import configparser
import os
import sqlalchemy.engine
from sqlalchemy import create_engine
from chuckwalla2.secrets import get_secrets


def get_engine(production : bool = True) -> sqlalchemy.engine.Engine:
    if production:
        secrets = get_secrets()

        engine = create_engine(f"mysql+pymysql://{secrets.username}:{secrets.password}@{secrets.host}:{secrets.port}/")
    else:
        engine = create_engine("sqlite:///chuckwalla2-storage.db")
    return engine
