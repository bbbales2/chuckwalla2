import configparser
import os
import sqlalchemy.engine
from sqlalchemy import create_engine

CONFIG_NAME = ".chuckwalla2.ini"


def get_engine(production : bool = True) -> sqlalchemy.engine.Engine:
    if production:
        parser = configparser.ConfigParser()

        directories = (os.getcwd(), os.path.expanduser("~"))
        filenames = (os.path.join(directory, CONFIG_NAME) for directory in directories)

        config = parser.read(filenames)

        if "mysql" not in config:
            raise Exception("A configuration must be provided for mysql")

        user = config.get("mysql", "user")
        password = config.get("mysql", "password")
        hostname = config.get("mysql", "hostname")
        port = config.get("mysql", "port")

        engine = create_engine(f"mysql+pymysql://{user}:{password}@{hostname}:{port}/")
    else:
        engine = create_engine("sqlite:///dino-storage.db")
    return engine
