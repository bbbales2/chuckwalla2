import chuckwalla2.schemas.nba
import sqlalchemy


def create_all(engine: sqlalchemy.engine.Engine):
    chuckwalla2.schemas.nba.NbaBase.metadata.create_all(engine)
