from chuckwalla2 import get_engine
import chuckwalla2.schemas.nba
from chuckwalla2.etl.argparse_helper import get_args


def drop_all(date_string: str, production: bool):
    engine = get_engine(production)

    chuckwalla2.schemas.nba.NbaBase.metadata.drop_all(engine)


if __name__ == "__main__":
    args = get_args(description="Drop all tables")

    drop_all(args.date, args.production)
