from chuckwalla2 import get_engine
import chuckwalla2.schemas.nba
from chuckwalla2.etl.argparse_helper import get_args


def create_all(date_string: str, production: bool):
    engine = get_engine(production)

    chuckwalla2.schemas.nba.NbaBase.metadata.create_all(engine)


if __name__ == "__main__":
    args = get_args(description="Create all tables")

    create_all(args.date, args.production)
