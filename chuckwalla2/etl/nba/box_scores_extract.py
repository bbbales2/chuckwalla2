from nba_api.stats.endpoints import boxscoretraditionalv2
from chuckwalla2 import get_engine, get_filesystem, S3_BUCKET
from chuckwalla2.etl.argparse_helper import get_args
from chuckwalla2.schemas.nba import Game
from chuckwalla2.etl.throttler import Throttler
from sqlalchemy.orm import Session
from sqlalchemy import select

import os
import pendulum


def get_folder(date_string: str):
    return f"{S3_BUCKET}/nba/box_scores/{date_string}/"


def extract(date_string: str, production: bool = False):
    engine = get_engine(production)

    date = pendulum.parse(date_string)

    with Session(engine) as session:
        games = session.execute(
            select(Game).where(Game.date == date_string)
        ).scalars().all()

        game_ids = set([game.game_id for game in games])

    throttler = Throttler()

    fs = get_filesystem(production)
    folder = get_folder(date_string)
    fs.makedirs(os.path.dirname(folder), exist_ok=True)
    for game_id in game_ids:
        throttler.sleep_if_necessary()
        print(f"Extracting games for game_id = {game_id}")
        results = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)

        try:
            int(game_id)
        except Exception:
            raise Exception(f"game_id must be an integer, found {game_id} instead")

        path = os.path.join(folder, f"{str(game_id)}.json")
        with fs.open(path, "w") as f:
            f.write(results.player_stats.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract for box score")

    extract(args.date, production=args.production)
