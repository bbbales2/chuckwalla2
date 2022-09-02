from nba_api.stats.endpoints import leaguegamelog
from chuckwalla2 import get_filesystem, S3_BUCKET
from chuckwalla2.etl.argparse_helper import get_args

import os
import pendulum


def get_path(date_string : str):
    return f"{S3_BUCKET}/nba/games/{date_string}.json"


def extract(date_string : str, production : bool = False):
    date = pendulum.parse(date_string)

    if date.month > 6:
        year = date.year
    else:
        year = date.year - 1

    next_year_remainder = year % 2000 + 1

    season = f"{year}-{next_year_remainder}"

    print(f"Extracting games for season = {season}")

    results = leaguegamelog.LeagueGameLog(season=season)

    fs = get_filesystem(production)
    path = get_path(date_string)
    fs.makedirs(os.path.dirname(path), exist_ok=True)
    with fs.open(path, "w") as f:
        f.write(results.league_game_log.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract games")

    extract(args.date, production=args.production)