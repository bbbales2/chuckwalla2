from nba_api.stats.endpoints import leaguegamelog
from chuckwalla2 import get_filesystem, S3_BUCKET

import argparse
import os
import pendulum
import sys


def get_path(date_string : str):
    return f"{S3_BUCKET}/nba/raw/games/{date_string}.json"


def extract(date_string : str, production : bool = False):
    date = pendulum.parse(date_string)

    if date.month > 6:
        year = date.year
    else:
        year = date.year - 1

    next_year_remainder = year % 2000 + 1

    season = f"{year}-{next_year_remainder}"

    results = leaguegamelog.LeagueGameLog(season=season)

    fs = get_filesystem(production)
    path = get_path(date_string)
    fs.makedirs(os.path.dirname(path), exist_ok=True)
    with fs.open(path, "w") as f:
        f.write(results.league_game_log.get_json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="games",
        description="Raw extract for games table",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.set_defaults(func=lambda x: parser.print_usage())

    parser.add_argument("date", type=str, help="Logical date (yyyy-MM-dd)")
    parser.add_argument("--production", action="store_true", help="Run in production mode")

    if len(sys.argv) < 2:
        parser.print_help()
        exit(1)

    args = parser.parse_args()

    extract(args.date, production=args.production)