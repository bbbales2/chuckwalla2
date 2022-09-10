from nba_api.stats.endpoints import leaguegamelog
from chuckwalla2 import get_folder, get_filesystem
from chuckwalla2.argparse_helper import get_args

import logging
import os
import pendulum


def run(partition_date : str, production : bool = True):
    date = pendulum.parse(partition_date)

    if date.month > 6:
        year = date.year
    else:
        year = date.year - 1

    next_year_remainder = year % 2000 + 1

    season = f"{year}-{next_year_remainder}"

    logging.info(f"Extracting games for season = {season}")

    fs = get_filesystem(production)
    folder = get_folder("nba_raw", "games", partition_name="partition_date", partition_value=partition_date)
    results_path = os.path.join(folder, "0000.json")
    if fs.exists(results_path):
        logging.info(f"Output already exists in {results_path}")
        return

    results = leaguegamelog.LeagueGameLog(season=season)

    with fs.open(results_path, "w") as f:
        f.write(results.league_game_log.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract games")

    run(args.date, production=args.production)