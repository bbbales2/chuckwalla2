from nba_api.stats.endpoints import leaguegamelog
from chuckwalla2 import get_folder, get_filesystem
from chuckwalla2.etl.argparse_helper import get_args

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

    print(f"Extracting games for season = {season}")

    results = leaguegamelog.LeagueGameLog(season=season)

    fs = get_filesystem(production)
    folder = get_folder("nba_raw", "games", partition_date)
    path = os.path.join(folder, "0000.json")
    with fs.open(path, "w") as f:
        f.write(results.league_game_log.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract games")

    run(args.date, production=args.production)