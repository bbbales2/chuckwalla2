from nba_api.stats.endpoints import boxscoretraditionalv2
from chuckwalla2 import get_folder, get_connection_manager, get_filesystem
from chuckwalla2.argparse_helper import get_args
from chuckwalla2.etl.throttler import Throttler

import os
import logging


def run(partition_date: str, production: bool = True):
    with get_connection_manager() as connection_manager:
        game_ids_list = connection_manager.execute(f"select game_id from nba_dw.games where date = '{partition_date}'")

    game_ids = set([row[0] for row in game_ids_list])

    if len(game_ids) == 0:
        logging.info(f"No games found for date = {partition_date}")
        return

    throttler = Throttler()

    fs = get_filesystem(production)
    folder = get_folder("nba_raw", "box_scores", partition_name="partition_date", partition_value=partition_date)
    for game_id in game_ids:
        throttler.sleep_if_necessary()

        logging.info(f"Extracting games for game_id = {game_id}")

        results_path = os.path.join(folder, f"{str(game_id)}.json")
        if fs.exists(results_path):
            logging.info(f"Output already exists for game_id = {game_id} in {results_path}")
            continue

        results = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)

        try:
            int(game_id)
        except Exception:
            raise Exception(f"game_id must be an integer, found {game_id} instead")

        with fs.open(results_path, "w") as f:
            f.write(results.player_stats.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract for box score")

    run(args.date, production=args.production)
