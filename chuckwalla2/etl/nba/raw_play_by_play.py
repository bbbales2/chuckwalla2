from nba_api.stats.endpoints import playbyplayv2
from chuckwalla2 import get_folder, get_connection_manager, get_filesystem
from chuckwalla2.etl.argparse_helper import get_args
from chuckwalla2.etl.throttler import Throttler

import os


def run(partition_date: str, production: bool = True):
    with get_connection_manager() as connection_manager:
        game_ids_list = connection_manager.execute(f"select game_id from nba_dw.games where date = '{partition_date}'")

    game_ids = set([row[0] for row in game_ids_list])

    throttler = Throttler()

    fs = get_filesystem(production)
    folder = get_folder("nba_raw", "play_by_play", partition_date)
    for game_id in game_ids:
        throttler.sleep_if_necessary()
        print(f"Extracting play-by-plays for game_id = {game_id}")
        results = playbyplayv2.PlayByPlayV2(game_id=game_id)

        try:
            int(game_id)
        except Exception:
            raise Exception(f"game_id must be an integer, found {game_id} instead")

        path = os.path.join(folder, f"{str(game_id)}.json")
        with fs.open(path, "w") as f:
            f.write(results.play_by_play.get_json())


if __name__ == "__main__":
    args = get_args(description="Extract for play by play")

    extract(args.date, production=args.production)
