from chuckwalla2 import get_connection_manager, get_filesystem, get_folder
from chuckwalla2.schema import Schema
from chuckwalla2.etl.argparse_helper import get_args

import json
import logging
import os
import pyarrow
import pyarrow.parquet

games_schema = Schema({
    "SEASON_ID": "string",
    "TEAM_ID": "bigint",
    "GAME_ID": "string",  # "0022100002"
    "TEAM_ABBREVIATION": "string",  # "LAL"
    "GAME_DATE": "string",  # "2021-10-19"
    "WL": "string",  # "L"
    "MIN": "bigint",  # "MIN" 240
    "FGA": "bigint",  # "FGM" 45
    "FGM": "bigint",  # "FGA" 95
    "FG3M": "bigint",  # "FG3M"15
    "FG3A": "bigint",  # "FG3A"42
    "FTM": "bigint",  # "FTM"9
    "FTA": "bigint",  # "FTA"19
    "OREB": "bigint",  # "OREB"5
    "DREB": "bigint",  # "DREB"40
    "REB": "bigint",  # "REB"45
    "AST": "bigint",  # "AST"21
    "STL": "bigint",  # "STL"7
    "BLK": "bigint",  # "BLK"4
    "TOV": "bigint",  # "TOV" 18
    "PF": "bigint",  # "PF" 25
    "PTS": "bigint",  # "PTS"": 114
    "PLUS_MINUS": "bigint",  # "PLUS_MINUS"": -7
 }, partitioned_by = ["SEASON_ID"])


def run(partition_date: str, production : bool = True):
    fs = get_filesystem(production)

    raw_folder = get_folder("nba_raw", "games", partition_name="partition_date", partition_value=partition_date)

    games = []
    for raw_path in fs.glob(f"{raw_folder}/*.json"):
        with fs.open(raw_path) as f:
            raw = json.load(f)

        headers = list(header.lower() for header in raw["headers"])
        for row_as_list in raw["data"]:
            row = dict(zip(headers, row_as_list))

            game = {name: row[name] for name in games_schema}
            games.append(game)

    if len(games) == 0:
        logging.info(f"No games found for partition_date {partition_date}")
        return

    season_id_set = set(game["season_id"] for game in games)
    assert len(season_id_set) == 1
    season_id = season_id_set.pop()

    table = pyarrow.Table.from_pylist(games, schema=games_schema.to_pyarrow_schema())

    folder = get_folder("nba_clean", "games", partition_name="season_id", partition_value=season_id)
    with fs.open(os.path.join(folder, "0000.parquet"), "wb") as f:
        pyarrow.parquet.write_table(table, f)

    with get_connection_manager() as connection_manager:
        connection_manager.ensure_table_exists("nba_clean", "games", games_schema)
        connection_manager.add_partition("nba_clean", "games", partition_name="season_id", partition_value=season_id)


if __name__ == "__main__":
    args = get_args(description="Games transform and load")

    run(args.date, production=args.production)