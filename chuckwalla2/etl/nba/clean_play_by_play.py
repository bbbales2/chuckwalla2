from chuckwalla2 import get_connection_manager, get_filesystem
from chuckwalla2.schema import Schema
from chuckwalla2.argparse_helper import get_args
from chuckwalla2.etl.nba.raw_play_by_play import get_folder

import json
import logging
import os
import pyarrow
import pyarrow.parquet

play_by_play_schema = Schema({
    "PARTITION_DATE": "string",
    "GAME_ID": "string",
    "EVENTNUM": "bigint",
    "EVENTMSGTYPE" : "bigint",
    "EVENTMSGACTIONTYPE" : "bigint",
    "PERIOD" : "bigint",
    "PCTIMESTRING" : "string",
    "HOMEDESCRIPTION" : "string",
    "NEUTRALDESCRIPTION" : "string",
    "VISITORDESCRIPTION" : "string",
    "SCORE" : "string",
    "SCOREMARGIN" : "string",
    "PERSON1TYPE" : "bigint",
    "PLAYER1_ID" : "bigint",
    "PLAYER1_NAME" : "string",
    "PLAYER1_TEAM_ID" : "bigint",
    "PLAYER1_TEAM_ABBREVIATION" : "string",
    "PERSON2TYPE" : "bigint",
    "PLAYER2_ID" : "bigint",
    "PLAYER2_NAME" : "string",
    "PLAYER2_TEAM_ID" : "bigint",
    "PLAYER2_TEAM_ABBREVIATION" : "string",
    "PERSON3TYPE" : "bigint",
    "PLAYER3_ID" : "bigint",
    "PLAYER3_NAME" : "string",
    "PLAYER3_TEAM_ID" : "bigint",
    "PLAYER3_TEAM_ABBREVIATION" : "string",
}, partitioned_by=["PARTITION_DATE"])


def run(partition_date: str, production: bool = True):
    fs = get_filesystem(production)

    play_by_plays = []
    folder = get_folder("nba_raw", "play_by_play", partition_name="partition_date", partition_value=partition_date)
    for raw_path in fs.glob(f"{folder}/*.json"):
        with fs.open(raw_path) as f:
            raw = json.load(f)

        headers = list(header.lower() for header in raw["headers"])
        for row_as_list in raw["data"]:
            row = dict(zip(headers, row_as_list))

            play_by_play = {name: row[name] for name in play_by_play_schema.fields()}
            play_by_plays.append(play_by_play)

    if len(play_by_plays) == 0:
        logging.info(f"No play-by-plays found for partition_date {partition_date}")
        return

    table = pyarrow.Table.from_pylist(play_by_plays, schema=play_by_play_schema.to_pyarrow_schema())

    folder = get_folder("nba_clean", "play_by_play", partition_name="partition_date", partition_value=partition_date)
    with fs.open(os.path.join(folder, "0000.parquet"), "wb") as f:
        pyarrow.parquet.write_table(table, f)

    with get_connection_manager() as connection_manager:
        connection_manager.ensure_table_exists("nba_clean", "play_by_play", play_by_play_schema)
        connection_manager.add_partition("nba_clean", "play_by_play", partition_name="partition_date", partition_value=partition_date)


if __name__ == "__main__":
    args = get_args("Play-by-play transform and load")

    run(args.date, production=args.production)