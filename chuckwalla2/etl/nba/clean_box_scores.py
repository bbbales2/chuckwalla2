from chuckwalla2 import get_connection_manager, get_filesystem
from chuckwalla2.schema import Schema
from chuckwalla2.etl.argparse_helper import get_args
from chuckwalla2.etl.nba.raw_box_scores import get_folder

import json
import logging
import os
import pyarrow
import pyarrow.parquet


box_scores_schema = Schema({
    "PARTITION_DATE" : "string",
    "GAME_ID" : "string",
    "TEAM_ID" : "bigint",
    "PLAYER_ID" : "bigint",
    "TEAM_ABBREVIATION" : "string",
    "PLAYER_NAME" : "string",
    "START_POSITION" : "string",
    "COMMENT" : "string",
    "MIN" : "string",
    "FGM" : "bigint",
    "FGA" : "bigint",
    "FG3M" : "bigint",
    "FG3A" : "bigint",
    "FTM" : "bigint",
    "FTA" : "bigint",
    "OREB" : "bigint",
    "DREB" : "bigint",
    "REB" : "bigint",
    "AST" : "bigint",
    "STL" : "bigint",
    "BLK" : "bigint",
    "TO" : "bigint",
    "PF" : "bigint",
    "PTS" : "bigint",
    "PLUS_MINUS" : "bigint",
}, partitioned_by=["PARTITION_DATE"])


def run(partition_date : str, production = True):
    fs = get_filesystem(production)

    folder = get_folder("nba_raw", "box_scores", partition_name="partition_date", partition_value=partition_date)

    box_scores = []
    for raw_path in fs.glob(f"{folder}/*.json"):
        with fs.open(raw_path) as f:
            raw = json.load(f)

        headers = list(header.lower() for header in raw["headers"])
        for row_as_list in raw["data"]:
            row = dict(zip(headers, row_as_list))

            box_score = {name: row[name] for name in box_scores_schema.fields()}
            box_scores.append(box_score)

    if len(box_scores) == 0:
        logging.info(f"No box scores found for partition_date {partition_date}")
        return

    table = pyarrow.Table.from_pylist(box_scores, schema=box_scores_schema.to_pyarrow_schema())

    folder = get_folder("nba_clean", "box_scores", partition_name="partition_date", partition_value=partition_date)
    with fs.open(os.path.join(folder, "0000.parquet"), "wb") as f:
        pyarrow.parquet.write_table(table, f)

    with get_connection_manager() as connection_manager:
        connection_manager.ensure_table_exists("nba_clean", "box_scores", box_scores_schema)
        connection_manager.add_partition("nba_clean", "box_scores", partition_name="partition_date", partition_value=partition_date)


if __name__ == "__main__":
    args = get_args("Box score transform and load")

    run(args.date, production=args.production)