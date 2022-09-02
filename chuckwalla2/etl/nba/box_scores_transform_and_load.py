from chuckwalla2 import get_engine, get_filesystem
from chuckwalla2.schemas.nba import BoxScore
from chuckwalla2.etl.argparse_helper import get_args
from chuckwalla2.etl.nba.box_scores_extract import get_folder
from sqlalchemy.orm import Session
from datetime import timedelta

import glob
import json
import os


def transform_and_load(date_string : str, production = False):
    fs = get_filesystem(production)
    engine = get_engine(production)

    folder = get_folder(date_string)
    with Session(engine) as session:
        for path in glob.glob("*.json", root_dir=folder):
            with fs.open(os.path.join(folder, path)) as f:
                raw = json.load(f)

            headers = raw["headers"]
            for row_as_list in raw["data"]:
                row = dict(zip(headers, row_as_list))

                game_id = row["GAME_ID"]
                team_id = row["TEAM_ID"]
                player_id = row["PLAYER_ID"]

                existing_box_score = session.get(BoxScore, {"game_id": game_id, "team_id": team_id, "player_id": player_id})

                if existing_box_score:
                    session.delete(existing_box_score)

                time_string = row["MIN"]
                if time_string:
                    minutes_string, seconds_string = time_string.split(":")
                    time_played_seconds = int(minutes_string) * 60 + int(seconds_string)
                else:
                    time_played_seconds = 0

                box_score = BoxScore(
                    game_id=game_id,
                    team_id = team_id,
                    player_id = player_id,
                    team = row["TEAM_ABBREVIATION"],
                    player = row["PLAYER_NAME"],
                    start_position = row["START_POSITION"],
                    comment = row["COMMENT"],
                    time_played_seconds = time_played_seconds,
                    fgm = row["FGM"],
                    fga = row["FGA"],
                    fg3m = row["FG3M"],
                    fg3a = row["FG3A"],
                    ftm = row["FTM"],
                    fta = row["FTA"],
                    oreb = row["OREB"],
                    dreb = row["DREB"],
                    reb = row["REB"],
                    ast = row["AST"],
                    stl = row["STL"],
                    blk = row["BLK"],
                    to = row["TO"],
                    pf = row["PF"],
                    pts = row["PTS"],
                    pm = row["PLUS_MINUS"]
                )
                session.add(box_score)

        session.commit()


if __name__ == "__main__":
    args = get_args("Box score transform and load")

    transform_and_load(args.date, production=args.production)