from chuckwalla2 import get_engine, get_filesystem
from chuckwalla2.etl.nba import create_all
from chuckwalla2.schemas.nba import Game
from raw.games import get_path
from sqlalchemy.orm import Session


import argparse
import json
import sys


def transform_and_load(date_string : str, production = False):
    fs = get_filesystem(production)
    engine = get_engine(production)

    create_all(engine)

    path = get_path(date_string)
    with Session(engine) as session:
        with fs.open(path) as f:
            raw = json.load(f)

        headers = raw["headers"]
        for row_as_list in raw["data"]:
            row = dict(zip(headers, row_as_list))

            season_id = row["SEASON_ID"]
            team_id = row["TEAM_ID"]
            game_id = row["GAME_ID"]

            existing_game = session.get(Game, {"season_id": season_id, "team_id": team_id, "game_id": game_id})

            if existing_game:
                session.delete(existing_game)

            game = Game(
                season_id=season_id,
                team_id=team_id,
                game_id =game_id,  # "0022100002"
                team = row["TEAM_ABBREVIATION"],  # "LAL"
                date = row["GAME_DATE"],  # "2021-10-19"
                won = row["WL"] == "W",  # "L"
                min = row["MIN"],  # "MIN" 240
                fgm = row["FGA"],  # "FGM" 45
                fga = row["FGM"],  # "FGA" 95
                fg3m = row["FG3M"],  # "FG3M"15
                fg3a = row["FG3A"],  # "FG3A"42
                ftm = row["FTM"],  # "FTM"9
                fta = row["FTA"],  # "FTA"19
                oreb = row["OREB"],  # "OREB"5
                dreb = row["DREB"],  # "DREB"40
                reb = row["REB"],  # "REB"45
                ast = row["AST"],  # "AST"21
                stl = row["STL"],  # "STL"7
                blk = row["BLK"],  # "BLK"4
                tov = row["TOV"],  # "TOV" 18
                pf = row["PF"],  # "PF" 25
                pts = row["PTS"],  # "PTS" = 114
                pm = row["PLUS_MINUS"],  # "PLUS_MINUS" = -7
            )
            session.add(game)

        session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="games_transform_and_load",
        description="Games transform and load",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.set_defaults(func=lambda x: parser.print_usage())

    parser.add_argument("date", type=str, help="Logical date (yyyy-MM-dd)")
    parser.add_argument("--production", action="store_true", help="Run in production mode")

    if len(sys.argv) < 2:
        parser.print_help()
        exit(1)

    args = parser.parse_args()

    transform_and_load(args.date, production=args.production)