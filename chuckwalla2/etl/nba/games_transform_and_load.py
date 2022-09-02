from chuckwalla2 import get_engine, get_filesystem
from chuckwalla2.schemas.nba import Game
from chuckwalla2.etl.nba.games_extract import get_path
from chuckwalla2.etl.argparse_helper import get_args
from sqlalchemy.orm import Session

import json


def transform_and_load(date_string: str, production: bool = False):
    fs = get_filesystem(production)
    engine = get_engine(production)

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
    args = get_args(description="Games transform and load")

    transform_and_load(args.date, production=args.production)