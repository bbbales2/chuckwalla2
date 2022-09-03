from chuckwalla2 import get_engine, get_filesystem
from chuckwalla2.schemas.nba import PlayByPlay
from chuckwalla2.etl.argparse_helper import get_args
from chuckwalla2.etl.nba.play_by_play_extract import get_folder
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
                event_number = row["EVENTNUM"]

                existing_event = session.get(PlayByPlay, {"game_id": game_id, "event_number": event_number})

                if existing_event:
                    session.delete(existing_event)

                period_time_string = row["PCTIMESTRING"]
                if period_time_string:
                    minutes_string, seconds_string = period_time_string.split(":")
                    period_seconds_remaining = int(minutes_string) * 60 + int(seconds_string)
                else:
                    period_seconds_remaining = None

                play_by_play = PlayByPlay(
                    game_id=game_id,
                    event_number = event_number,
                    event_message_type = row["EVENTMSGTYPE"],
                    event_message_action_type = row["EVENTMSGACTIONTYPE"],
                    period = row["PERIOD"],
                    seconds_remaining_period = period_seconds_remaining,
                    home_description = row["HOMEDESCRIPTION"],
                    neutral_description = row["NEUTRALDESCRIPTION"],
                    visitor_description = row["VISITORDESCRIPTION"],
                    score = row["SCORE"],
                    score_margin = row["SCOREMARGIN"],
                    player1_type = row["PERSON1TYPE"],
                    player1_id = row["PLAYER1_ID"],
                    player1_name = row["PLAYER1_NAME"],
                    player1_team_id = row["PLAYER1_TEAM_ID"],
                    player1_team = row["PLAYER1_TEAM_ABBREVIATION"],
                    player2_type = row["PERSON2TYPE"],
                    player2_id = row["PLAYER2_ID"],
                    player2_name = row["PLAYER2_NAME"],
                    player2_team_id = row["PLAYER2_TEAM_ID"],
                    player2_team = row["PLAYER2_TEAM_ABBREVIATION"],
                    player3_type = row["PERSON3TYPE"],
                    player3_id = row["PLAYER3_ID"],
                    player3_name = row["PLAYER3_NAME"],
                    player3_team_id = row["PLAYER3_TEAM_ID"],
                    player3_team = row["PLAYER3_TEAM_ABBREVIATION"],
                )
                session.add(play_by_play)

        session.commit()


if __name__ == "__main__":
    args = get_args("Play-by-play transform and load")

    transform_and_load(args.date, production=args.production)