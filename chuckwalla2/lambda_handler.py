from chuckwalla2.etl.nba import \
    raw_games, clean_games, dw_games,\
    raw_box_scores, clean_box_scores, dw_box_scores,\
    raw_play_by_play, clean_play_by_play, dw_play_by_play
from chuckwalla2.argparse_helper import get_args
from textwrap import dedent

import logging
import pendulum

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    if "time" not in event:
        raise KeyError(f"A runtime (string, UTC) must be passed to the lambda, found event {event}")

    time_argument = event["time"]
    date = pendulum.parse(time_argument)
    partition_date = date.subtract(days=1).to_date_string()
    production = True

    msg = dedent(f"""
        Running with:
         -- time : {time_argument}
         -- partition_date : {partition_date}
         -- production : {production}
    """)

    logging.info(msg)

    logging.info("Running raw_games")
    raw_games.run(partition_date=partition_date, production=production)

    logging.info("Running raw_box_scores")
    raw_box_scores.run(partition_date=partition_date, production=production)

    logging.info("Running raw_play_by_play")
    raw_play_by_play.run(partition_date=partition_date, production=production)

    logging.info("Running clean_games")
    clean_games.run(partition_date=partition_date, production=production)

    logging.info("Running clean_box_scores")
    clean_box_scores.run(partition_date=partition_date, production=production)

    logging.info("Running clean_play_by_play")
    clean_play_by_play.run(partition_date=partition_date, production=production)

    logging.info("Running dw_games")
    dw_games.run(partition_date=partition_date, production=production)

    logging.info("Running dw_box_scores")
    dw_box_scores.run(partition_date=partition_date, production=production)

    logging.info("Running dw_play_by_play")
    dw_play_by_play.run(partition_date=partition_date, production=production)


if __name__ == "__main__":
    handler({"time": "2022-01-02"}, {})
