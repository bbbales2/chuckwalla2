from chuckwalla2.etl.nba import \
    raw_games, clean_games, dw_games,\
    raw_box_scores, clean_box_scores, dw_box_scores,\
    raw_play_by_play, clean_play_by_play, dw_play_by_play
from textwrap import dedent

import argparse
import logging
import pendulum

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(time_string: str):
    date = pendulum.parse(time_string)
    partition_date = date.subtract(days=1).to_date_string()

    msg = dedent(f"""
        Running with:
         -- time : {time_string}
         -- partition_date : {partition_date}
    """)

    logging.info(msg)

    logging.info("Running raw_games")
    raw_games.run(partition_date=partition_date)

    logging.info("Running raw_box_scores")
    raw_box_scores.run(partition_date=partition_date)

    logging.info("Running raw_play_by_play")
    raw_play_by_play.run(partition_date=partition_date)

    logging.info("Running clean_games")
    clean_games.run(partition_date=partition_date)

    logging.info("Running clean_box_scores")
    clean_box_scores.run(partition_date=partition_date)

    logging.info("Running clean_play_by_play")
    clean_play_by_play.run(partition_date=partition_date)

    logging.info("Running dw_games")
    dw_games.run(partition_date=partition_date)

    logging.info("Running dw_box_scores")
    dw_box_scores.run(partition_date=partition_date)

    logging.info("Running dw_play_by_play")
    dw_play_by_play.run(partition_date=partition_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Entrypoint to run ETLs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.set_defaults(func=lambda x: parser.print_usage())

    parser.add_argument(
        "--time",
        type=str,
        default="2022-01-02",
        help="Run time in a form pendulum.parse can understand. Will execute ETLs for previous day"
    )

    args = parser.parse_args()

    main(args.time)
