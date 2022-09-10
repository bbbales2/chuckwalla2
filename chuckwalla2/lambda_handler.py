from textwrap import dedent

import importlib
import logging


def handler(event, context):
    if "etl" not in event:
        raise KeyError(f"An etl module name (string) must be passed to the lambda, found event {event}")
    if "partition_date" not in event:
        raise KeyError(f"A partition_date (string) must be passed to the lambda, found event {event}")
    if "production" not in event:
        raise KeyError(f"A value for production (boolean) must be passed to the lambda, found event {event}")

    etl_path = str(event["etl"])
    partition_date = str(event["partition_date"])
    production = bool(event["production"])

    msg = dedent(f"""
        Running with:
         -- etl : {etl_path}
         -- partition_date : {partition_date}
         -- production : {production}
    """)

    logging.info(msg)

    etl = importlib.import_module(etl_path)

    return etl.run(partition_date=partition_date, production=production)