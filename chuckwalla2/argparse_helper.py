import argparse
import sys


def get_args(description : str):
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.set_defaults(func=lambda x: parser.print_usage())

    parser.add_argument("--date", type=str, default="2022-09-05", help="Logical date (yyyy-MM-dd)")

    #if len(sys.argv) < 2:
    #    parser.print_help()
    #    exit(1)

    return parser.parse_args()

