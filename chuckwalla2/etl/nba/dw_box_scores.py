from chuckwalla2 import get_connection_manager
from chuckwalla2.argparse_helper import get_args


def run(partition_date: str, production: bool = True):
    with get_connection_manager() as connection_manager:
        sql = f"""
        create or replace view nba_dw.box_scores as
            with clean_box_scores as (
                select
                    *
                    , cast(split_part(min, ':', 1) as int) as minutes_played
                    , cast(split_part(min, ':', 2) as int) as remainder_seconds_played
                from nba_clean.box_scores
            )
            select
                partition_date
                , game_id
                , team_id
                , player_id
                , team_abbreviation as team
                , player_name as player
                , start_position
                , comment
                , minutes_played * 60 + remainder_seconds_played as time_played_seconds
                , fgm
                , fga
                , fg3m
                , fg3a
                , ftm
                , fta
                , oreb
                , dreb
                , reb
                , ast
                , stl
                , blk
                , to
                , pf
                , pts
                , plus_minus as pm
            from clean_box_scores
        """

        connection_manager.execute(sql)


if __name__ == "__main__":
    args = get_args(description="Box scores data warehouse view")

    run(args.date, production=args.production)