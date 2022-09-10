from chuckwalla2 import get_connection_manager
from chuckwalla2.argparse_helper import get_args


def run(partition_date: str, production: bool = True):
    with get_connection_manager() as connection_manager:
        sql = f"""
        create or replace view nba_dw.play_by_play as
            with clean_play_by_play as (
                select
                    *
                    , cast(split_part(pctimestring, ':', 1) as int) as minutes_period
                    , cast(split_part(pctimestring, ':', 2) as int) as seconds_period
                from nba_clean.play_by_play
            )
            select
                partition_date
                , game_id
                , eventnum as event_number
                , eventmsgtype as event_message_type
                , eventmsgactiontype as event_message_action_type
                , period
                , minutes_period * 60 + seconds_period as seconds_remaining_period
                , homedescription as home_description
                , neutraldescription as neutral_description
                , visitordescription as visitor_description
                , score
                , scoremargin as score_margin
                , person1type as player1_type
                , player1_id
                , player1_name
                , player1_team_id
                , player1_team_abbreviation as player1_team
                , person2type as player2_type
                , player2_id
                , player2_name
                , player2_team_id
                , player2_team_abbreviation as player2_team
                , person3type as player3_type
                , player3_id
                , player3_name
                , player3_team_id
                , player3_team_abbreviation player3_team
            from clean_play_by_play
        """

        connection_manager.execute(sql)


if __name__ == "__main__":
    args = get_args(description="Play by play data warehouse view")

    run(args.date, production=args.production)