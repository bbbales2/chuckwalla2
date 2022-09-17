from chuckwalla2 import get_connection_manager
from chuckwalla2.argparse_helper import get_args


def run(partition_date: str):
    with get_connection_manager() as connection_manager:
        sql = f"""
        create or replace view nba_dw.games as
            select
                season_id
                , team_id
                , game_id  -- "0022100002"
                , team_abbreviation as team  -- "LAL"
                , game_date as date  -- "2021-10-19"
                , wl = 'W' as won  -- "L"
                , min  -- "MIN" 240
                , fgm  -- "FGM" 45
                , fga  -- "FGA" 95
                , fg3m -- "FG3M"15
                , fg3a -- "FG3A"42
                , ftm  -- "FTM"9
                , fta  -- "FTA"19
                , oreb -- "OREB"5
                , dreb -- "DREB"40
                , reb  -- "REB"45
                , ast  -- "AST"21
                , stl  -- "STL"7
                , blk  -- "BLK"4
                , tov  -- "TOV" 18
                , pf  -- "PF" 25
                , pts  -- "PTS"": 114
                , plus_minus as pm  -- "PLUS_MINUS"": -7
            from nba_clean.games
        """

        connection_manager.execute(sql)


if __name__ == "__main__":
    args = get_args(description="Games data warehouse view")

    run(args.date)