import sqlalchemy.engine
from sqlalchemy import Column, Integer, String, BigInteger, Boolean, Text
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import declarative_base

# sqlite doesn't support schema names
# metadata = MetaData(schema="nba")
NbaBase = declarative_base()
SEASON_ID_LENGTH = 10
GAME_ID_LENGTH = 20


class Game(NbaBase):
    __tablename__ = 'nba_games'

    season_id = Column(String(SEASON_ID_LENGTH), primary_key=True) # "22021"
    team_id = Column(BigInteger, primary_key=True) # 1610612747
    game_id = Column(String(GAME_ID_LENGTH), primary_key=True) # "0022100002"
    team = Column(Text) # "LAL"
    date = Column(Text) #"2021-10-19"
    won = Column(Boolean) # "L"
    min = Column(Integer) #"MIN" 240
    fgm = Column(Integer) #"FGM" 45
    fga = Column(Integer) #"FGA" 95
    fg3m = Column(Integer) #"FG3M"15
    fg3a = Column(Integer) #"FG3A"42
    ftm = Column(Integer) #"FTM"9
    fta = Column(Integer) #"FTA"19
    oreb = Column(Integer) #"OREB"5
    dreb = Column(Integer) #"DREB"40
    reb = Column(Integer) #"REB"45
    ast = Column(Integer) #"AST"21
    stl = Column(Integer) #"STL"7
    blk = Column(Integer) #"BLK"4
    tov = Column(Integer) #"TOV" 18
    pf = Column(Integer) #"PF" 25
    pts = Column(Integer) #"PTS" = 114
    pm = Column(Integer) #"PLUS_MINUS" = -7


class BoxScore(NbaBase):
    __tablename__ = "nba_box_scores"

    game_id = Column(String(GAME_ID_LENGTH), primary_key = True)
    team_id = Column(BigInteger, primary_key = True)
    player_id = Column(BigInteger, primary_key = True)
    team = Column(Text)
    player = Column(Text)
    start_position = Column(Text)
    comment = Column(Text)
    time_played_seconds = Column(Integer)
    fgm = Column(Integer)
    fga = Column(Integer)
    fg3m = Column(Integer)
    fg3a = Column(Integer)
    ftm = Column(Integer)
    fta = Column(Integer)
    oreb = Column(Integer)
    dreb = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    to = Column(Integer)
    pf = Column(Integer)
    pts = Column(Integer)
    pm = Column(Integer)

class PlayByPlay(NbaBase):
    __tablename__ = "nba_play_by_play"

    game_id = Column(String(GAME_ID_LENGTH), primary_key = True)
    event_number = Column(Integer, primary_key = True)
    event_message_type = Column(Integer)
    event_message_action_type = Column(Integer)
    period = Column(Integer)
    seconds_remaining_period = Column(Integer)
    home_description = Column(Text)
    neutral_description = Column(Text)
    visitor_description = Column(Text)
    score = Column(Integer)
    score_margin = Column(Integer)
    player1_type = Column(Integer)
    player1_id = Column(BigInteger)
    player1_name = Column(Text)
    player1_team_id = Column(BigInteger)
    player1_team = Column(Text)
    player2_type = Column(Integer)
    player2_id = Column(BigInteger)
    player2_name = Column(Text)
    player2_team_id = Column(BigInteger)
    player2_team = Column(Text)
    player3_type = Column(Integer)
    player3_id = Column(BigInteger)
    player3_name = Column(Text)
    player3_team_id = Column(BigInteger)
    player3_team = Column(Text)
