import sqlalchemy.engine
from sqlalchemy import Column, Integer, BigInteger, Boolean, String
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import declarative_base

# sqlite doesn't support schema names
# metadata = MetaData(schema="nba")
NbaBase = declarative_base()


class Game(NbaBase):
    __tablename__ = 'nba_games'

    season_id = Column(String, primary_key=True) # "22021"
    team_id = Column(BigInteger, primary_key=True) # 1610612747
    game_id = Column(String, primary_key=True) # "0022100002"
    team = Column(String) # "LAL"
    date = Column(String) #"2021-10-19"
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