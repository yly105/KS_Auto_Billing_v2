from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from configs.db_conf import *

Base = declarative_base()
Engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')
DBSession = sessionmaker(bind=Engine)
db_session = DBSession()
