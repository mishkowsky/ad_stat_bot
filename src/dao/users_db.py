
import enum
from sqlalchemy import Enum, create_engine, MetaData, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, Session
from src.dao.db_config import DB_CONFIG

metadata_obj = MetaData(schema='top_blogger_stat_bot')
Base = declarative_base(metadata=metadata_obj)


class RequestTypesEnum(enum.Enum):
    blogger = 'blogger'
    sku = 'sku'
    brand = 'brand'


class RequestPlatformsEnum(enum.Enum):
    telegram = 'tg'
    instagram = 'inst'
    vk = 'vk'


class Request(Base):

    __tablename__ = 'tg_bot_stat_user_request'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    request_type = Column(Enum(RequestTypesEnum))
    request_platform = Column(Enum(RequestPlatformsEnum))
    request = Column(String)
    created_at = Column(DateTime)


class User(Base):

    __tablename__ = 'tg_bot_stat_user'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    username = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    created_at = Column(DateTime)


if __name__ == '__main__':
    engine = create_engine(DB_CONFIG.DB_URI, echo=False)
    session = Session(bind=engine)
    with session.begin():
        Base.metadata.create_all(session.connection())
        session.commit()
    session.close()
    engine.dispose()