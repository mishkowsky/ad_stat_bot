import enum
from datetime import datetime
from sqlalchemy import Enum, MetaData, Column, Integer, String, DateTime, update
from sqlalchemy.orm import declarative_base, Session

metadata_obj = MetaData(schema='users')
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

    __tablename__ = 'user_request'

    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    request_type = Column(Enum(RequestTypesEnum))
    request_platform = Column(Enum(RequestPlatformsEnum))
    request = Column(String)
    created_at = Column(DateTime)


class User(Base):

    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    username = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    created_at = Column(DateTime)
    last_interaction_date = Column(DateTime)


class UserDatabase:

    def __init__(self, session: Session):
        self.session = session

    def check_user(self, user):
        user_from_db = self.session.query(User).filter(User.user_id == str(user.user_id)).one_or_none()
        if user_from_db is None:
            self.session.add(user)
            self.session.commit()

    def add_new_user_request(self, request):
        self.session.add(request)
        self.session.commit()
        self.update_user_last_interaction(request.user_id)

    def update_user_last_interaction(self, user_id):
        stmt = (
            update(User).
            where(User.user_id == str(user_id)).
            values(last_interaction_date=datetime.now())
        )
        self.session.execute(stmt)
        self.session.commit()
