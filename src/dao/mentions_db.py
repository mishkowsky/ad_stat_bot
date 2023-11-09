import enum
from datetime import datetime
from typing import Dict, List, Union, Any

from sqlalchemy import Column, DateTime, ForeignKey, Identity, Integer, String, text, MetaData, Enum, \
    orm, Float, func
from sqlalchemy.orm import declarative_base, relationship, Session

metadata_obj = MetaData(schema='mentions')
Base = declarative_base(metadata=metadata_obj)

# ORM of mentions schema
# https://dbdiagram.io/d/parser_result_post-6508a60c02bd1c4a5ece5ba9


class Brand(Base):
    __tablename__ = 'brand'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    name = Column(String)
    brand_id = Column(Integer, unique=True)

    sku = relationship('Sku', back_populates='brand')

    def __repr__(self):
        return "<Brand(id='%s'; brand_id='%s'; name='%s')>" % \
            (self.id, self.brand_id, self.name)


class ChatContentType(enum.Enum):
    ad_review = 'ad_review'
    wb_items_ads = 'wb_items_ads'
    chat_ads = 'chat_ads'


class TgChatsToParse(Base):
    __tablename__ = 'tg_chats_to_parse'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    tg_id = Column(String)
    link = Column(String)
    title = Column(String)
    chat_content = Column(Enum(ChatContentType))
    followers = Column(Integer)
    recent_parsed_post_tg_id = Column(Integer)
    session_id = Column(Integer)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(DateTime)

    parser_result_tg_post = relationship('ParserResultTgPost', back_populates='chat')

    def __init__(self, link=None, title=None, tg_id=None, followers=None, chat_content=None, updated_at=None,
                 created_at=datetime.now(), session_id=None, update_required=False, recent_parsed_post_tg_id=None):
        super().__init__(
            link=link, title=title, followers=followers, chat_content=chat_content, updated_at=updated_at,
            created_at=created_at, session_id=session_id, recent_parsed_post_tg_id=recent_parsed_post_tg_id)
        self.tg_id = tg_id
        self.session_id = session_id
        self.title = title
        self.link = link
        self.followers = followers
        self.chat_content = chat_content
        self.created_at = created_at
        self.recent_parsed_post_tg_id = recent_parsed_post_tg_id
        self.update_required = update_required
        self.updated_at = updated_at

    @orm.reconstructor
    def init_on_load(self):
        self.update_required = False

    def __repr__(self):
        return "<TgChatToParse(id='%s'; tg_id='%s'; title='%s' link='%s';)>" % \
            (self.id, self.tg_id, self.title, self.link)

    def __eq__(self, obj):
        if not isinstance(obj, TgChatsToParse):
            return False
        if self.tg_id is not None and obj.tg_id is not None:
            return obj.tg_id == self.tg_id
        return obj.link == self.link

    def __hash__(self):
        return hash(self.link)


class ParserResultTgPost(Base):
    __tablename__ = 'parser_result_tg_post'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    chat_id = Column(ForeignKey('tg_chats_to_parse.id'))
    message_id = Column(String)
    views_count = Column(Integer)
    replies_count = Column(Integer)
    shared_count = Column(Integer)
    comments_count = Column(Integer)
    reactions_count = Column(Integer)
    er = Column(Float)
    err = Column(Float)
    date = Column(DateTime)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    chat = relationship('TgChatsToParse', back_populates='parser_result_tg_post')
    sku_per_post = relationship('SkuPerPost', back_populates='post')

    def __repr__(self):
        return "<TgPost(id='%s'; chat_id='%s'; msg_id='%s' date='%s')>" % \
            (self.id, self.chat_id, self.message_id, self.date)

    def __eq__(self, obj):
        if not isinstance(obj, ParserResultTgPost):
            return False
        return obj.chat_id == self.chat_id and obj.message_id == self.message_id

    def __hash__(self):
        return hash((self.chat_id, self.message_id))


class Sku(Base):
    __tablename__ = 'sku'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    sku_code = Column(Integer, unique=True)
    brand_id = Column(ForeignKey('brand.brand_id'))

    brand = relationship('Brand', back_populates='sku')
    sku_per_post = relationship('SkuPerPost', back_populates='sku', cascade='merge')

    def __repr__(self):
        return "<Sku(id='%s'; sku_code='%s'; brand_id='%s')>" % \
            (self.id, self.sku_code, self.brand_id)


class SkuPerPost(Base):
    __tablename__ = 'sku_per_post'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    post_id = Column(ForeignKey('parser_result_tg_post.id'))
    sku_code = Column(ForeignKey('sku.sku_code'))

    post = relationship('ParserResultTgPost', back_populates='sku_per_post')
    sku = relationship('Sku', back_populates='sku_per_post', cascade='merge')

    def __repr__(self):
        return "<SkuPerPost(id='%s'; post_id='%s'; sku_code='%s')>" % \
            (self.id, self.post_id, self.sku_code)


class MentionsDatabase:
    """
   Class to interact with parsers_bd.top_blogger_bot_schema
   """

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_mentions_by_sku(self, sku_code: int) -> \
            dict[TgChatsToParse, dict[ParserResultTgPost, list[SkuPerPost]]]:
        """
        method for obtaining a dictionary of mentions filtered by sku code
        :param sku_code: sku to filter
        :return: dict with mentions per posts per chats
        """
        mentions = self.session.query(SkuPerPost).filter(SkuPerPost.sku_code == sku_code).all()
        return self.generate_mentions_dict(mentions)

    def get_mentions_by_brand(self, brand_name: str) -> \
            dict[TgChatsToParse, dict[ParserResultTgPost, list[SkuPerPost]]]:
        """
        method for obtaining a dictionary of mentions filtered by brand name
        :param brand_name: string, case-insensitive
        :return: dict with mentions per posts per chats
        """
        brand_id = self.session.query(Brand.brand_id).filter(func.lower(Brand.name) == brand_name).one_or_none()
        if brand_id is None:
            return {}
        brand_skus = self.session.query(Sku.sku_code).filter(Sku.brand_id == brand_id[0]).all()
        brand_skus_list = list(brand_sku[0] for brand_sku in brand_skus)
        mentions = self.session.query(SkuPerPost).filter(SkuPerPost.sku_code.in_(brand_skus_list)).all()
        return self.generate_mentions_dict(mentions)

    def generate_mentions_dict(self, mentions: list[SkuPerPost]) -> \
            dict[TgChatsToParse, dict[ParserResultTgPost, list[SkuPerPost]]]:
        """
        creates dict from orm objects
        :param mentions: list of orm of SkuPerPost class
        :return: dict with mentions per posts per chats
        """
        resulting_dict = dict()
        for mention in mentions:
            chat: TgChatsToParse = mention.post.chat
            post: ParserResultTgPost = mention.post
            if chat in resulting_dict.keys():
                if post in resulting_dict[chat].keys():
                    resulting_dict[chat][post].append(mention)
                else:
                    resulting_dict[chat][post] = [mention]
            else:
                resulting_dict[chat] = {post: [mention]}
        return resulting_dict
