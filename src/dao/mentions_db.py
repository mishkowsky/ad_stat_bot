import enum
from datetime import datetime
from sqlalchemy import Column, DateTime, ForeignKey, Identity, Integer, String, text, MetaData, Enum, \
    orm, Float, func, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, relationship, Session

from src.utils.wb_utils import get_brands_by_skus, BrandRec

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

    def clean_sku_post(self) -> None:
        for sku_per_post in self.sku_per_post:
            post = sku_per_post.post
            if post is not None:
                post.sku_per_post.remove(sku_per_post)

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

    def update_tg_chat(self, tg_chat: TgChatsToParse) -> None:
        """
        Updates entry in top_blogger_stat_bot.tg_chats_to_parse table
        :param tg_chat: object type of TgChatsToParse to update
        """
        tg_chat.updated_at = datetime.now()
        self.update_tg_chat_without_update_time(tg_chat)

    def update_tg_chat_without_update_time(self, tg_chat: TgChatsToParse) -> None:
        if not tg_chat.update_required:
            return
        tg_chat_dict = vars(tg_chat).copy()
        for var in vars(tg_chat):
            if var.startswith('_'):
                tg_chat_dict.pop(var)
        tg_chat.update_required = False
        self.session.query(TgChatsToParse). \
            filter(TgChatsToParse.id == tg_chat.id). \
            update(tg_chat_dict, synchronize_session=False)
        self.session.commit()

    def upload_tg_posts_to_db(self, parsed_posts, parsed_skus):
        brand_dict = get_brands_by_skus(list(parsed_skus.keys()))

        new_skus_counter = 0
        for sku in parsed_skus.values():
            sku_from_db = self.session.query(Sku).filter(Sku.sku_code == sku.sku_code).one_or_none()
            if sku_from_db is None:
                brand = brand_dict.get(sku.sku_code)
                is_loaded = self.load_sku(sku, brand)  # int 0 or 1
                new_skus_counter += is_loaded
        new_post_counter = 0
        total_mentions_count = 0
        for post in parsed_posts:
            post_from_db = self.session.query(ParserResultTgPost). \
                filter(and_(ParserResultTgPost.chat_id == post.chat_id,
                            ParserResultTgPost.message_id == post.message_id)).one_or_none()
            skus_in_post = len(post.sku_per_post)

            if post_from_db is None and skus_in_post != 0:
                total_mentions_count = total_mentions_count + skus_in_post
                new_post_counter = new_post_counter + 1
                self.session.add(post)
        return new_post_counter, total_mentions_count

    def load_sku(self, sku: Sku, brand: BrandRec) -> int:
        if brand is not None and brand.brand_id != 0:
            brand_from_db = self.session.query(Brand).filter(Brand.brand_id == brand.brand_id).one_or_none()
            if brand_from_db is None:
                try:
                    with self.session.begin_nested():
                        brand_from_db = Brand(brand_id=brand.brand_id, name=brand.name)
                        self.session.add(brand_from_db)
                except IntegrityError:
                    pass
            sku.brand_id = brand.brand_id
            try:
                with self.session.begin_nested():
                    self.session.add(sku)
                    self.session.flush()
            except IntegrityError:
                return 0
            return 1
        # if brand is not present in dict from wb api
        else:
            sku.clean_sku_post()
            return 0
