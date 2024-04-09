import enum
from datetime import datetime
from loguru import logger
from sqlalchemy import Column, DateTime, ForeignKey, Identity, Integer, String, text, MetaData, Enum, \
    orm, Float, func, and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, relationship, Session
from src.utils.wb_utils import get_brands_by_skus, BrandRec

metadata_obj = MetaData(schema='mentions')
Base = declarative_base(metadata=metadata_obj)


# ORM of mentions schema
# https://dbdiagram.io/d/parser_result_post-6508a60idc02bd1c4a5ece5ba9


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


class Chat(Base):
    __tablename__ = 'chat'

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

    post = relationship('Post', back_populates='chat')

    def __init__(self, obj_id=None, link=None, title=None, tg_id=None, followers=None, chat_content=None,
                 updated_at=None,
                 created_at=datetime.now(), session_id=None, update_required=False, recent_parsed_post_tg_id=None):
        super().__init__(
            id=obj_id, link=link, title=title, followers=followers, chat_content=chat_content, updated_at=updated_at,
            created_at=created_at, session_id=session_id, recent_parsed_post_tg_id=recent_parsed_post_tg_id)
        self.id = obj_id
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
        return "<Chat(id='%s'; tg_id='%s'; title='%s'; link='%s')>" % \
            (self.id, self.tg_id, self.title, self.link)

    def __eq__(self, obj):
        if not isinstance(obj, Chat):
            return False
        if self.tg_id is not None and obj.tg_id is not None:
            return obj.tg_id == self.tg_id
        return obj.link == self.link

    def __hash__(self):
        return hash(self.link)


class Post(Base):
    __tablename__ = 'post'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    chat_id = Column(ForeignKey('chat.id'))
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

    chat = relationship('Chat', back_populates='post')
    sku_per_post = relationship('SkuPerPost', back_populates='post')

    def __repr__(self):
        return "<Post(id='%s'; chat_id='%s'; msg_id='%s'; date='%s')>" % \
            (self.id, self.chat_id, self.message_id, self.date)

    def __eq__(self, obj):
        if not isinstance(obj, Post):
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

    def __eq__(self, obj):
        if not isinstance(obj, Sku):
            return False
        return obj.sku_code == self.sku_code


class SkuPerPost(Base):
    __tablename__ = 'sku_per_post'

    id = Column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    post_id = Column(ForeignKey('post.id'))
    sku_code = Column(ForeignKey('sku.sku_code'))

    post = relationship('Post', back_populates='sku_per_post')
    sku = relationship('Sku', back_populates='sku_per_post', cascade='merge')

    def __repr__(self):
        return "<SkuPerPost(id='%s'; post_id='%s'; sku_code='%s')>" % \
            (self.id, self.post_id, self.sku_code)

    def __eq__(self, obj):
        if not isinstance(obj, SkuPerPost):
            return False
        return obj.post_id == self.post_id and obj.sku_code == self.sku_code

    def __hash__(self):
        if self.post_id is None:
            post_id = self.post.id
        else:
            post_id = self.post_id
        return hash((post_id, self.sku_code))


class Proxy(Base):
    __tablename__ = 'proxies'

    id = Column(Integer,
                Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
                primary_key=True)
    host = Column(String)
    username = Column(String)
    password = Column(String)
    http_port = Column(Integer)
    sock5_port = Column(Integer)

    def get_http_dict(self) -> dict[str, str]:
        """
        dict for requests library
        :return: dict with schema as key, connection str as value
        """
        if self.username is not None:
            http_url = f'http://{self.username}:{self.password}@{self.host}:{self.http_port}'
        else:
            http_url = f'http://{self.host}:{self.http_port}'
        return {'http': http_url, 'https': http_url}

    def get_http_config_dict(self) -> dict[str, str]:
        """
        dict for telethon library
        :return: dict with proxy properties
        """
        return {
            'proxy_type': 'http',  # (mandatory) protocol to use
            'addr': self.host,  # (mandatory) proxy IP address
            'port': self.http_port,  # (mandatory) proxy port number
            'username': self.username,  # (optional) username if the proxy requires auth
            'password': self.password,  # (optional) password if the proxy requires auth
            'rdns': True  # (optional) whether to use remote or local resolve, default remote
        }

    def __repr__(self):
        return "<Proxy(id='%s'; host='%s'; user='%s'; http='%s'; sock5='%s')>" % \
            (self.id, self.host, self.username, self.http_port, self.sock5_port)


class MentionsDatabase:
    """
   Class to interact with parsers_bd.top_blogger_bot_schema
   """

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_chats_by_content_type(self, chat_content_type: ChatContentType) -> list[Chat]:
        """
        filter chats by content type
        :param chat_content_type: value to filter
        :return: collection of Chats
        """
        result = self.session.execute(select(Chat).where(Chat.chat_content == chat_content_type)).scalars().all()
        return list(result)

    def upload_wb_items_ad_parser_results(self, parser_result) -> None:
        """
        :param parser_result: object type of TgWbItemsAdChatParserResult
        """
        for tg_chat in parser_result.tg_chats_to_update:
            self.update_tg_chat(tg_chat)
        self.upload_chats_to_db(parser_result.parsed_tg_chats)
        self.upload_tg_posts_to_db(parser_result.parsed_posts, parser_result.parsed_skus)
        self.session.commit()

    def upload_chat_ad_parser_result(self, parser_result):
        """
        :param parser_result: object type of TgChatAdChatParserResult
        """
        for tg_chat in parser_result.tg_chats_to_update:
            self.update_tg_chat(tg_chat)
        self.upload_chats_to_db(parser_result.parsed_tg_chats)
        self.session.commit()

    def upload_chats_to_db(self, tg_chats: set[Chat]) -> None:
        """
        Uploads tg_chats to db if db doesn't has entry with same tg_id or link
        :param tg_chats: iterable with elements type of TgChatsToParse
        """
        uploaded_chats_counter = 0
        if len(tg_chats) != 0:
            for tg_chat in tg_chats:
                if tg_chat.tg_id is not None:
                    filter_condition = Chat.tg_id == tg_chat.tg_id
                else:
                    filter_condition = Chat.link == tg_chat.link
                if self.session.query(Chat).filter(filter_condition).one_or_none() is None:
                    self.session.add(tg_chat)
                    uploaded_chats_counter = uploaded_chats_counter + 1
            logger.info(f'UPLOADED {uploaded_chats_counter} NEW CHATS')

    def get_mentions_by_sku(self, sku_code: int) -> \
            dict[Chat, dict[Post, set[SkuPerPost]]]:
        """
        method for obtaining a dictionary of mentions filtered by sku code
        :param sku_code: sku to filter
        :return: dict with mentions per posts per chats
        """
        mentions = self.session.execute(
            select(SkuPerPost).where(SkuPerPost.sku_code == sku_code)
        ).scalars().all()
        return self.generate_mentions_dict(set(mentions))

    def get_mentions_by_brand(self, brand_name: str) -> \
            dict[Chat, dict[Post, set[SkuPerPost]]]:
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
        mentions = self.session.execute(
            select(SkuPerPost).where(SkuPerPost.sku_code.in_(brand_skus_list))
        ).scalars().all()
        return self.generate_mentions_dict(set(mentions))

    def generate_mentions_dict(self, mentions: set[SkuPerPost]) -> \
            dict[Chat, dict[Post, set[SkuPerPost]]]:
        """
        creates dict from orm objects
        :param mentions: list of orm of SkuPerPost class
        :return: dict with mentions per posts per chats
        """
        resulting_dict = {}
        for mention in mentions:
            chat: Chat = mention.post.chat
            post: Post = mention.post
            if chat in resulting_dict.keys():
                if post in resulting_dict[chat].keys():
                    resulting_dict[chat][post].add(mention)
                else:
                    resulting_dict[chat][post] = {mention}
            else:
                resulting_dict[chat] = {post: {mention}}
        return resulting_dict

    def update_tg_chat(self, tg_chat: Chat) -> None:
        """
        Updates entry in top_blogger_stat_bot.tg_chats_to_parse table
        :param tg_chat: object type of TgChatsToParse to update
        """
        tg_chat.updated_at = datetime.now()
        self.update_tg_chat_without_update_time(tg_chat)

    def update_tg_chat_without_update_time(self, tg_chat: Chat) -> None:
        if not tg_chat.update_required:
            return
        tg_chat_dict = vars(tg_chat).copy()
        for var in vars(tg_chat):
            if var.startswith('_') or var == 'update_required':
                tg_chat_dict.pop(var)
        tg_chat.update_required = False
        self.session.query(Chat). \
            filter(Chat.id == tg_chat.id). \
            update(tg_chat_dict, synchronize_session=False)
        self.session.commit()

    def upload_tg_posts_to_db(self, parsed_posts: set[Chat], parsed_skus: dict[int, Sku]) -> (int, int):
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
            post_from_db = self.session.query(Post). \
                filter(and_(Post.chat_id == post.chat_id,
                            Post.message_id == post.message_id)).one_or_none()
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
                except IntegrityError:  # pragma: no cover
                    pass
            sku.brand_id = brand.brand_id
            try:
                with self.session.begin_nested():
                    self.session.add(sku)
                    self.session.flush()
            except IntegrityError:  # pragma: no cover
                return 0
            return 1
        # if brand is not present in dict from wb api
        else:
            sku.clean_sku_post()
            return 0
