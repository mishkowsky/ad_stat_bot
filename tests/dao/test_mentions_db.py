import datetime
import time
from sqlalchemy import select
from src.dao.mentions_db import Post, Chat, ChatContentType, Sku, Brand, MentionsDatabase, SkuPerPost, Proxy
from src.parsers.telegram.chat import TgChatAdChatParser
from src.parsers.telegram.sku import TgWbItemsAdChatParser
from tests.conftest import *


@pytest.fixture(scope='function')
def mdb(db_session) -> MentionsDatabase:
    """
    creates MentionDatabase instance for testing
    :param db_session: generator of session object
    :return: created instance
    """
    return MentionsDatabase(db_session())


@pytest.fixture(scope='function')
def chat_test_objs(db_session) -> list[Chat]:
    """
    populates the database with test chats
    :param db_session: generator of session object
    :return: list of created chats
    """
    test_chats = [
        Chat(tg_id='1', link='t.me/+link1', chat_content=ChatContentType.wb_items_ads),
        Chat(tg_id='2', link='t.me/+link2', chat_content=ChatContentType.wb_items_ads),
        Chat(tg_id='3', link='t.me/+link3', chat_content=ChatContentType.wb_items_ads),
        Chat(tg_id='4', link='t.me/+link4', chat_content=ChatContentType.chat_ads)
    ]
    add_test_objs_to_db(db_session, test_chats)
    return test_chats


@pytest.fixture(scope='function')
def mentions_test_objs(chat_test_objs, db_session) -> list[SkuPerPost]:
    """
    populates the database with test posts
    :param chat_test_objs:
    :param db_session: generator of session object
    :return: list of created chats
    """

    session = db_session()

    brand_1 = Brand(brand_id=1, name='brand_1')
    brand_2 = Brand(brand_id=2, name='brand_2')

    session.add_all([brand_1, brand_2])
    session.flush()

    sku_1 = Sku(sku_code=10, brand=brand_1)
    sku_2 = Sku(sku_code=11, brand=brand_1)
    sku_3 = Sku(sku_code=20, brand=brand_2)

    session.add_all([sku_1, sku_2, sku_3])
    session.flush()

    post_1 = Post(chat=chat_test_objs[0], message_id=1, date=datetime.datetime(year=2010, month=5, day=19))
    post_2 = Post(chat=chat_test_objs[0], message_id=2, date=datetime.datetime(year=2011, month=4, day=23))
    post_3 = Post(chat=chat_test_objs[1], message_id=1, date=datetime.datetime(year=2010, month=10, day=1))

    session.add_all([post_1, post_2, post_3])
    session.flush()

    sku_per_post_1 = SkuPerPost(post=post_1, sku=sku_1)
    sku_per_post_2 = SkuPerPost(post=post_1, sku=sku_2)
    sku_per_post_3 = SkuPerPost(post=post_2, sku=sku_1)
    sku_per_post_4 = SkuPerPost(post=post_3, sku=sku_1)
    sku_per_post_5 = SkuPerPost(post=post_3, sku=sku_2)
    sku_per_post_6 = SkuPerPost(post=post_3, sku=sku_3)

    mentions = [sku_per_post_1, sku_per_post_2, sku_per_post_3, sku_per_post_4, sku_per_post_5, sku_per_post_6]

    session.add_all(mentions)
    session.flush()

    session.commit()

    return mentions


class TestBrand:

    def test_repr(self):
        obj_id = 1
        brand_id = 1
        name = 'name'
        brand = Brand(id=obj_id, brand_id=brand_id, name=name)

        assert f"<Brand(id='{obj_id}'; brand_id='{brand_id}'; name='{name}')>" == brand.__repr__()


class TestChat:

    def test_eq(self):
        chat_1 = Chat(tg_id='10')
        chat_2 = Chat(tg_id='10', link='link')
        chat_3 = Chat(link='link')
        not_chat = 1
        assert chat_1 == chat_2
        assert chat_2 == chat_3
        assert not_chat != chat_1

    def test_repr(self):
        obj_id = 1
        tg_id = '1'
        title = 't'
        link = 'l'
        chat = Chat(obj_id=obj_id, tg_id=tg_id, title=title, link=link)
        assert f"<Chat(id='{obj_id}'; tg_id='{tg_id}'; title='{title}'; link='{link}')>" == chat.__repr__()


class TestPost:

    def test_repr(self):
        obj_id = 1
        chat_id = 1
        message_id = '1'
        date = datetime.datetime.now()
        post = Post(id=obj_id, chat_id=chat_id, message_id=message_id, date=date)
        assert f"<Post(id='{obj_id}'; chat_id='{chat_id}'; msg_id='{message_id}'; date='{date}')>" == post.__repr__()

    def test_eq(self):
        post_1 = Post(chat_id=1, message_id='1')
        post_2 = Post(chat_id=1, message_id='1')
        not_post = 1
        assert post_1 == post_2
        assert post_2 != not_post


class TestSku:

    def test_repr(self):
        obj_id = 1
        sku_code = 1
        brand_id = 1
        sku = Sku(id=obj_id, sku_code=sku_code, brand_id=brand_id)
        assert f"<Sku(id='{obj_id}'; sku_code='{sku_code}'; brand_id='{brand_id}')>" == sku.__repr__()

    def test_eq(self):
        sku_1 = Sku(sku_code=1)
        sku_2 = Sku(sku_code=1)
        not_sku = 1
        assert sku_1 == sku_2
        assert sku_2 != not_sku


class TestSkuPerPost:

    def test_repr(self):
        obj_id = 1
        sku_code = 1
        post_id = 1
        sku_per_post = SkuPerPost(id=obj_id, sku_code=sku_code, post_id=post_id)
        assert f"<SkuPerPost(id='{obj_id}'; post_id='{post_id}'; sku_code='{sku_code}')>" == sku_per_post.__repr__()

    def test_eq(self):
        sku_per_post_1 = SkuPerPost(sku_code=1, post_id=1)
        sku_per_post_2 = SkuPerPost(sku_code=1, post_id=1)
        not_sku_per_post = 1
        assert sku_per_post_1 == sku_per_post_2
        assert sku_per_post_2 != not_sku_per_post

    def test_hash(self):
        sku_per_post_1 = SkuPerPost(sku_code=1, post_id=1)
        post = Post(id=1)
        sku_per_post_2 = SkuPerPost(sku_code=1, post=post)
        assert sku_per_post_1.__hash__() == sku_per_post_2.__hash__()


class TestProxy:

    def test_repr(self):
        proxy = Proxy(host='host', username='username', password='password', http_port=1, sock5_port=1)
        assert "<Proxy(id='None'; host='host'; user='username'; http='1'; sock5='1')>" == proxy.__repr__()

    def test_get_http_dict(self):
        proxy_1 = Proxy(host='host', username='username', password='password', http_port=1)
        connection_string = 'http://username:password@host:1'
        assert proxy_1.get_http_dict() == {
            'http': connection_string, 'https': connection_string
        }

        proxy_2 = Proxy(host='host', http_port=1)
        connection_string = 'http://host:1'
        assert proxy_2.get_http_dict() == {'http': connection_string, 'https': connection_string}

    def test_get_http_config_dict(self):
        proxy = Proxy(host='host', username='username', password='password', http_port=1)
        assert proxy.get_http_config_dict() == {'proxy_type': 'http', 'addr': 'host', 'port': 1,
                                                'username': 'username', 'password': 'password', 'rdns': True}


class TestMentionsDatabase:

    def test_get_chats_by_content_type(self, chat_test_objs, mdb):

        expected_chats_with_chat_ads = []
        expected_chats_with_wb_items_ads = []

        for chat in chat_test_objs:
            if chat.chat_content == ChatContentType.chat_ads:
                expected_chats_with_chat_ads.append(chat)
            else:
                expected_chats_with_wb_items_ads.append(chat)

        actual_chats_with_chat_ads = mdb.get_chats_by_content_type(ChatContentType.chat_ads)
        assert actual_chats_with_chat_ads == expected_chats_with_chat_ads

        actual_chats_with_wb_items_ads = mdb.get_chats_by_content_type(ChatContentType.wb_items_ads)
        assert actual_chats_with_wb_items_ads == expected_chats_with_wb_items_ads

    def test_upload_wb_items_ad_parser_results(self, mdb):

        chat = Chat(tg_id='1', link='t.me/+link1', chat_content=ChatContentType.wb_items_ads)

        mdb.session.add(chat)

        post = Post(chat_id=chat.id, message_id='1')
        post_duplicate = Post(chat_id=chat.id, message_id='1')
        posts_set = {post, post_duplicate}

        sku = Sku(sku_code=72136920)  # this sku will be loaded to db as it is present wb sku
        sku.sku_per_post.append(SkuPerPost(sku_code=sku.sku_code, post=post))

        sku_2 = Sku(sku_code=1)  # this sku will not be loaded to db as it is not present wb sku
        sku_2.sku_per_post.append(SkuPerPost(sku_code=sku_2.sku_code, post=post))

        skus_dict = {sku.sku_code: sku, sku_2.sku_code: sku_2}

        chat.recent_parsed_post_tg_id = post.message_id
        chat.update_required = True

        parser_result = TgWbItemsAdChatParser.Result(
            parsed_posts=posts_set,
            parsed_skus=skus_dict,
            parsed_tg_chats=set(),
            tg_chats_to_update=[chat]
        )

        time_before_upload = datetime.datetime.now()
        time.sleep(0.01)

        mdb.upload_wb_items_ad_parser_results(parser_result)

        time_after_upload = datetime.datetime.now()
        time.sleep(0.01)

        # test if correct skus were loaded
        actual_mentions = mdb.session.execute(select(SkuPerPost)).scalars().all()
        assert len(actual_mentions) == 1
        assert actual_mentions[0].sku_code == sku.sku_code

        # test that chat was updated
        updated_chat_from_db = mdb.session.execute(select(Chat).where(Chat.tg_id == chat.tg_id)).one_or_none()[0]
        assert updated_chat_from_db.recent_parsed_post_tg_id == post.message_id
        assert time_before_upload < updated_chat_from_db.updated_at < time_after_upload

        # test that no duplicated posts loaded
        assert len(mdb.session.execute(select(Post)).scalars().all()) == 1
        pass

    def test_upload_chat_ad_parser_result(self, chat_test_objs, db_session):
        parsed_tg_chats = {
            Chat(tg_id='4', link='t.me/+link4', chat_content=ChatContentType.chat_ads),  # this chat is duplicate
            Chat(tg_id='5', link='t.me/+link5', chat_content=ChatContentType.chat_ads),
            Chat(tg_id='6', link='t.me/+link6', chat_content=ChatContentType.wb_items_ads)
        }
        chat_to_update = chat_test_objs[0]
        chat_to_update.recent_parsed_post_tg_id = 123
        chat_to_update.update_required = True

        parser_result = TgChatAdChatParser.Result(parsed_tg_chats=parsed_tg_chats, tg_chats_to_update=[chat_to_update])
        mdb = MentionsDatabase(db_session())

        time_before_upload = datetime.datetime.now()
        time.sleep(0.01)

        mdb.upload_chat_ad_parser_result(parser_result)

        time.sleep(0.01)
        time_after_upload = datetime.datetime.now()

        actual_chats_in_db_len = len(mdb.session.execute(select(Chat)).scalars().all())
        expected_chats_in_db_len = \
            len(chat_test_objs) + len(parsed_tg_chats) - len(parsed_tg_chats.intersection(chat_test_objs))
        assert actual_chats_in_db_len == expected_chats_in_db_len

        updated_chat_from_db = \
            mdb.session.execute(select(Chat).where(Chat.tg_id == chat_to_update.tg_id)).one_or_none()[0]
        assert updated_chat_from_db.recent_parsed_post_tg_id == chat_to_update.recent_parsed_post_tg_id
        assert time_before_upload < updated_chat_from_db.updated_at < time_after_upload

    def test_upload_chats_to_db(self, chat_test_objs, db_session):
        extra_chats_to_load = {
            Chat(tg_id='4', link='t.me/+link4', chat_content=ChatContentType.chat_ads),  # this chat is duplicate
            Chat(tg_id='5', link='t.me/+link5', chat_content=ChatContentType.chat_ads),
            Chat(tg_id='6', link='t.me/+link6', chat_content=ChatContentType.wb_items_ads),
            Chat(link='t.me/+link7', chat_content=ChatContentType.wb_items_ads)
        }

        mdb = MentionsDatabase(db_session())
        mdb.upload_chats_to_db(extra_chats_to_load)
        mdb.session.commit()

        actual_chats_in_db_len = len(mdb.session.execute(select(Chat)).scalars().all())
        expected_chats_in_db_len = \
            len(chat_test_objs) + len(extra_chats_to_load) - len(extra_chats_to_load.intersection(chat_test_objs))
        assert actual_chats_in_db_len == expected_chats_in_db_len

    def test_get_mentions_by_sku(self, mentions_test_objs, db_session):

        mdb = MentionsDatabase(db_session())

        sku_code = 11

        expected_mentions = set()
        for mention in mentions_test_objs:
            if mention.sku_code == sku_code:
                expected_mentions.add(mention)

        actual_mentions = set()
        actual_mentions_dict = mdb.get_mentions_by_sku(sku_code)
        for chat in actual_mentions_dict.keys():
            for post in actual_mentions_dict[chat].keys():
                for mention in actual_mentions_dict[chat][post]:
                    actual_mentions.add(mention)

        assert actual_mentions == expected_mentions

    def test_get_mentions_by_brand(self, mentions_test_objs, db_session):

        mdb = MentionsDatabase(db_session())

        brand = 'brand_1'

        expected_mentions = set()
        for mention in mentions_test_objs:
            if mention.sku.brand.name == brand:
                expected_mentions.add(mention)

        actual_mentions = set()
        actual_mentions_dict = mdb.get_mentions_by_brand(brand)
        for chat in actual_mentions_dict.keys():
            for post in actual_mentions_dict[chat].keys():
                for mention in actual_mentions_dict[chat][post]:
                    actual_mentions.add(mention)

        assert actual_mentions == expected_mentions

        brand = 'this_brand_name_is_not_present_in_db'
        actual_mentions = mdb.get_mentions_by_brand(brand)
        assert actual_mentions == {}

    def test_update_tg_chat(self, mdb, chat_test_objs):

        new_title = 'new_title'
        chat_to_update = chat_test_objs[0]
        chat_to_update.title = new_title
        chat_to_update.update_required = True

        time_before_update = datetime.datetime.now()
        time.sleep(0.01)

        mdb.update_tg_chat(chat_to_update)

        time.sleep(0.01)
        time_after_update = datetime.datetime.now()

        updated_chat = mdb.session.execute(select(Chat).where(Chat.id == chat_to_update.id)).scalars().one_or_none()
        assert updated_chat.title == new_title
        assert time_before_update < updated_chat.updated_at < time_after_update

    def test_update_tg_chat_without_update_time(self, mdb, chat_test_objs):

        new_title = 'new_title'
        chat_to_update = chat_test_objs[0]
        chat_to_update.title = new_title
        chat_to_update.update_required = True

        mdb.update_tg_chat_without_update_time(chat_to_update)

        updated_chat = mdb.session.execute(select(Chat).where(Chat.id == chat_to_update.id)).scalars().one_or_none()
        assert updated_chat.title == new_title
        assert updated_chat.updated_at is None

        chat_to_update = chat_test_objs[1]
        last_name = chat_to_update.title
        chat_to_update.title = new_title

        mdb.update_tg_chat_without_update_time(chat_to_update)

        updated_chat = mdb.session.execute(select(Chat).where(Chat.id == chat_to_update.id)).scalars().one_or_none()
        assert updated_chat.title != new_title
        assert updated_chat.title == last_name
