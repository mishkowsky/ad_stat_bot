from sqlalchemy import select
from sqlalchemy.orm import Session
from src.dao.mentions_db import Chat, ChatContentType, Proxy, Post
from tests.conftest import *

"""
expected posts with skus from test telegram channel https://t.me/+hxHHhoNwN2ZmZTNi
"""
expected_post_dict = {
    '6': {'reactions_count': 1, 'comments_count': 0, 'skus': {164683588, 175492260, 175490896, 174506579, 176446595,
                                                              115052453, 141418697, 119923290, 106483377, 138922750,
                                                              148332819}},
    '7': {'reactions_count': 0, 'comments_count': 0, 'skus': {138922750}},
    '8': {'reactions_count': 0, 'comments_count': 1, 'skus': {138922750}},
    '12': {'reactions_count': 0, 'comments_count': 0, 'skus': {179738820}},
    '14': {'reactions_count': 0, 'comments_count': 0, 'skus': {15180390}},
    '15': {'reactions_count': 0, 'comments_count': 0, 'skus': {178992783, 178992791}},
    '16': {'reactions_count': 0, 'comments_count': 0, 'skus': {145007641}},
    '17': {'reactions_count': 0, 'comments_count': 0, 'skus': {115588584}},
    '18': {'reactions_count': 0, 'comments_count': 0, 'skus': {181551018, 181549780, 181547208, 181547210}},
    '19': {'reactions_count': 0, 'comments_count': 0, 'skus': {192263981}},
    '26': {'reactions_count': 0, 'comments_count': 0, 'skus': {186952623}},
    '27': {'reactions_count': 0, 'comments_count': 0, 'skus': {185281602}},
}

"""
expected links to other telegram channels from test telegram channel https://t.me/+hxHHhoNwN2ZmZTNi
"""
expected_parsed_links = {
    't.me/joinchat/ob_lsojTOVJhZGIy',
    't.me/+2V7YyltP17E3ODUy',
    't.me/telegram'
}


def get_skus_from_post(post: Post) -> set[int]:
    """
    Collects skus from post entity
    :param post: db post entity
    :return: set of skus
    """
    skus = set()
    for sku_per_post in post.sku_per_post:
        skus.add(sku_per_post.sku.sku_code)
    return skus


def assert_parser_posts_result(parsed_posts: list[Post]) -> None:
    """
    iterates through the parsed posts and compares them with the expected_post_dict
    :param parsed_posts: list of posts from database
    """
    assert len(parsed_posts) == len(expected_post_dict)

    for post in parsed_posts:
        actual_skus = get_skus_from_post(post)
        expected_skus = expected_post_dict[post.message_id]['skus']
        assert actual_skus == expected_skus
        assert post.reactions_count == expected_post_dict[post.message_id]['reactions_count']
        assert post.comments_count == expected_post_dict[post.message_id]['comments_count']


@pytest.fixture(scope='function')
def chats_test_objs(db_session) -> list[Chat]:
    """
    populates the database with test chats
    :param db_session: generator of session object
    :return: list of created chats
    """
    test_chats = [
        Chat(link='t.me/+qC9nbDs1_N9iNzIy', session_id=1, tg_id='1944859703',
             followers=3, chat_content=ChatContentType.wb_items_ads),
        Chat(link='t.me/testingpublicchannel', recent_parsed_post_tg_id=6,
             chat_content=ChatContentType.wb_items_ads)
    ]
    add_test_objs_to_db(db_session, test_chats)
    return test_chats


@pytest.fixture(scope='function')
def proxy_test_obj(db_session) -> Proxy:
    """
    gets one proxy from db and populates test_db with it
    :param db_session: generator of session object
    :return: created proxy entity
    """
    db_config_ = DBConfigInstance(
        DBConfig(
            DBMS=os.getenv('DBMS'),
            DRIVER=os.getenv('DB_DRIVER'),
            HOSTNAME=os.getenv('DB_HOSTNAME'),
            DATABASE=os.getenv('DB_DATABASE'),
            USERNAME=os.getenv('DB_USERNAME'),
            PASSWORD=os.getenv('DB_PASSWORD'),
            config_name='debugging_config'
        ))

    engine = create_engine(db_config_.DB_URI)
    session = Session(bind=engine)

    proxy_from_real_db = session.execute(select(Proxy)).scalars().one_or_none()
    test_proxy = Proxy(host=proxy_from_real_db.host, username=proxy_from_real_db.username,
                       password=proxy_from_real_db.password, http_port=proxy_from_real_db.http_port)

    session.close()
    engine.dispose()

    add_test_objs_to_db(db_session, [test_proxy])
    return test_proxy
