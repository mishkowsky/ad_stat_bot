import asyncio
from tests.parsers.conftest import assert_parser_posts_result
from freezegun import freeze_time


@freeze_time("2023-12-03")
def test_launch_parser(chats_test_objs, proxy_test_obj, db_session):
    """
    testing launch_parser() function
    :param chats_test_objs: fixture to put test chat records into test_db
    :param proxy_test_obj: fixture to put test proxy record into test_db
    :param db_session: create test_db fixture
    """

    from src.parsers.telegram.launcher import ParserLauncher
    p_l = ParserLauncher()
    asyncio.run(p_l.launch_all_parsers())

    from src.dao.mentions_db import Post
    session = db_session()
    loaded_posts = session.query(Post).all()
    assert_parser_posts_result(loaded_posts)
