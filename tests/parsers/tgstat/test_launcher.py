import os
from freezegun import freeze_time
from src.dao.mentions_db import Post, MentionsDatabase, ChatContentType
from tests.parsers.conftest import assert_parser_posts_result


def test_launch_many_parsers(chats_test_objs, proxy_test_obj, db_session):
    """
    testing launch_many_parsers() function
    :param chats_test_objs: fixture to put test chat records into test_db
    :param proxy_test_obj: fixture to put test proxy record into test_db
    :param db_session: create test_db fixture
    """

    if os.name == 'nt':
        # this test runs correctly only under linux
        return

    from src.parsers.tgstat.launcher import launch_many_parsers
    launch_many_parsers()

    session = db_session()
    loaded_posts = session.query(Post).all()

    assert_parser_posts_result(loaded_posts)


@freeze_time("2023-12-12")
def test_launch_parser(chats_test_objs, db_session):
    """
    testing launch_parser() function
    :param chats_test_objs: put test chats into test_db fixture
    :param db_session: create test_db fixture
    """

    session = db_session()
    database = MentionsDatabase(session)
    chats = database.get_chats_by_content_type(ChatContentType.wb_items_ads)

    from src.parsers.tgstat.launcher import launch_parser
    launch_parser(chats, None)
    loaded_posts = session.query(Post).all()

    assert_parser_posts_result(loaded_posts)
