from datetime import datetime
from config import *
from src.dao.mentions_db import Chat
from tests.parsers.conftest import assert_parser_posts_result, expected_parsed_links


def test_launch():
    """
    testing main launch() function of TgWbItemsAdChatParser class
    """

    session_id = 1
    chats = [Chat(link='https://t.me/+hxHHhoNwN2ZmZTNi', tg_id='1944859703', followers=3)]
    start_date = datetime(year=2023, month=12, day=1)

    from src.parsers.telegram.sku import TgWbItemsAdChatParser
    parser = TgWbItemsAdChatParser(session_id, chats, start_date)

    session_file_path = rf'{ROOT_DIR}/{SESSIONS_FILE_PATH}/{session_id}/anon'
    parser.launch(session_file_path, API_IDS[session_id], API_HASHES[session_id], None)
    result = parser.get_parser_results()

    actual_parsed_links = {tg_chat.link for tg_chat in result.parsed_tg_chats}
    assert actual_parsed_links == expected_parsed_links

    assert_parser_posts_result(result.parsed_posts)
