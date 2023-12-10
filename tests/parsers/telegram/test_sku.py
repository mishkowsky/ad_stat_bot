from datetime import datetime
from config import *
from src.dao.mentions_db import Chat, Post, Sku
from tests.parsers.conftest import assert_parser_posts_result, expected_parsed_links


class TestTgWbItemsAdChatParser:

    def test_launch(self):
        """
        testing main launch() function of TgWbItemsAdChatParser class
        """

        session_id = 1
        chats = [Chat(link='t.me/+hxHHhoNwN2ZmZTNi', tg_id='1944859703', followers=3),
                 Chat(link='t.me/testingpublicchannel', followers=3),
                 Chat(link='t.me/+jidLb0JqeedkZTMy', tg_id='1928374120', followers=2)]
        start_date = datetime(year=2023, month=12, day=1)

        from src.parsers.telegram.sku import TgWbItemsAdChatParser
        parser = TgWbItemsAdChatParser(session_id, chats, start_date)

        session_file_path = rf'{ROOT_DIR}/{SESSIONS_FILE_PATH}/{session_id}/anon'
        parser.launch(session_file_path, API_IDS[session_id], API_HASHES[session_id], None)
        result = parser.get_parser_results()

        actual_parsed_links = {tg_chat.link for tg_chat in result.parsed_tg_chats}
        assert actual_parsed_links == expected_parsed_links

        assert_parser_posts_result(result.parsed_posts)

        chats = [Chat(link='t.me/@fdgwerx', tg_id=None, followers=None)]
        parser = TgWbItemsAdChatParser(session_id, chats, start_date)
        parser.launch(session_file_path, API_IDS[session_id], API_HASHES[session_id], None)
        assert parser.get_parser_results().get_parsed_items_count() == 0


class TestTgWbItemsAdChatParserResult:

    def test_merge_with(self):
        from src.parsers.telegram.sku import TgWbItemsAdChatParser
        chats = [Chat(tg_id='1'), Chat(tg_id='2')]
        posts = [Post(chat_id='1', message_id=1), Post(chat_id='1', message_id=2)]
        res_1 = TgWbItemsAdChatParser.Result([], {chats[0]}, {posts[0]}, {1: Sku(sku_code=1)})
        res_2 = TgWbItemsAdChatParser.Result([], {chats[1]}, {posts[1]}, {2: Sku(sku_code=2)})
        res_1.merge_with(res_2)

        assert res_1.get_parsed_items_count() == len(posts)
