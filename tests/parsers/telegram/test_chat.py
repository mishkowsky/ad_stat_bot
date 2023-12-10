from datetime import datetime
from config import *
from src.dao.mentions_db import Chat
from src.parsers.telegram.chat import TgChatAdChatParser
from tests.parsers.conftest import expected_parsed_links


class TestTgChatAdChatParser:

    def test_launch(self):
        """
        testing main launch() function of TgChatAdChatParser class
        """

        session_id = 1
        chats = [Chat(link='https://t.me/+hxHHhoNwN2ZmZTNi', tg_id='1944859703', followers=3)]
        start_date = datetime(year=2023, month=12, day=1)

        from src.parsers.telegram.chat import TgChatAdChatParser
        parser = TgChatAdChatParser(session_id, chats, start_date)

        session_file_path = rf'{ROOT_DIR}/{SESSIONS_FILE_PATH}/{session_id}/anon'
        parser.launch(session_file_path, API_IDS[session_id], API_HASHES[session_id], None)
        result = parser.get_parser_results()

        actual_parsed_links = {tg_chat.link for tg_chat in result.parsed_tg_chats}
        assert actual_parsed_links == expected_parsed_links

        actual_links = set()
        for chat in result.parsed_tg_chats:
            actual_links.add(chat.link)
            if chat.link == 't.me/+2V7YyltP17E3ODUy':
                assert chat.title == 'TestChannel'


class TestTgChatAdChatParserResult:

    def test_merge_with(self):
        chats = [Chat(tg_id='1'), Chat(tg_id='2')]
        res_1 = TgChatAdChatParser.Result([], {chats[0]})
        res_2 = TgChatAdChatParser.Result([], {chats[1]})
        res_1.merge_with(res_2)

        assert res_1.get_parsed_items_count() == len(chats)
