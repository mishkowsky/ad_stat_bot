import datetime
from telethon.tl.patched import Message
from src.parsers.telegram.abstract import AbstractTgChatParser


class TestAbstractTgChatParser:

    def test_parse_message(self):
        AbstractTgChatParser.__abstractmethods__ = set()
        parser = AbstractTgChatParser(0, [], datetime.datetime.now())
        res = parser.parse_message(Message(0))
        assert res is None

        res = parser.get_parser_results()
        assert res is None
