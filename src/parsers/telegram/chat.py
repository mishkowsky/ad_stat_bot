import re
from dataclasses import dataclass
from datetime import datetime
from loguru import logger
from telethon.tl.types import MessageEntityTextUrl
from telethon.tl.patched import Message
from src.dao.mentions_db import ChatContentType
from src.dao.mentions_db import Chat
from src.parsers.telegram.abstract import AbstractTgChatParser
from src.parsers.telegram.utils import get_chat_info_by_link, refactor_tg_url


class TgChatAdChatParser(AbstractTgChatParser):

    tg_link_pattern = re.compile(r'(?:(?:telegram\.(?:me|dog)|t\.me)/(?:@|\+|joinchat/)?' 
                                 r'|tg://join\?invite=)'
                                 r'[a-zA-Z0-9.+_-]+(?=/)?')
    tg_mention_pattern = re.compile(r'(?<=@)[a-zA-Z0-9_]+')

    chats_type = ChatContentType.chat_ads

    def __init__(self, session_id: int, tg_chats_to_parse: list[Chat], start_date: datetime):
        super().__init__(session_id, tg_chats_to_parse, start_date)
        self.parsed_links: set[str] = set()

    def parse_message(self, message: Message) -> list[Chat]:
        logger.debug(f'PARSING CHAT LINKS')
        links = set()
        for url_entity, inner_text in message.get_entities_text(MessageEntityTextUrl):
            # <editor-fold desc="log">
            logger.debug(f'INNER TEXT: {inner_text}; LINK: {url_entity}')
            # </editor-fold>
            if 'отзыв' not in inner_text.lower():
                links = links.union(self.find_tg_links(url_entity.url))
        links = links.union(self.find_tg_links(message.message))
        links = links.union(self.find_tg_mentions(message.message))

        # <editor-fold desc="log">
        logger.debug(f'PARSED {len(links)} FROM {message.id} MSG.ID: {links}')
        # </editor-fold>

        resolved_tg_chats = []
        for link in links:
            self.parsed_links.add(link)
            title, members_count = get_chat_info_by_link(link)
            if title is not None and 'отзыв' not in title.lower():
                resolved_tg_chats.append(Chat(link=link, chat_content=ChatContentType.wb_items_ads,
                                              title=title, followers=members_count, update_required=True))
        return resolved_tg_chats

    def find_tg_links(self, url: str) -> set[str]:
        links = set()
        match_results = self.tg_link_pattern.findall(url)
        for match_result in match_results:
            refactored_url = refactor_tg_url(match_result)
            if refactored_url not in self.parsed_links:
                links.add(refactored_url)
        return links

    def find_tg_mentions(self, message) -> set[str]:
        mentions = set()
        match_result = self.tg_mention_pattern.findall(message)
        for mention in match_result:
            tg_link = f't.me/{mention}'
            if tg_link not in self.parsed_links:
                mentions.add(tg_link)
        return mentions

    @dataclass
    class Result(AbstractTgChatParser.AbstractResult):
        parsed_tg_chats: set[Chat]

        def merge_with(self, another_parser_result):
            super().merge_with(another_parser_result)
            self.parsed_tg_chats = self.parsed_tg_chats.union(another_parser_result.parsed_tg_chats)

        def get_parsed_items_count(self):
            return len(self.parsed_tg_chats)

    def get_parser_results(self) -> Result:
        return TgChatAdChatParser.Result(self.get_tg_chats_to_update(), self.parsed_items)
