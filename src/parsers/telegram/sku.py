from dataclasses import dataclass
from datetime import datetime
from loguru import logger
from telethon.tl.types import PeerChat, PeerChannel
from telethon.tl.patched import Message
from src.dao.mentions_db import Post, Sku, SkuPerPost, ChatContentType
from src.parsers.telegram.chat import TgChatAdChatParser
from src.utils.wb_utils import wb_sku_pattern, wb_size_pattern, wb_link_pattern
from src.dao.mentions_db import Chat
from src.utils import LinkSkuResolver


class TgWbItemsAdChatParser(TgChatAdChatParser):

    chats_type = ChatContentType.wb_items_ads

    def __init__(self, session_id: int, tg_chats_to_parse: list[Chat], start_date: datetime):
        super().__init__(session_id, tg_chats_to_parse, start_date)
        self.parsed_tg_chats: set[Chat] = set()
        self.parsed_sku_db_instances: dict[int, Sku] = dict()

    def parse_message(self, message: Message) -> set[Post]:
        parsed_tg_chats_from_message = super().parse_message(message)
        self.parsed_tg_chats = self.parsed_tg_chats.union(parsed_tg_chats_from_message)

        if message.fwd_from is not None:  # skip if message is reply
            return set()

        skus = LinkSkuResolver().get_skus_from_telethon_message(message)
        wb_links = wb_link_pattern.findall(message.message)
        for wb_link in wb_links:
            skus.add(int(wb_sku_pattern.findall(wb_link)[0]))
        skus = skus.difference([int(s) for s in wb_size_pattern.findall(message.message)])
        # <editor-fold desc="log">
        logger.debug(f'SKUS FOR MSG_ID: {message.id} ARE: {skus}')
        # </editor-fold>
        chat_id = None
        if isinstance(message.peer_id, PeerChat):
            chat_id = message.peer_id.chat_id
        elif isinstance(message.peer_id, PeerChannel):
            chat_id = message.peer_id.channel_id
        if chat_id is None:
            return set()

        if len(skus) == 0:
            return set()

        date = message.date                                                     # publication date
        forwards_count = message.forwards or 0  # int                           # share count
        views_count = message.views or 0        # int or None if it is chat     # views
        reactions_count = 0                                                     # reactions if present
        if message.reactions is not None:
            for reaction_count_entity in message.reactions.results:
                reactions_count = reactions_count + reaction_count_entity.count

        if message.replies is not None:
            replies_count = message.replies.replies or 0                        # comments
        else:
            replies_count = 0

        followers = self.get_followers_by_chat_id(chat_id)                      # followers
        er = (replies_count + reactions_count) / followers * 100                # er

        post = Post(message_id=str(message.id), chat_id=self.get_chat_id_by_tg_chat_id(chat_id),
                    views_count=views_count, replies_count=replies_count, shared_count=forwards_count,
                    er=er, reactions_count=reactions_count, comments_count=replies_count, date=date)

        for sku in skus:
            if sku not in self.parsed_sku_db_instances.keys():
                sku_db_instance = Sku(sku_code=sku)
                self.parsed_sku_db_instances[sku] = sku_db_instance
            else:
                sku_db_instance = self.parsed_sku_db_instances[sku]

            sku_db_instance.sku_per_post.append(SkuPerPost(sku_code=sku, post=post))
        return {post}

    def get_followers_by_chat_id(self, tg_chat_id: int) -> int:
        for tg_chat in self.tg_chats_to_parse:
            if tg_chat.tg_id == str(tg_chat_id):
                return tg_chat.followers

    def get_chat_id_by_tg_chat_id(self, tg_chat_id: int) -> int:
        for tg_chat in self.tg_chats_to_parse:
            if tg_chat.tg_id == str(tg_chat_id):
                return tg_chat.id

    @dataclass
    class Result(TgChatAdChatParser.Result):
        """
        Wrapper for parser results
        """
        parsed_posts: set[Post]
        parsed_skus: dict[int, Sku]

        def merge_with(self, another_parser_result) -> None:
            super().merge_with(another_parser_result)
            self.parsed_posts = self.parsed_posts.union(another_parser_result.parsed_posts)
            self.parsed_skus.update(another_parser_result.parsed_skus)

        def get_parsed_items_count(self) -> int:
            """
            :return: amount of parsed mentions
            """
            return len(self.parsed_skus)

    def get_parser_results(self) -> Result:
        return TgWbItemsAdChatParser.Result(self.get_tg_chats_to_update(), self.parsed_tg_chats,
                                            self.parsed_items, self.parsed_sku_db_instances)
