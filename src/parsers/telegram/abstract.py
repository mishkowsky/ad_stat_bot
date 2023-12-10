import asyncio
import threading
import time
from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import datetime
from loguru import logger
from opentele.api import API
from opentele.tl import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Chat, ChatEmpty, Channel
from telethon.tl.patched import Message
from config import ROOT_DIR, THREAD_LOGGER_FORMAT, LOGGER_LEVEL
from src.dao.mentions_db import Chat
from src.utils import format_message_to_print
from src.parsers.telegram.utils import get_list_of_chat_ids, send_join_requests


class AbstractTgChatParser(ABC):
    """
    Abstract parser for telegram chats/channels
    """

    chats_type = None

    def __init__(self, session_id: int, tg_chats_to_parse: list[Chat], start_date: datetime):
        """
        :param session_id: id of telegram account, relevant api_id, api_hash must be passed to launch method
        """
        self.client: TelegramClient | None = None  # client is created at parse() method
        self.time_to_sleep: int = 0
        self.session_id = session_id
        self.chat_ids: set[int] = set()
        self.tg_chats_to_parse: list[Chat] = tg_chats_to_parse
        self.joined_chats_id: set[int] = set()
        self.chats: list[Chat] = list()
        self.parsed_items: set = set()
        self.chats_count: int = 0
        self.total_message_counter: int = 0
        self.start_date: datetime = start_date
        self.processed_chats_id: set[int] = set()

    def add_logger(self) -> None:
        """
        adds logger output to file
        """
        thread_id = threading.get_native_id()
        output_log_file = f'{ROOT_DIR}/logs/{self.__class__.__name__}/Thread-{self.session_id}/' \
                          f'{datetime.now().strftime("%d.%m.%Y_%H.%M")}/log.txt'
        logger.debug(f'ADDING LOGGER TO {output_log_file}')
        logger.add(output_log_file, format=THREAD_LOGGER_FORMAT, level=LOGGER_LEVEL,
                   filter=lambda record: record['thread'].id == thread_id)

    async def parse(self, anon_path: str, api_id: int, api_hash: str, proxy_config: dict[str, str]) -> set:
        """
        Resolves chat entities, joins them, scans and parses items from messages from chats.
        The method will be called recursively after flood timeout if a FloodWaitError occurs when sending join requests.
        """
        api = API.TelegramDesktop.Generate(unique_id=str(api_id))
        self.client = TelegramClient(session=anon_path, api_id=api_id, api_hash=api_hash, proxy=proxy_config, api=api)
        logger.debug('CONNECTING TO CLIENT')
        async with self.client:
            # <editor-fold desc="log">
            logger.info('STARTED')
            # </editor-fold>
            await self.fill_chats()
            await self.process_chats()

            # if time_to_sleep != 0 means that we have caught FloodWaitError and
            # there still chats to parse that we haven't joined (due to Flood)
            if self.time_to_sleep != 0:  # pragma: no cover
                time.sleep(self.time_to_sleep)
                self.time_to_sleep = 0
                # we don't care about return value because return value is field of self
                _ = await self.parse(anon_path, api_id, api_hash, proxy_config)
        # <editor-fold desc="log">
        logger.info('JOB DONE!')
        # </editor-fold>
        return self.parsed_items

    async def fill_chats(self) -> None:
        """
        Method to get chat entities by tg_id or send join request using link.
        Results stored in self.chats.
        """
        tg_chat_ids = []
        tg_chats_to_join = []
        for tg_chat in self.tg_chats_to_parse:
            # tg_chat.tg_id is not None means we have received chat entity earlier, so we are already joined this chat
            if tg_chat.tg_id is not None and tg_chat.tg_id not in self.processed_chats_id:
                tg_chat_ids.append(int(tg_chat.tg_id))
            else:  # pragma: no cover
                tg_chats_to_join.append(tg_chat)

        # we will not send extra requests on the next recursive calls because chats that were successfully parsed have
        # tg_chat.tg_id and this ids will be in the self.processed_chats_id
        try:
            self.chats.extend(await send_join_requests(self.client, tg_chats_to_join, self.session_id))
        except FloodWaitError as e:  # pragma: no cover
            self.time_to_sleep = e.seconds

        self.chats.extend(await self.client.get_entity(tg_chat_ids))

        self.chats_count = len(self.chats)
        await self.get_chats_info()
        self.joined_chats_id = await get_list_of_chat_ids(self.client)

    async def process_chats(self):
        """
        Iterates over self.chats, calls scans_messages for chats that were not processed yet.
        """
        for index, chat in enumerate(self.chats):
            # <editor-fold desc="log">
            logger.info(f'LOOKING FOR MESSAGES IN "{chat.title}" DATED FROM {self.start_date}')
            # </editor-fold>
            # if multiple links from db leads to same chat, or we already processed this chat earlier
            if chat.id not in self.processed_chats_id:
                await self.scan_messages(chat, self.start_date)
                self.processed_chats_id.add(chat.id)
            # <editor-fold desc="log">
            logger.info(
                f'TOTALLY PARSED {len(self.parsed_items)} UNIQUE ITEMS FROM {self.total_message_counter} MESSAGES '
                f'FROM {index + 1} CHATS')
            # </editor-fold>

    async def scan_messages(self, chat, start_date):
        """
        Iterates over messages in chat from older to newer starting from start_date.
        :param chat: chat for retrieving
        :param start_date: messages after this date will be retrieved. exclusive.
        """
        message_counter = 0
        chat_index = self.chats.index(chat)
        parsed_usernames_counter_before = len(self.parsed_items)
        async for message in self.client.iter_messages(chat, reverse=True, offset_date=start_date):
            if message.message is None or message.message == '':
                continue
            message_counter = message_counter + 1
            # <editor-fold desc="log">
            logger.debug(
                f'CHAT TITLE: {chat.title}({chat_index}/{self.chats_count}); ' +
                message.date.strftime('DATE: %d.%m.%Y %H:%M UTC+0; MSG: ') +
                format_message_to_print(message.message)
            )
            # </editor-fold>
            parsed_items_from_message = self.parse_message(message)
            self.parsed_items = self.parsed_items.union(parsed_items_from_message)
        # <editor-fold desc="log">
        if message_counter == 0:
            logger.warning(f'CHAT "{chat.title}" HAS NO TEXT MESSAGES SINCE {start_date}')
        # </editor-fold>
        parsed_usernames_counter_after = len(self.parsed_items)
        # <editor-fold desc="log">
        logger.info(f'PARSED {parsed_usernames_counter_after - parsed_usernames_counter_before} UNIQUE ITEMS '
                    f'FROM {message_counter} MESSAGES FROM "{chat.title}" CHAT')
        # </editor-fold>
        self.total_message_counter = self.total_message_counter + message_counter

    async def get_chats_info(self):
        """
        Iterates over user chats and adds id of each chat to self.chat_ids
        :return:
        """
        async for dialog in self.client.iter_dialogs():
            if isinstance(dialog.entity, Chat) or isinstance(dialog.entity, Channel) \
                    or isinstance(dialog.entity, ChatEmpty):
                self.chat_ids.add(dialog.entity.id)

    @abstractmethod
    def parse_message(self, message: Message):
        """
        Resolves item(s) from message.
        Override is necessary.
        Returned value will be added to self.parsed_items set.
        So returned value must be hashable.
        :param message:
        :return parsed_items: some collection(!) of parsed_items
        """
        pass

    def launch(self, anon_path: str, api_id: int, api_hash: str, proxy_config: dict[str, str] | None) -> None:
        """
        Launch async parse() function in new_event_loop()
        :param anon_path: path to .session file
        :param api_id: api_id for account with same session_id that was passed to __init__
        :param api_hash: api_hash for account with same session_id that was passed to __init__
        :param proxy_config:
        """
        if LOGGER_LEVEL != 'OFF':
            self.add_logger()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.parse(anon_path, api_id, api_hash, proxy_config))
        loop.close()

    def get_tg_chats_to_update(self) -> list[Chat]:
        """
        Filter self.tg_chats_to_parse that require db update
        :return: list with objects type of TgChatsToParse with attribute update_required == True
        """
        tg_chats_to_update = []
        for tg_chat in self.tg_chats_to_parse:
            if tg_chat.update_required:
                tg_chats_to_update.append(tg_chat)
        return tg_chats_to_update

    @dataclass
    class AbstractResult(ABC):
        """
        Wrapper for parser results
        """
        tg_chats_to_update: list

        @abstractmethod
        def merge_with(self, another_parser_result) -> None:
            self.tg_chats_to_update.extend(another_parser_result.tg_chats_to_update)

    @abstractmethod
    def get_parser_results(self) -> AbstractResult:
        pass
