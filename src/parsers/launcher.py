import asyncio
import sys
import threading
from datetime import datetime
from typing import Type
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from config import SESSIONS_FILE_PATH, SESSION_COUNT, API_IDS, API_HASHES, ROOT_DIR, THREAD_LOGGER_FORMAT
from src.dao.proxy import Proxy
from src.dao.db_config import DBConfigInstance, DB_CONFIG
from src.dao.mentions_db import MentionsDatabase
from src.parsers.tg_abstract import AbstractTgChatParser
from src.utils import divide_into_chunks
from src.utils import split_joined_non_joined_chats

from src.parsers.tg_sku import TgWbItemsAdChatParser


class ParserLauncher:

    def __init__(self, db_config: DBConfigInstance):
        thread_id = threading.get_native_id()
        output_log_file = f'{ROOT_DIR}/logs/{self.__class__.__name__}/' \
                          f'{datetime.now().strftime("%d.%m.%Y_%H.%M")}/log.txt'
        logger.debug(f'ADDING LOGGER TO {output_log_file}')
        logger.add(output_log_file, format=THREAD_LOGGER_FORMAT, filter=lambda record: record['thread'].id == thread_id)
        self.engine = create_engine(db_config.DB_URI, echo=False)
        self.session = Session(bind=self.engine, autoflush=False)
        self.database = MentionsDatabase(self.session)

    async def launch_all_parsers(self):
        self.launch_tg_parsers(TgWbItemsAdChatParser, self.database.upload_wb_items_ad_parser_results)
        self.session.close()
        self.engine.dispose()

    def launch_tg_parsers(self, tg_parser_class: Type[AbstractTgChatParser], upload_parser_result_function):
        """
        Launches min(SESSION_COUNT, len(proxies)) threads with parsers, collects result and loads to database
        :param tg_parser_class: class of parser to launch
        :param upload_parser_result_function: database function to load object type of SomeParser.Result
        """

        tg_chats = self.database.get_chats_by_content_type(tg_parser_class.chats_type)

        non_joined_tg_chats, session_id_chats = split_joined_non_joined_chats(tg_chats, SESSION_COUNT)

        parsers = []
        threads = []
        proxies = self.database.session.query(Proxy).all()
        threads_count = min(SESSION_COUNT, len(proxies))
        logger.info(f'STARTING {threads_count} THREADS')
        chunks = list(divide_into_chunks(non_joined_tg_chats, threads_count))
        start_date = datetime.now().replace(hour=0)
        for i, chunk in enumerate(chunks):
            session_id = i + 1
            session_id_chats[session_id].extend(chunk)
            session_file_path = rf'{ROOT_DIR}/{SESSIONS_FILE_PATH}/{session_id}/anon'
            if len(session_id_chats[session_id]) == 0:
                continue
            parser = tg_parser_class(session_id, session_id_chats[session_id], start_date)
            parsers.append(parser)
            t = threading.Thread(name=f'Thread-{session_id}', target=parser.launch,
                                 args=[session_file_path, API_IDS[session_id], API_HASHES[session_id],
                                       proxies[i].get_http_config_dict()])
            threads.append(t)
            t.start()

        logger.debug('WAITING FOR THREADS')
        for t in threads:
            logger.debug(f'WAITING FOR {t.name}')
            t.join()

        logger.debug('ALL THREADS ARE HERE')

        parser_results = None
        total_scanned_messages = 0
        total_processed_chats = 0
        for parser in parsers:
            total_processed_chats = total_processed_chats + len(parser.processed_chats_id)
            total_scanned_messages = total_scanned_messages + parser.total_message_counter
            if parser_results is None:
                parser_results = parser.get_parser_results()
            else:
                parser_results.merge_with(parser.get_parser_results())

        logger.info(f'TOTALLY PARSED {parser_results.get_parsed_items_count()} '
                    f'FROM {total_scanned_messages} MESSAGES '
                    f'FROM {total_processed_chats} CHATS')

        logger.info('UPLOADING RESULTS TO DB')
        upload_parser_result_function(parser_results)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stdout, format=THREAD_LOGGER_FORMAT)
    p_l = ParserLauncher(DB_CONFIG)
    asyncio.run(p_l.launch_all_parsers())
