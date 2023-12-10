import sys
from datetime import datetime
from multiprocessing import Process
from loguru import logger
from sqlalchemy import select
from config import LOGGER_LEVEL, PROCESS_LOGGER_FORMAT
from src.dao.db_config import get_db
from src.dao.mentions_db import MentionsDatabase, ChatContentType, Chat
from src.dao.mentions_db import Proxy
from src.parsers.tgstat.chat import ChannelParser
from src.utils import divide_into_chunks, add_log_to_file_for_process


def launch_parser(chats: list[Chat], proxy: dict[str, str] | None) -> None:
    """
    launch one parser
    :param chats: chats to parse
    :param proxy: proxy dict for requests library
    """
    if LOGGER_LEVEL == 'OFF':
        logger.remove()

    database = MentionsDatabase(next(get_db()))
    start_date = datetime.min
    cp = ChannelParser(start_date=start_date, database=database, proxy=proxy)

    cp.process_chats(chats)


def launch_many_parsers() -> None:
    """
    launches separate parsers in multiple processes, waits until parsing is done
    """
    logger.remove()
    if LOGGER_LEVEL != 'OFF':
        logger.add(sys.stdout, format=PROCESS_LOGGER_FORMAT, level=LOGGER_LEVEL)  # pragma: no cover
    add_log_to_file_for_process('ChannelParserLauncher')

    database = MentionsDatabase(next(get_db()))
    logger.debug(database.session.get_bind().url)
    chats = database.get_chats_by_content_type(ChatContentType.wb_items_ads)
    proxies = database.session.execute(select(Proxy)).scalars().all()
    logger.debug(f'GOT {len(proxies)} FROM DB')
    processes = []
    proxies_list = [proxy.get_http_dict() for proxy in proxies]
    chats_chunks = list(divide_into_chunks(chats, len(proxies_list)))

    for (proxy, chats_chunk) in zip(proxies_list, chats_chunks):
        p = Process(target=launch_parser, args=[chats_chunk, proxy])
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
    logger.info('ALL PROCESSES ARE DONE')


if __name__ == '__main__':  # pragma: no cover
    launch_many_parsers()
