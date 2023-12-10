import re
from datetime import datetime
from multiprocessing import current_process
import requests
from bs4 import PageElement
from loguru import logger
from telethon.tl.types import MessageEntityTextUrl
from config import *
from src.utils.wb_utils import get_sku_from_url, get_sku_from_text, wb_link_pattern, wb_sku_pattern, non_wb_links


def format_message_to_print(message: str) -> str:
    """
    removes newlines, hashtags, multiple whitespaces and trims the first 200 characters for pretty print
    :param message: str non formatted string
    :return: str
    """
    message_to_print = message.replace('\n', ' ')
    message_to_print = re.sub(r'#\S+', ' ', message_to_print)[0:200]
    message_to_print = re.sub(r'\s+', ' ', message_to_print)
    return message_to_print


def resolve_redirection_link(link: str) -> int | None:
    """
    resolves sku from link
    :param link: link to resolve
    :return: sku retrieved from link
    """
    logger.debug(f'RESOLVING {link}')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36'
    }
    if not link.startswith('http'):
        link = f'http://{link}'
    try:
        response = requests.get(link, timeout=15, headers=headers)
    except requests.exceptions as e:   # pragma: no cover
        logger.warning(f'ERROR {e} ON URL: {link}')
        return None
    sku = get_sku_from_url(response.url)
    if sku is not None:
        return int(sku)
    if len(response.history) != 0:
        sku = get_sku_from_url(response.history[0].url)
    if sku is not None:
        return int(sku)
    return get_sku_from_text(response.text)


def divide_into_chunks(input_list: list, chunks: int):
    """
    divides input list of objects into several lists
    :param input_list: list to divide
    :param chunks:
    :return: generator
    """
    for i in range(0, chunks):
        yield input_list[i::chunks]


def split_joined_non_joined_chats(tg_chats: list, sessions_count: int) -> (list, dict):
    """
    Splits given chat list into: list with non-joined chats, dict with joined chats list per session_id
    :param tg_chats:
    :param sessions_count:
    :return:
    """
    joined_tg_chats = dict()
    for i in range(sessions_count):
        joined_tg_chats[i+1] = []
    non_joined_tg_chats = []
    for chat in tg_chats:
        if chat.session_id is None:
            non_joined_tg_chats.append(chat)
        else:
            joined_tg_chats[chat.session_id].append(chat)
    return non_joined_tg_chats, joined_tg_chats


def add_log_to_file_for_process(class_name: str) -> None:
    """
    adds logger for process to file with path WORKDIR/LOG_FILES_FOLDER/class_name/dd.mm.YYYY_HH.MM/log.txt
    :param class_name: name of program to log
    """
    if LOGGER_LEVEL != 'OFF' and LOG_FILES_FOLDER is not None:
        current_process_name = current_process().name
        pid = current_process().pid
        output_log_file = f'{ROOT_DIR}/{LOG_FILES_FOLDER}/{class_name}/' \
                          f'{datetime.now().strftime("%d.%m.%Y_%H.%M")}/{current_process_name}/log.txt'
        logger.debug(f'ADDING LOGGER TO {output_log_file}')
        logger.add(output_log_file, format=PROCESS_LOGGER_FORMAT, level=LOGGER_LEVEL,
                   filter=lambda record: record['process'].id == pid)


class LinkSkuResolver:

    def __init__(self):
        self.skus: set[int] = set()
        self.resolved_links: set[str] = set()

    def get_skus_from_tgstat_post(self, post: PageElement) -> set[int]:
        """
        searches for links in text of post, iterates over hyperlinks in post and resolves skus from links
        :param post: tgstat post web element
        :return: set of skus
        """
        self.get_skus_from_tgstat_post_hyperlinks(post)
        post_text = post.find_next('div', {'class': 'post-text'})
        self.get_skus_from_text(post_text.text)
        return self.skus

    def get_skus_from_tgstat_post_hyperlinks(self, post: PageElement) -> set[int]:
        """
        searches for skus in hyperlinks in post page element
        :param post: post element from tgstat channel html response
        :return: set of skus
        """
        logger.debug('LOOKING FOR SKU IN HYPERLINKS')
        post_text = post.find_next('div', {'class': 'post-text'})
        if post_text is None:
            return self.skus
        for hyperlink in post_text.find_all('a'):
            link = hyperlink['href']
            # <editor-fold desc="log">
            logger.debug(f'INNER TEXT: {hyperlink.text}; LINK: {link}')
            # </editor-fold>
            self.resolve_link(link)
        return self.skus

    def get_skus_from_telethon_message(self, message) -> set[int]:
        """
        searches for links in text of message, iterates over hyperlinks in message and resolves skus from links
        :param message: entity
        :return: set of skus
        """
        self.get_skus_from_message_hyperlinks(message)
        self.get_skus_from_text(message.message)
        return self.skus

    def get_skus_from_message_hyperlinks(self, message) -> set[int]:
        """
        iterates over hyperlinks entities in message and resolves skus from links
        :param message:
        :return: set of skus, side effect: adds sku to self.skus
        """
        for url_entity, inner_text in message.get_entities_text(MessageEntityTextUrl):
            # <editor-fold desc="log">
            logger.debug(f'INNER TEXT: {inner_text}; LINK: {url_entity}')
            # </editor-fold>
            link = url_entity.url
            self.resolve_link(link)
        return self.skus

    def get_skus_from_text(self, text: str) -> set[int]:
        """
        searches for links in text and resolves skus from links
        :param text: any text
        :return: set of skus, side effect: adds sku to self.skus
        """
        url_pattern = re.compile(r'https?://\S+')
        urls = url_pattern.findall(text)
        for url in urls:
            self.resolve_link(url)
        return self.skus

    def resolve_link(self, link: str) -> None:
        """
        adds resolved sku from link to self.skus
        :param link: any url link
        """
        if link == '#' or link in self.resolved_links or link.startswith(non_wb_links):
            return
        match = wb_link_pattern.findall(link)
        if len(match) != 0:
            sku = int(wb_sku_pattern.findall(link)[0])
        else:
            sku = resolve_redirection_link(link)
        if sku is not None:
            self.skus.add(sku)
        self.resolved_links.add(link)
