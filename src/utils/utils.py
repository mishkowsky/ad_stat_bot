import re
import requests
from loguru import logger
from requests.exceptions import InvalidURL, MissingSchema, InvalidSchema, ConnectTimeout
from src.utils.wb_utils import get_sku_from_url, get_sku_from_text


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
    except Exception as e:  # requests.exceptions as e:
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
