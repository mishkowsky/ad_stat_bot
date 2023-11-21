import re
from datetime import datetime
from bs4 import PageElement
from loguru import logger
from src.dao.mentions_db import Chat
from src.utils import resolve_redirection_link
from src.utils.wb_utils import wb_sku_pattern


def get_post_date_from_string(date_str: str) -> datetime:
    """
    converts str tgstat date into datetime object
    :param date_str: date from tgstat post
    :return: datetime
    """
    date_with_year_format = '%d %b %Y, %H:%M'
    date_format = '%d %b, %H:%M'
    if len(date_str) > 14:
        datetime_object = datetime.strptime(date_str, date_with_year_format)
    else:
        datetime_object = datetime.strptime(date_str, date_format)
        datetime_object = datetime_object.replace(year=datetime.now().year)
    return datetime_object


def get_post_id(post: PageElement) -> int:
    """
    resolves post id from post element of tgstat channel
    :param post: post from tgstat channel
    :return: id of post
    """
    view_count_icon = post.find_next('i', {'class': 'uil-eye'})
    view_button = view_count_icon.parent
    err_stat_link = f'http://tgstat.ru{view_button["href"]}'
    post_id_pattern = re.compile(r'(?<=/)\d+(?=/stat)')
    match_res = post_id_pattern.findall(err_stat_link)
    post_id = match_res[-1]
    return int(post_id)


def get_tgstat_url(chat_from_db: Chat) -> str:
    """
    converts tg link into tgstat link
    :param chat_from_db: instance of chat with tg url
    :return: tgstat url
    """
    if chat_from_db.link.startswith('t.me/+'):
        offset = len('t.me/+')
        return f'https://tgstat.ru/channel/{chat_from_db.link[offset:]}'
    else:
        offset = len('t.me/')
        return f'https://tgstat.ru/channel/@{chat_from_db.link[offset:]}'


def get_skus_from_post_hyperlinks(post: PageElement) -> set[int]:
    """
    searches for skus in hyperlinks in post page element
    :param post: post element from tgstat channel html response
    :return: set of skus
    """
    logger.debug('LOOKING FOR SKU IN HYPERLINKS')
    skus = set()
    processed_links = set()
    post_text = post.find_next('div', {'class': 'post-text'})
    if post_text is None:
        return skus
    for hyperlink in post_text.find_all('a'):
        link = hyperlink['href']
        if link == '#' or link in processed_links \
                or link.startswith(('https://tgstat.ru/', 'https://ttttt.me/',
                                    'https://t.me/', 'https://market.yandex.ru/')):
            continue
        # <editor-fold desc="log">
        logger.debug(f'INNER TEXT: {hyperlink.text}; LINK: {link}')
        # </editor-fold>
        resolved_sku = None
        if link.startswith('https://www.wildberries.') or link.startswith('https://wildberries'):
            match = wb_sku_pattern.findall(link)
            if match:
                resolved_sku = match[0]
        else:
            resolved_sku = resolve_redirection_link(link)
        processed_links.add(link)
        if resolved_sku is not None:
            skus = skus.union([int(resolved_sku)])

    return skus


def get_value_from_icon_element(icon_element: PageElement) -> int:
    """
    resolves value from icon_element
    :param icon_element: icon element under the tgstat post
    :return: value associated with icon
    """
    number_format = re.compile(r'\d+((\.\d)?k)?')
    if icon_element is not None:
        count_text = icon_element.parent.text.replace('\n', '')
        count = number_format.match(count_text)[0]
        if count[-1] == 'k':
            count = count.replace('k', '')
            count = float(count) * 1000
    else:
        count = 0
    return int(count)


def get_tgstat_csrk_from_cookie(cookie: str) -> str:
    """
    parsers cookie from key-value pair
    :param cookie: cookie key-value str pair
    :return: cookie value
    """
    tgstat_csrk_pattern = re.compile(r'(?<=_tgstat_csrk=).+?(?=%)')
    match = tgstat_csrk_pattern.findall(cookie)
    if match:
        return match[0]
