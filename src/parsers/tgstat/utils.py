import re
from datetime import datetime
from bs4 import PageElement
from src.dao.mentions_db import Chat


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
