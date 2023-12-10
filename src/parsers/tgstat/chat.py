import re
import sys
import time
import warnings
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup, ResultSet, PageElement
from loguru import logger
from requests import JSONDecodeError
from requests.exceptions import ProxyError
from sqlalchemy import exc as sa_exc
from config import PROCESS_LOGGER_FORMAT, LOGGER_LEVEL
from src.dao.db_config import get_db
from src.dao.mentions_db import SkuPerPost, Sku, Post, MentionsDatabase, Proxy, ChatContentType, Chat
from src.parsers.tgstat.utils import get_tgstat_url, get_value_from_icon_element, \
    get_post_date_from_string, get_post_id, get_tgstat_csrk_from_cookie
from src.utils import format_message_to_print, add_log_to_file_for_process, LinkSkuResolver
from src.utils.wb_utils import wb_sku_pattern, wb_size_pattern, wb_link_pattern


class ChannelParser:

    def __init__(self, start_date: datetime, database: MentionsDatabase, proxy: dict[str, str]):
        """
        :param start_date: will parse posts later this date
        :param database: connection with db
        :param proxy: proxy for requests library
        """

        logger.remove()
        if LOGGER_LEVEL != 'OFF':
            logger.add(sys.stdout, format=PROCESS_LOGGER_FORMAT, level=LOGGER_LEVEL)
            add_log_to_file_for_process(self.__class__.__name__)

        logger.debug('INIT SUCCESFULL')

        self.database = database
        self.parser_start_time = datetime.now()
        self.session = requests.Session()
        self.session.proxies = proxy

        self.start_date = start_date
        self.chat = None
        self.chats = None
        self.parsed_sku_db_instances = dict()

        self.earliest_post_date = datetime.now()
        self.recent_parsed_post_tg_id = None
        self.previous_recent_parsed_post_tg_id = None

        self.tgstat_csrk = None
        self.url = None
        self.headers = None
        self.cookie = None
        self.tg_link = None
        self.cookies = None

        self.parsed_posts_count_from_channel = None
        self.processed_posts_count_from_channel = None
        self.parsed_mentions_count_from_chat = None
        self.last_info_log_time = datetime.now()

        self.total_parsed_posts_count = 0
        self.total_processed_posts_count = 0
        self.total_processed_chat_count = 0
        self.total_parsed_mentions_count = 0

        self.setup_connection()

    def setup_connection(self) -> None:
        """
        establish connection with https://tgstat.ru
        """

        headers_0 = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Connection': 'keep-alive'
        }

        main_page_response = None
        while main_page_response is None:
            try:
                main_page_response = self.session.get('https://tgstat.ru', timeout=10, headers=headers_0)
            except requests.exceptions.Timeout:  # pragma: no cover
                logger.warning("RESPONSE WASN'T RECEIVED IN 10 SEC, RETRYING IN 15 SEC")
                time.sleep(15)
        logger.debug(main_page_response)
        cookie = main_page_response.headers['Set-Cookie']
        self.cookie = cookie
        self.tgstat_csrk = get_tgstat_csrk_from_cookie(cookie)
        self.headers = headers_0

    def process_chats(self, chats: list[Chat]) -> None:
        """
        processes list of chats
        :param chats: list of chats to parse
        """
        self.chats = chats
        self.total_processed_chat_count = 0

        for chat in chats:
            try:
                self.total_processed_chat_count = self.total_processed_chat_count + 1
                # <editor-fold desc="log info">
                logger.info(f'PARSING #{self.total_processed_chat_count}/{len(chats)} '
                            f'CHANNEL {chat.title} WITH URL: {chat.link}')  # pragma: no cover
                # </editor-fold>
                self.process_chat(chat)
                # <editor-fold desc="log stat"> # pragma: no cover
                logger.info(f'DONE PARSING CHANNEL {chat.title} WITH URL: {chat.link}')   # pragma: no cover
                logger.info(f'PARSED AND LOADED TO DB {self.parsed_posts_count_from_channel} POSTS'
                            f' WITH {self.parsed_mentions_count_from_chat} '
                            f'MENTIONS IN CURRENT CHANNEL')  # pragma: no cover
                logger.info(f'PROCESSED {self.processed_posts_count_from_channel} '
                            f'POSTS IN CURRENT CHANNEL')  # pragma: no cover
                # </editor-fold>
            except Exception as e:
                logger.error(f'ERROR OCCURRED {e}')
                raise
        logger.info('ALL CHANNELS WERE PARSED')
        logger.info(f'TOTAL PARSED AND LOADED TO DB {self.total_parsed_posts_count} POSTS '
                    f'WITH {self.total_parsed_mentions_count} MENTIONS')
        logger.info(f'TOTAL {self.total_processed_posts_count} POSTS PROCESSED')
        logger.info(f'ELAPSED TIME: {datetime.now() - self.parser_start_time}')
        self.session.close()

    def process_chat(self, chat: Chat) -> None:
        """
        parses tgstat chat, uploads results to db
        :param chat: chat to parse
        """

        self.parsed_posts_count_from_channel = 0
        self.processed_posts_count_from_channel = 0
        self.parsed_mentions_count_from_chat = 0
        self.chat = chat

        if self.chat.recent_parsed_post_tg_id is None:
            self.previous_recent_parsed_post_tg_id = -1
        else:
            self.previous_recent_parsed_post_tg_id = self.chat.recent_parsed_post_tg_id

        self.url = get_tgstat_url(self.chat)

        logger.info(f'TGSTAT URL: {self.url}')

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Connection': 'keep-alive'
        }

        first_page_request = self.make_request_with_timeout(self.session.get, self.url)
        logger.debug(f'RECEIVED {first_page_request}')

        if first_page_request.status_code == 404:  # pragma: no cover
            logger.warning(f'{self.url} NOT FOUND')
            return

        try:
            value = first_page_request.json()
            if 'status' in value.keys():  # pragma: no cover
                logger.warning(f'{self.url} NOT FOUND')
                return
        except Exception as e:  # pragma: no cover
            logger.error(f'ERROR OCCURRED {e}')
            pass

        soup = BeautifulSoup(first_page_request.content, features="html.parser")

        tg_hash_pattern = re.compile(r'@?[A-Za-z_0-9\-]+$')
        match = tg_hash_pattern.findall(first_page_request.url)
        link = match[0]
        if link.startswith('@'):
            link = f't.me/{link[1:]}'
        else:
            link = f't.me/+{link}'

        if self.chat.link != link:
            self.chat.link = link
            self.chat.update_required = True

        chat_followers = soup.find('h2', {'class': 'mb-1 text-dark'}).text.replace(' ', '')
        if chat_followers != self.chat.followers:
            self.chat.followers = int(chat_followers)
            self.chat.update_required = True

        posts = soup.find_all('div', {'class': 'post-container'})
        self.process_posts(posts)

        if self.chat.update_required:
            logger.debug(f'UPDATING TGCHAT {self.chat.link}; RPPID: {self.chat.recent_parsed_post_tg_id}')
            self.database.update_tg_chat(self.chat)
            logger.debug(f'CHAT WAS UPDATED')

        more_button = soup.find('div', {'class': 'lm-button-container'})
        if more_button is not None:
            page_for_request = more_button.find_next('input', {'class': 'lm-page'})['value']
            offset_for_request = more_button.find_next('input', {'class': 'lm-offset'})['value']

            self.headers['X-Requested-With'] = 'XMLHttpRequest'
            self.send_posts_request(page_for_request, offset_for_request)

    def send_posts_request(self, page: str, offset: str) -> None:
        """
        request for more posts
        :param page: page parameter from 'load more' button on html web page
        :param offset: offset parameter from 'load more' button on html web page
        """

        form_data = {
            '_tgstat_csrk': self.tgstat_csrk,
            'date': '0',
            'q': '',
            'hideDeleted': ['0', '1'],
            'hideForwards': '0',
            'page': page,
            'offset': offset
        }

        json_response = None
        r_counter = 0
        while json_response is None and r_counter < 10:
            if r_counter > 0:
                time.sleep(30)  # pragma: no cover
            result = self.make_request_with_timeout(self.session.post, f'{self.url}/posts-last', form_data)
            r_counter += 1
            logger.debug(f'RECEIVED {result}')
            if result.status_code == 403:
                logger.warning('RETURNED STATUS CODE 403 SLEEP FOR 30 SECS')  # pragma: no cover
            try:
                json_response = result.json()
            except JSONDecodeError:  # pragma: no cover
                logger.warning('CANT DECODE JSON DUE TO ...')
                json_response = None
            except Exception as e:  # pragma: no cover
                logger.exception(f"UNEXPECTED ERROR {e} WHILE MAKING POSTS REQUEST")
        if json_response is None:
            return  # pragma: no cover

        soup = BeautifulSoup(json_response['html'], features="html.parser")

        posts = soup.find_all('div', {'class': 'post-container'})
        logger.debug(f'RECEIVED RESPONSE HAS {len(posts)} POSTS')
        self.process_posts(posts)

        has_next = json_response['hasMore']
        next_page = json_response['nextPage']
        next_offset = json_response['nextOffset']

        if self.chat.update_required:
            # <editor-fold desc="log">
            logger.debug(f'UPDATING TGCHAT {self.chat}; '
                         f'RPPID: {self.chat.recent_parsed_post_tg_id}')   # pragma: no cover
            # </editor-fold>
            self.database.update_tg_chat(self.chat)

        if has_next and self.earliest_post_date > self.start_date \
                and (self.recent_parsed_post_tg_id is None
                     or self.recent_parsed_post_tg_id > self.previous_recent_parsed_post_tg_id):
            self.send_posts_request(next_page, next_offset)

    def process_posts(self, posts: ResultSet) -> None:
        """
        processes posts from html response, uploads results to db
        :param posts:
        :return: datetime of last parsed post ()
        """
        parsed_posts = set()
        self.parsed_sku_db_instances = dict()
        for post in posts:
            self.total_processed_posts_count += 1
            self.processed_posts_count_from_channel += 1

            post_entity = self.process_post(post)

            if post_entity is not None and post_entity.date > self.start_date:
                parsed_posts.add(post_entity)

        self.total_parsed_posts_count = self.total_parsed_posts_count + len(parsed_posts)

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=sa_exc.SAWarning)
            new_posts_count, new_mentions_count = \
                self.database.upload_tg_posts_to_db(parsed_posts, self.parsed_sku_db_instances)
            self.database.session.commit()

        self.parsed_posts_count_from_channel += new_posts_count
        self.parsed_mentions_count_from_chat += new_mentions_count
        self.total_parsed_mentions_count += new_mentions_count

    def process_post(self, post: PageElement) -> Post | None:
        """
        parses post from tgstat channel
        :param post: post element
        :return: post database instance
        """

        date_str = post.find_next('small').text
        post_date = get_post_date_from_string(date_str)
        post_id = get_post_id(post)

        # <editor-fold desc="log big stat">
        if datetime.now() - self.last_info_log_time > timedelta(minutes=3):  # pragma: no cover
            logger.info(f'PARSING #{self.total_processed_chat_count}/{len(self.chats)} CHANNEL {self.chat.title} '
                        f'WITH URL: {self.url}')  # pragma: no cover
            logger.info(f'CURRENT POST: id: {post_id}; date: {date_str}; '
                        f'post_link: {self.url}/{post_id}')  # pragma: no cover
            logger.info(f'PARSED AND LOADED TO DB {self.parsed_posts_count_from_channel} POSTS'
                        f' WITH {self.parsed_mentions_count_from_chat} MENTIONS IN CURRENT CHANNEL')  # pragma: no cover
            logger.info(f'PROCESSED {self.processed_posts_count_from_channel} '
                        f'POSTS IN CURRENT CHANNEL')  # pragma: no cover
            logger.info(f'TOTAL PARSED AND LOADED TO DB {self.total_parsed_posts_count} POSTS '
                        f'WITH {self.total_parsed_mentions_count} MENTIONS')  # pragma: no cover
            logger.info(f'TOTAL {self.total_processed_posts_count} POSTS PROCESSED')  # pragma: no cover
            logger.info(f'ELAPSED TIME: {datetime.now() - self.parser_start_time}')  # pragma: no cover
            self.last_info_log_time = datetime.now()  # pragma: no cover
        # </editor-fold>

        if post_date < self.earliest_post_date:
            self.earliest_post_date = post_date

        post_entity = None
        # <editor-fold desc="log debug">
        if datetime.now() - post_date < timedelta(hours=12):
            logger.debug(f'SKIPPING POST DATED FROM "{date_str}" '
                         f'because {datetime.now()} - {post_date} = {datetime.now() - post_date}')  # pragma: no cover
        # </editor-fold>

        self.recent_parsed_post_tg_id = post_id
        if post_date > self.start_date and datetime.now() - post_date > timedelta(hours=12) \
                and post_id > self.previous_recent_parsed_post_tg_id:

            if self.chat.recent_parsed_post_tg_id is None \
                    or post_id > self.chat.recent_parsed_post_tg_id:
                self.chat.recent_parsed_post_tg_id = post_id
                self.chat.update_required = True

            post_text_element = post.find_next('div', {'class': 'post-text'})
            if post_text_element is None:
                logger.debug('POST DOESNT HAVE TEXT => SKIPPING')
                return None
            post_text = post_text_element.text

            if post_text_element.parent.has_attr('class'):
                if 'post-body-forwarded' in post_text_element.parent['class']:
                    # <editor-fold desc="log">
                    logger.debug(f'SKIPPING POST AS IT IS REPLY')  # pragma: no cover
                    # </editor-fold>
                    return None

            # <editor-fold desc="log debug">
            logger.debug(f'PROCESSING POST DATED FROM {date_str}: '
                         f'{format_message_to_print(post_text)}')  # pragma: no cover
            # </editor-fold>
            skus = LinkSkuResolver().get_skus_from_tgstat_post(post)
            wb_links = wb_link_pattern.findall(post_text)
            for wb_link in wb_links:
                skus.add(int(wb_sku_pattern.findall(wb_link)[0]))
            skus = skus.difference([int(s) for s in wb_size_pattern.findall(post_text)])

            if len(skus) == 0:
                return None

            logger.debug(f'FOUND {len(skus)}')

            last_row_item_element = post.find_next('i', {'class': 'uil-eye'})
            last_row = last_row_item_element.parent.parent

            reactions_count_icon = last_row.find('i', {'class': 'uil-thumbs-up'})
            reactions_count = get_value_from_icon_element(reactions_count_icon)

            shared_count_icon = last_row.find('i', {'class': 'uil-share-alt'})
            shared_count = get_value_from_icon_element(shared_count_icon)

            view_count_icon = last_row.find('i', {'class': 'uil-eye'})
            views_count = get_value_from_icon_element(view_count_icon)

            replies_count_icon = last_row.find('i', {'class': 'uil-corner-up-right'})
            replies_count = get_value_from_icon_element(replies_count_icon)

            comments_count_icon = last_row.find('i', {'class': 'uil-comments-alt'})
            comments_count = get_value_from_icon_element(comments_count_icon)

            er = (replies_count + reactions_count + comments_count) / self.chat.followers * 100
            err = (replies_count + reactions_count + comments_count) / views_count * 100

            # <editor-fold desc="log debug">
            logger.debug(f'POST ID: {post_id}; VIEWS: {views_count}; '
                         f'SHARED: {shared_count}; REPLIED: {replies_count}; '
                         f'COMMENTS: {comments_count}; REACTIONS: {reactions_count}')  # pragma: no cover
            # </editor-fold>

            post_entity = Post(message_id=str(post_id), chat_id=str(self.chat.id),
                               views_count=views_count, replies_count=replies_count,
                               shared_count=shared_count, er=float(er), err=float(err),
                               reactions_count=reactions_count, comments_count=comments_count,
                               date=post_date)
            for sku in skus:
                if sku not in self.parsed_sku_db_instances.keys():
                    sku_db_instance = Sku(sku_code=sku)
                    self.parsed_sku_db_instances[sku] = sku_db_instance
                else:
                    sku_db_instance = self.parsed_sku_db_instances[sku]
                sku_db_instance.sku_per_post.append(SkuPerPost(sku_code=sku, post=post_entity))

        return post_entity

    def make_request_with_timeout(self, method, url, form_data=None):
        counter = 0
        while counter < 10:
            try:
                return method(url, timeout=10, headers=self.headers, data=form_data)
            except ProxyError:  # pragma: no cover
                # <editor-fold desc="log">
                logger.warning('PROXY DISCONNECT')  # pragma: no cover
                # </editor-fold>
                time.sleep(30)
                counter += 1
            except requests.exceptions.Timeout:  # pragma: no cover
                # <editor-fold desc="log">
                logger.warning('RESPONSE WASN\'T RECEIVED IN 10 SEC, RETRYING IN 15 SEC')  # pragma: no cover
                # </editor-fold>
                time.sleep(15)
                counter += 1
            except Exception as e:  # pragma: no cover
                # <editor-fold desc="log">
                logger.warning(f'UNEXPECTED ERROR {e} RETRYING IN 15 SEC')  # pragma: no cover
                # </editor-fold>
                time.sleep(15)
                counter += 1


if __name__ == '__main__':  # pragma: no cover
    database_ = MentionsDatabase(next(get_db()))
    proxy_ = database_.session.query(Proxy).first()[0]
    start_date_ = datetime.min
    cp = ChannelParser(start_date=start_date_, database=database_, proxy=proxy_.get_http_dict())

    chats_ = database_.get_chats_by_content_type(ChatContentType.wb_items_ads)

    cp.process_chats(chats_)
