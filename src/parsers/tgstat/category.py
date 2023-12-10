import requests
from bs4 import BeautifulSoup, ResultSet
from loguru import logger
from src.dao.mentions_db import Chat, ChatContentType
from src.parsers.telegram.chat import TgChatAdChatParser


class CategoryParser:

    def __init__(self):
        self.chat_limit = None
        self.first_page_request = None
        self.session = None
        self.chat_counter = 0
        self.parsed_chats = set()
        self.url = None

    def process_category(self, url: str, chat_limit: int) -> None:
        """
        gets chats/channels from html web pages like tgstat.ru/beauty
        :param url: url to category to parse with specified schema
        :param chat_limit: limit of chats to parse
        """
        self.url = url
        self.chat_limit = chat_limit
        headers_0 = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Connection': 'keep-alive'
        }

        self.session = requests.Session()
        self.first_page_request = self.session.get(url, headers=headers_0)

        soup = BeautifulSoup(self.first_page_request.content, features="html.parser")
        container = soup.find_all("div", {"class": "lm-list-container"})[0]

        hyperlinks = container.find_all('a', {'class': 'text-body'}, href=True)

        self.process_chat_hyperlinks(hyperlinks)

        more_button = soup.find('div', {'class': 'lm-button-container'})
        page_for_request = more_button.find_next('input', {'class': 'lm-page'})['value']
        offset_for_request = more_button.find_next('input', {'class': 'lm-offset'})['value']

        if self.chat_counter < self.chat_limit:  # and has_next
            self.send_requests(page_for_request, offset_for_request)
        self.chat_counter = 0

    def send_requests(self, page: str, offset: str) -> None:
        """
        send requests for more chats
        :param page: page from html web page
        :param offset: offset from html web page
        """
        headers = {
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': self.first_page_request.headers['Set-Cookie'],
            'Host': 'tgstat.ru',
            'Origin': 'https://tgstat.ru',
            'Referer': 'https://tgstat.ru/beauty',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'X-Requested-With': 'XMLHttpRequest'
        }
        data = {
            '_tgstat_csrk': '',
            'peer_type': 'channel',
            'sort_channel': 'members',
            'sort_chat': 'members',
            'page': str(page),
            'offset': str(offset)
        }

        result = self.session.get(f'{self.url}/items', headers=headers, data=data)

        json_response = result.json()
        soup = BeautifulSoup(json_response['html'], features="html.parser")
        has_next = json_response['hasMore']
        next_page = json_response['nextPage']
        next_offset = json_response['nextOffset']
        hyperlinks = soup.find_all('a', {'class': 'text-body'}, href=True)
        self.process_chat_hyperlinks(hyperlinks)
        if has_next and self.chat_counter < self.chat_limit:
            self.send_requests(next_page, next_offset)

    def process_chat_hyperlinks(self, hyperlinks: ResultSet) -> None:
        """
        parsers chats from hyperlinks
        :param hyperlinks: <a> elements that were found on page
        """
        for hyperlink in hyperlinks:
            if self.chat_counter >= self.chat_limit:
                return
            followers = int(hyperlink.find_next('b').text.replace(' ', ''))

            link = hyperlink['href'][len('https://tgstat.ru/channel/'):]
            title = hyperlink.find('div', {'class': 'font-16'}).text
            if link.startswith('@'):
                link = f't.me/{link[1:]}'
            else:
                link = f't.me/+{link}'
            # <editor-fold desc="log"> # pragma: no cover
            logger.info(f'#{self.chat_counter} title: {title}; link: {link}; followers: {followers}')
            # </editor-fold>

            chat = Chat(link=link, chat_content=ChatContentType.wb_items_ads,
                        title=title, followers=followers, update_required=True)

            self.chat_counter = self.chat_counter + 1
            self.parsed_chats.add(chat)

    def get_parsed_results(self) -> TgChatAdChatParser.Result:
        """
        getter for parser results in a TgChatAdChatParser.Result wrap for upload to db
        :return: TgChatAdChatParser.Result
        """
        return TgChatAdChatParser.Result([], self.parsed_chats)


if __name__ == '__main__':  # pragma: no cover
    cp = CategoryParser()
    cp.process_category('https://tgstat.ru/beauty', 5)
