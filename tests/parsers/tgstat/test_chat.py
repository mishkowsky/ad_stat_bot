import os
from datetime import datetime
from pathlib import Path
from freezegun import freeze_time
from sqlalchemy import select
from src.dao.mentions_db import ChatContentType, Chat, MentionsDatabase
from src.parsers.tgstat.chat import ChannelParser


class TestChannelParser:
    
    @freeze_time("2023-12-12")
    def test_process_chats(self, requests_mock, db_session):

        current_path = Path(os.path.dirname(os.path.realpath(__file__)))

        requests_mock.get('https://tgstat.ru', content=b'', headers={'Set-Cookie': ''})

        chat_site_content_path = current_path / 'resources' / 'tgstat_chat_site'
        chat_page = (chat_site_content_path / 'main_page').read_bytes()

        requests_mock.get('https://tgstat.ru/channel/@testingpublicchannel', content=chat_page, headers={'Set-Cookie': ''})

        requests_mock.get('https://card.wb.ru/cards/detail?appType=1&curr=rub&dest=-1257786&regions=68,64,83,4,38,80,33,'
                          '70,82,86,75,30,69,1,48,22,66,31,40,71&spp=33&nm=123456',
                          json={'data': {'products': [{'id': 123456, 'brand': 'brand_name', 'brandId': 1}]}})

        session = db_session()
        chat = Chat(link='t.me/testingpublicchannel', recent_parsed_post_tg_id=6, chat_content=ChatContentType.wb_items_ads)
        session.add(chat)
        session.commit()

        database = MentionsDatabase(session)
        start_date = datetime.min
        cp = ChannelParser(start_date=start_date, database=database, proxy=None)

        cp.process_chats([chat])

        updated_chat = session.execute(select(Chat).where(Chat.link == 't.me/testingpublicchannel')).scalar()

        assert updated_chat.recent_parsed_post_tg_id == 8
