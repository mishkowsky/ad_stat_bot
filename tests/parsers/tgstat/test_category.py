import os
from pathlib import Path
from src.dao.mentions_db import Chat
from src.parsers.tgstat.category import CategoryParser


def test_process_category(requests_mock):
    """
    testing process_category() function of CategoryParser class
    :param requests_mock: fixture to mock requests library
    """

    current_path = Path(os.path.dirname(os.path.realpath(__file__)))
    site_content_path = current_path / 'resources' / 'tgstat_category_site'
    main_page = (site_content_path / 'main_page').read_bytes()

    extra_items_1 = (site_content_path / 'extra_items_1').read_text(encoding='utf-8')
    extra_items_2 = (site_content_path / 'extra_items_2').read_text(encoding='utf-8')

    requests_mock.get('https://tgstat.ru/beauty', content=main_page, headers={'Set-Cookie': ''})

    requests_mock.get('https://tgstat.ru/beauty/items',
                      [{'json': {'html': extra_items_1, 'hasMore': True, 'nextPage': 2, 'nextOffset': 0}},
                       {'json': {'html': extra_items_2, 'hasMore': False, 'nextPage': -1, 'nextOffset': 0}}])

    cp = CategoryParser()
    cp.process_category('https://tgstat.ru/beauty', 6)

    parsed_chats = cp.get_parsed_results()

    expected_chats = {
        Chat(title='Нашла на Wildberries', link='t.me/+QBJtwNE7IpI3NTgy', followers=630670),
        Chat(title='Находки WB для девушек', link='t.me/podborchik_wb', followers=503574),
        Chat(title='Маникюр | Ногти', link='t.me/+ZkaaiAghVXxjNzRi', followers=495109),
        Chat(title='MyChannel', link='t.me/somelink', followers=73622),
        Chat(title='ШМОТКИ С ВБ 🎀🤍НОВОГОДНЯЯ РАСПРОДАЖА 🔥', link='t.me/+eCABAk458q43OGZi', followers=468513),
        Chat(title='Поговорим о маникюре 💅🏻', link='t.me/+K3_SxwIGn0wzZTli', followers=73690),
    }

    assert parsed_chats.parsed_tg_chats == expected_chats
