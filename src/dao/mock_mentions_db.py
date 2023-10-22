import dataclasses
from datetime import datetime

from sqlalchemy.orm import Session


@dataclasses.dataclass(frozen=True)
class Chat:
    tg_id: str
    title: str
    link: str


@dataclasses.dataclass(frozen=True)
class Post:
    date: datetime
    message_id: int


@dataclasses.dataclass(frozen=True)
class Mention:
    sku_code: int


class MentionsDatabase:

    def __init__(self, session: Session):
        self.mock_chat = Chat(title='some_chat', link='t.me/some_chat1', tg_id='2046566930')

    def get_mentions_by_sku(self, sku: int) -> dict[Chat: dict[Post: list[Mention]]]:
        result = {}
        if sku == 1345:
            result[self.mock_chat] = {Post(datetime(2022, 12, 28, 23, 55, 59, 342380), 5): [Mention(1345)]}
        elif sku == 1344:
            result[self.mock_chat] = {Post(datetime(2023, 1, 28, 23, 55, 59, 342380), 4): [Mention(1344)]}
        return result

    def get_mentions_by_brand(self, brand: str) -> dict[Chat: dict[Post: list[Mention]]]:
        return {
            self.mock_chat:
                {Post(datetime(2022, 12, 28, 23, 55, 59, 342380), 5): [Mention(1345)],
                 Post(datetime(2023, 1, 28, 23, 55, 59, 342380), 4): [Mention(1344)]}
        }