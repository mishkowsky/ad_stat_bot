import asyncio
import os
from aiogram import Bot
from loguru import logger
from src.bot.keyboards import main_menu_keyboard
from src.dao.db_config import get_db
from src.dao.mentions_db import MentionsDatabase
from src.dao.users_db import UserDatabase, User

TOKEN = os.getenv("BOT_TOKEN")


async def spam(user_ids: list[int]) -> None:
    """
    sends message to all users with ids from user_ids
    :param user_ids: list of ids to send to
    """
    bot = Bot(token=TOKEN)
    for user_id in user_ids:
        try:
            await bot.send_message(user_id, text='Главное меню', reply_markup=main_menu_keyboard,
                                   parse_mode='html', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f'ERROR: {e} ON {user_id}')
            logger.exception('')


if __name__ == "__main__":
    logger.info('starting')

    db = MentionsDatabase(session=next(get_db()))
    user_db = UserDatabase(next(get_db()))
    user_ids_ = [int(user_id[0]) for user_id in user_db.session.query(User.user_id)]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(spam(user_ids_))
