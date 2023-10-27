import os
from datetime import datetime
from loguru import logger
from src.dao.users_db import User, Request, RequestTypesEnum, RequestPlatformsEnum
from aiogram.dispatcher import FSMContext
from src.dao.db_config import DB_CONFIG
from src.bot.user_states import UserStates
from src.bot.keyboards import *
from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.bot.keyboards import main_menu_keyboard, back_keyboard
from src.dao.mock_mentions_db import Chat, Post, Mention, MentionsDatabase

TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

engine = create_engine(DB_CONFIG.DB_URI, echo=False)
session = Session(bind=engine)

db = MentionsDatabase(session=session)


@dp.message_handler(commands="start", state="*")
async def handle_start(message: types.Message, state: FSMContext):
    await state.finish()
    await bot.send_message(message.from_user.id, text='👋 Привет! Я могу показать, какие товары рекламируют '
                                                      'твои любимые блогеры в Telegram, а также у каких блогеров брали'
                                                      ' рекламу твои топовые конкуренты.',
                           reply_markup=main_menu_keyboard, parse_mode='html', disable_web_page_preview=True)
    user_from_db = session.query(User).filter(User.user_id == message.from_user.id).first()
    if user_from_db is None:
        now = datetime.now()
        user = User(user_id=message.from_user.id, username=message.from_user.username,
                    first_name=message.from_user.first_name, last_name=message.from_user.last_name,
                    created_at=now)
        session.add(user)
        session.commit()


@dp.message_handler()
async def handle_plain_text(message: types.Message):
    await bot.send_message(message.from_user.id, text='Пожалуйста, воспользуйтесь кнопками меню!',
                           reply_markup=main_menu_keyboard, parse_mode='html', disable_web_page_preview=True)


@dp.callback_query_handler(lambda call: call.data == "back_to_main_menu", state="*")
async def handle_back_to_main_menu(call: types.callback_query, state: FSMContext):
    await state.finish()
    await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id,
                                text=main_menu_text, reply_markup=main_menu_keyboard)


@dp.callback_query_handler(lambda call: call.data == "check_sku", state='*')
async def check_sku_call(call: types.callback_query, state: FSMContext):
    await state.finish()
    await bot.edit_message_text(text='✏️ <b>Отправь мне артикул</b>, по которому ты хочешь получить информацию.\n\n'
                                     'Я отправлю тебе список каналов, которые рекламировали '
                                     'данный товар!',
                                message_id=call.message.message_id, chat_id=call.from_user.id,
                                reply_markup=back_keyboard, parse_mode='html')
    await UserStates.EnterSKU.set()


@dp.message_handler(state=UserStates.EnterSKU)
async def handle_entered_sku(message: types.Message, state: FSMContext):
    await state.finish()
    request = Request(user_id=message.from_user.id, request_type=RequestTypesEnum.sku,
                      request_platform=RequestPlatformsEnum.telegram,
                      request=message.text, created_at=datetime.now())
    session.add(request)
    session.commit()
    sku = message.text.strip()
    if not sku.isdigit():
        await bot.send_message(message.from_user.id, text='Артикул должен быть числом.')
        await UserStates.EnterSKU.set()
        return
    mentions = db.get_mentions_by_sku(int(sku))
    if not mentions:
        text = f'Артикул <i>{sku}</i> не упоминался ни в одном канале.'
    else:
        text = get_text_for_sku_response(sku, mentions)
    await send_message_with_limit(message.from_user.id, text)


def get_text_for_sku_response(sku, mentions):
    mentions_count = 0
    text = ''
    for chat, posts in mentions.items():
        sorted_posts = list(posts.keys())
        sorted_posts.sort(key=lambda p: p.date)
        mentions_count_per_channel = len(posts)
        mentions_ending = 'е' if mentions_count_per_channel == 1 else 'й'
        mentions_ending = 'я' if mentions_count_per_channel in range(2, 5) else mentions_ending

        text += f'\n\n<i>{mentions_count_per_channel}</i> упоминани{mentions_ending} ' \
                f'в канале <a href="{chat.link}">"{chat.title}"</a>:'
        for post in sorted_posts:
            mention = posts[post][0]
            if len(posts[post]) > 1:
                logger.warning('AAAAA')
            delimiter = ';' if post != sorted_posts[-1] else '.'
            text += f'\n<a href="t.me/c/{chat.tg_id}/{post.message_id}">Пост</a> от {post.date}{delimiter}'
        mentions_count += mentions_count_per_channel
    channel_count = len(mentions.keys())
    channel_ending = 'е' if channel_count == 1 else 'ах'
    times_ending = 'a' if mentions_count in range(2, 5) else ''
    text = f'<b>Артикул <a href="wb.ru/catalog/{sku}/detail.aspx">{sku}</a> упоминался <i>{mentions_count}</i> ' \
           f'раз{times_ending} в <i>{channel_count}</i> канал{channel_ending}:</b>{text}\n'
    return text


@dp.callback_query_handler(lambda call: call.data == "check_brand", state='*')
async def check_brand_call(call: types.callback_query, state: FSMContext):
    await state.finish()
    await bot.edit_message_text(text='✏️ <b>Отправь мне наименование бренда</b>, по которому ты хочешь получить '
                                     'информацию \n\nЯ отправлю тебе список блогеров, которые '
                                     'рекламировали данный бренд!',
                                message_id=call.message.message_id, chat_id=call.from_user.id,
                                reply_markup=back_keyboard, parse_mode='html')
    await UserStates.EnterBrand.set()


@dp.message_handler(state=UserStates.EnterBrand)
async def handle_entered_brand(message: types.Message, state: FSMContext):
    await state.finish()
    request = Request(user_id=message.from_user.id, request_type=RequestTypesEnum.brand,
                      request_platform=RequestPlatformsEnum.telegram,
                      request=message.text, created_at=datetime.now())
    session.add(request)
    session.commit()
    brand = message.text.strip()
    mentions = db.get_mentions_by_brand(brand.lower())
    if not mentions:
        text = f'Ни один артикул бренда <i>{brand}</i> не упоминался ни в одном канале.'
    else:
        text = get_text_for_brand_response(brand, mentions)
    await send_message_with_limit(message.from_user.id, text)
    # await send_main_menu_message(message.from_user.id)


def get_text_for_brand_response(brand: str, mentions: dict[Chat: dict[Post: list[Mention]]]):
    mentions_count = 0
    text = ''
    for chat, posts in mentions.items():
        text_for_chat, mentions_count_for_chat = get_mentions_list_text_for_chat(chat, posts)
        mentions_count += mentions_count_for_chat
        text += f'\n{text_for_chat}'
    channel_count = len(mentions.keys())
    channel_ending = 'е' if channel_count == 1 else 'ах'
    times_ending = 'a' if mentions_count in range(2, 5) else ''
    text = f'<b>Артикулы бренда {brand} упоминались <i>{mentions_count}</i> раз{times_ending} ' \
           f'в <i>{channel_count}</i> канал{channel_ending}:</b>{text}'
    return text


def get_mentions_list_text_for_chat(chat: Chat, posts: dict[Post: list[Mention]]) -> tuple[str, int]:
    """
    Generates text response
    :param chat: chat where Mentions where found
    :param posts: dict with chats Posts as keys and list of Mentions in each Post as values
    :return: generated text message, total mentions count
    """
    text = ''
    sorted_posts = list(posts.keys())
    sorted_posts.sort(key=lambda p: p.date)
    mentions_count = 0
    for post in sorted_posts:
        mentions = posts[post]
        mentions_count += len(mentions)
        text += f'\n<a href="t.me/c/{chat.tg_id}/{post.message_id}">Пост</a> от {post.date}:'
        if len(mentions) == 1:
            sku_code = mentions[0].sku_code
            text += f'\nАртикул: <a href="wb.ru/catalog/{sku_code}/detail.aspx">{sku_code}</a>.'
        else:
            text += f'\nАртикулы: '
            for mention in mentions:
                sku_code = mention.sku_code
                delimiter = '; ' if mention != mentions[-1] else '.'
                text += f'<a href="wb.ru/catalog/{sku_code}/detail.aspx">{sku_code}</a>{delimiter}'
    mentions_ending = 'е' if mentions_count == 1 else 'й'
    mentions_ending = 'я' if mentions_count in range(2, 5) else mentions_ending
    text = f'\n<i>{mentions_count}</i> упоминани{mentions_ending} в канале <a href="{chat.link}">"{chat.title}"</a>:{text}'
    return text, mentions_count


def get_blogger_list_message(instagram_usernames_with_date_and_followers):
    text = ''
    for i, (instagram_username, date, followers) in enumerate(instagram_usernames_with_date_and_followers):
        text = f'{text}{i + 1}. '
        text = text + f'{date.strftime("%d.%m.%Y")} ' \
                      f'<a href="instagram.com/{instagram_username}">{instagram_username}</a> ' \
                      f'({followers} подписчиков)\n'
    return text


async def send_message_with_limit(user_id: int, text: str):
    message_limit = 4096
    text_len = len(text)
    sent_text_len = 0
    start = 0
    while sent_text_len < text_len:
        if (text_len - 1) - start < message_limit:
            end = text_len - 1
        else:
            end = start + (message_limit - 1)
            newline_index = find_index_of_nearest_newline(end, text)
            if newline_index > start:
                end = newline_index
        text_to_send = text[start:end + 1]
        keyboard = main_menu_keyboard if end == text_len - 1 else None
        await bot.send_message(user_id, text=text_to_send, reply_markup=keyboard, parse_mode='html',
                               disable_web_page_preview=True)
        sent_text_len = sent_text_len + end - start + 1
        start = end + 1


def find_index_of_nearest_newline(end_index: int, text: str) -> int:
    while end_index > 0 and text[end_index] != '\n':
        end_index = end_index - 1
    return end_index
