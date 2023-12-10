import os
from datetime import datetime
from typing import Type
from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified
from loguru import logger
from src.bot.keyboards import *
from src.bot.keyboards import main_menu_keyboard, back_keyboard
from src.bot.user_states import UserStates
from src.dao.db_config import get_db
from src.dao.mentions_db import MentionsDatabase, Chat, Post, SkuPerPost
from src.dao.users_db import User, Request, RequestTypesEnum, RequestPlatformsEnum, UserDatabase

TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

session = next(get_db())

db = MentionsDatabase(session=session)
user_db = UserDatabase(session)

CURRENT_PAGE_KEY = 'current_page'
PAGES_COUNT_KEY = 'pages_count'
REPLY_KEY = 'reply'
LAST_MESSAGE_WITH_KEYBOARD_ID = 'last_message_with_keyboard_id'


@dp.message_handler(commands="start", state="*")
async def handle_start(message: types.Message, state: FSMContext) -> None:
    message = await bot.send_message(message.from_user.id,
                                     text='👋 Привет!\n\n🔥 Я могу показать, в каких Telegram каналах твои '
                                          'конкуренты закупают рекламу!\n\n⬇️ Нажимай на кнопку ниже и '
                                          'вводи артикул! Я выведу все каналы, в которых этот '
                                          'артикул упоминался!',
                                     reply_markup=main_menu_keyboard, parse_mode='html', disable_web_page_preview=True)
    user_db.check_user(User(user_id=str(message.from_user.id), username=message.from_user.username,
                            first_name=message.from_user.first_name, last_name=message.from_user.last_name,
                            created_at=datetime.now()))
    await delete_keyboard_under_last_message(state, message.from_user.id)
    await state.finish()
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: message.message_id})


@dp.message_handler()
async def handle_plain_text(message: types.Message, state: FSMContext) -> None:
    await delete_keyboard_under_last_message(state, message.from_user.id)
    message = await bot.send_message(message.from_user.id, text='Пожалуйста, воспользуйтесь кнопками меню!',
                                     reply_markup=main_menu_keyboard, parse_mode='html', disable_web_page_preview=True)
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: message.message_id})


@dp.callback_query_handler(lambda call: call.data == "back_to_main_menu", state="*")
async def handle_back_to_main_menu(call: types.callback_query, state: FSMContext) -> None:
    await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id,
                                text=main_menu_text, reply_markup=main_menu_keyboard)
    await state.finish()
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: call.message.message_id})


async def set_new_state(state_to_finish: FSMContext, state_to_set) -> None:
    await state_to_finish.finish()
    await state_to_set.set()


check_sku_message = '✏️ <b>Отправь мне артикул</b>, по которому ты хочешь получить информацию.\n\n' \
                    'Я отправлю тебе список каналов, которые рекламировали данный товар!'


@dp.callback_query_handler(lambda call: call.data == "check_sku_res")
async def check_sku_call(call: types.callback_query, state: FSMContext) -> None:
    message = await bot.send_message(text=check_sku_message, chat_id=call.from_user.id,
                                     reply_markup=back_keyboard, parse_mode='html')
    await bot.edit_message_reply_markup(message_id=call.message.message_id,
                                        chat_id=call.from_user.id, reply_markup=None)
    await set_new_state(state, UserStates.EnterSKU)
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: message.message_id})


@dp.callback_query_handler(lambda call: call.data == "check_sku")
async def check_sku_call(call: types.callback_query, state: FSMContext) -> None:
    await bot.edit_message_text(text=check_sku_message, message_id=call.message.message_id, chat_id=call.from_user.id,
                                reply_markup=back_keyboard, parse_mode='html')
    await set_new_state(state, UserStates.EnterSKU)
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: call.message.message_id})


@dp.message_handler(state=UserStates.EnterSKU)
async def handle_entered_sku(message: types.Message, state: FSMContext) -> None:
    request = Request(user_id=message.from_user.id, request_type=RequestTypesEnum.sku,
                      request_platform=RequestPlatformsEnum.telegram,
                      request=message.text, created_at=datetime.now())
    user_db.add_new_user_request(request)
    sku = message.text.strip()
    if not sku.isdigit():
        await bot.send_message(message.from_user.id, text='Артикул должен быть числом.')
        await UserStates.EnterSKU.set()
        return
    await delete_keyboard_under_last_message(state, message.from_user.id)
    mentions = db.get_mentions_by_sku(int(sku))
    if not mentions:
        text = f'Артикул <i>{sku}</i> не упоминался ни в одном канале.'
    else:
        text = get_text_for_sku_response(sku, mentions)
    await state.finish()
    await send_page_result(message.from_user.id, split_message(text), 1, state)


def get_text_for_sku_response(sku: str, mentions: dict[Chat, dict[Post, set[SkuPerPost]]]) -> str:
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
            delimiter = ';' if post != sorted_posts[-1] else '.'
            text += f'\n<a href="t.me/c/{chat.tg_id}/{post.message_id}">Пост</a> от {post.date}{delimiter}'
        mentions_count += mentions_count_per_channel
    channel_count = len(mentions.keys())
    channel_ending = 'е' if channel_count == 1 else 'ах'
    times_ending = 'a' if mentions_count in range(2, 5) else ''
    text = f'<b>Артикул <a href="wb.ru/catalog/{sku}/detail.aspx">{sku}</a> упоминался <i>{mentions_count}</i> ' \
           f'раз{times_ending} в <i>{channel_count}</i> канал{channel_ending}:</b>{text}\n'
    return text


check_brand_message = '✏️ <b>Отправь мне наименование бренда</b>, по которому ты хочешь получить информацию \n\n' \
                      'Я отправлю тебе список блогеров, которые рекламировали данный бренд!'


@dp.callback_query_handler(lambda call: call.data == "check_brand_res")
async def check_brand_call(call: types.callback_query, state: FSMContext) -> None:
    message = await bot.send_message(text=check_brand_message, chat_id=call.from_user.id,
                                     reply_markup=back_keyboard, parse_mode='html')
    await bot.edit_message_reply_markup(message_id=call.message.message_id,
                                        chat_id=call.from_user.id, reply_markup=None)
    await set_new_state(state, UserStates.EnterBrand)
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: message.message_id})


@dp.callback_query_handler(lambda call: call.data == "check_brand")
async def check_brand_call(call: types.callback_query, state: FSMContext) -> None:
    await bot.edit_message_text(text=check_brand_message, message_id=call.message.message_id, chat_id=call.from_user.id,
                                reply_markup=back_keyboard, parse_mode='html')
    await set_new_state(state, UserStates.EnterBrand)
    await state.update_data({LAST_MESSAGE_WITH_KEYBOARD_ID: call.message.message_id})


@dp.message_handler(state=UserStates.EnterBrand)
async def handle_entered_brand(message: types.Message, state: FSMContext) -> None:
    request = Request(user_id=message.from_user.id, request_type=RequestTypesEnum.brand,
                      request_platform=RequestPlatformsEnum.telegram,
                      request=message.text, created_at=datetime.now())
    user_db.add_new_user_request(request)
    brand = message.text.strip()
    mentions = db.get_mentions_by_brand(brand.lower())
    if not mentions:
        text = f'Ни один артикул бренда <i>{brand}</i> не упоминался ни в одном канале.'
    else:
        text = get_text_for_brand_response(brand, mentions)
    await delete_keyboard_under_last_message(state, message.from_user.id)
    await state.finish()
    await send_page_result(message.from_user.id, split_message(text), 1, state)


def get_text_for_brand_response(brand: str, mentions: dict[Chat: dict[Post: set[SkuPerPost]]]) -> str:
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


def get_mentions_list_text_for_chat(chat: Chat, posts: dict[Post: set[Type[SkuPerPost]]]) -> (str, int):
    text = ''
    sorted_posts = list(posts.keys())
    sorted_posts.sort(key=lambda p: p.date)
    mentions_count = 0
    for post in sorted_posts:
        mentions = posts[post]
        mentions_count += len(mentions)
        text += f'\n<a href="t.me/c/{chat.tg_id}/{post.message_id}">Пост</a> от {post.date}:'
        if len(mentions) == 1:
            sku_code = list(mentions)[0].sku_code
            text += f'\nАртикул: <a href="wb.ru/catalog/{sku_code}/detail.aspx">{sku_code}</a>.'
        else:
            text += f'\nАртикулы: '
            i = -1
            for mention in mentions:
                i += 1
                sku_code = mention.sku_code
                delimiter = '; ' if i != len(mentions) - 1 else '.'
                text += f'<a href="wb.ru/catalog/{sku_code}/detail.aspx">{sku_code}</a>{delimiter}'
    mentions_ending = 'е' if mentions_count == 1 else 'й'
    mentions_ending = 'я' if mentions_count in range(2, 5) else mentions_ending
    text = f'\n<i>{mentions_count}</i> упоминани{mentions_ending} в канале ' \
           f'<a href="{chat.link}">"{chat.title}"</a>:{text}'
    return text, mentions_count


async def send_message_with_limit(user_id: int, text: str) -> None:
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
        keyboard = main_menu_keyboard_after_res if end == text_len - 1 else None
        await bot.send_message(user_id, text=text_to_send, reply_markup=keyboard, parse_mode='html',
                               disable_web_page_preview=True)
        sent_text_len = sent_text_len + end - start + 1
        start = end + 1


def split_message(text: str) -> list[str]:
    splitted_text: list[str] = []
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
        splitted_text.append(text_to_send)
        sent_text_len = sent_text_len + end - start + 1
        start = end + 1
    return splitted_text


async def send_page_result(user_id: int, splitted_text: list[str], page_to_send: int, state: FSMContext) -> None:
    pages_count = len(splitted_text)
    keyboard = get_pagination_keyboard(1, pages_count)
    message = await bot.send_message(user_id, text=splitted_text[page_to_send - 1], reply_markup=keyboard,
                                     parse_mode='html', disable_web_page_preview=True)
    await state.set_data({
        CURRENT_PAGE_KEY: 1,
        REPLY_KEY: splitted_text,
        LAST_MESSAGE_WITH_KEYBOARD_ID: message.message_id
    })


@dp.callback_query_handler(lambda call: call.data == 'go_to_next_page')
async def next_page_call(call: types.callback_query, state: FSMContext) -> None:
    data = await state.get_data()
    splitted_text = data[REPLY_KEY]
    pages_count = len(splitted_text)
    page = data[CURRENT_PAGE_KEY] + 1
    if page > pages_count:
        page = 1
    keyboard = get_pagination_keyboard(page, pages_count)
    await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id,
                                text=splitted_text[page - 1], reply_markup=keyboard, parse_mode='html',
                                disable_web_page_preview=True)
    await state.update_data({CURRENT_PAGE_KEY: page, LAST_MESSAGE_WITH_KEYBOARD_ID: call.message.message_id})


@dp.callback_query_handler(lambda call: call.data == 'go_to_prev_page')
async def prev_page_call(call: types.callback_query, state: FSMContext) -> None:
    data = await state.get_data()
    splitted_text = data[REPLY_KEY]
    pages_count = len(splitted_text)
    page = data[CURRENT_PAGE_KEY] - 1
    if page == 0:
        page = pages_count
    keyboard = get_pagination_keyboard(page, pages_count)
    await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id,
                                text=splitted_text[page - 1], reply_markup=keyboard, parse_mode='html',
                                disable_web_page_preview=True)
    await state.update_data({CURRENT_PAGE_KEY: page, LAST_MESSAGE_WITH_KEYBOARD_ID: call.message.message_id})


@dp.callback_query_handler(lambda call: call.data == 'none')
async def none_action(call: types.callback_query, _) -> None:
    await bot.answer_callback_query(call.id)


def find_index_of_nearest_newline(end_index, text):
    while end_index > 0 and text[end_index] != '\n':
        end_index = end_index - 1
    return end_index


async def delete_keyboard_under_last_message(state, chat_id):
    data = await state.get_data()
    try:
        last_message_id = data[LAST_MESSAGE_WITH_KEYBOARD_ID]
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=last_message_id, reply_markup=None)
    except KeyError:
        pass
    except MessageNotModified:
        pass


async def on_shutdown(d: Dispatcher):
    await d.bot.close_bot()
    logger.info('bot closed')


if __name__ == '__main__':
    logger.info('starting')
    executor.start_polling(dp, on_shutdown=on_shutdown)
