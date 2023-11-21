from loguru import logger
from opentele.tl import TelegramClient
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetFullChatRequest
from telethon.tl.types import Chat, Channel, ChatEmpty, Updates
from telethon.utils import parse_username
import requests
from bs4 import BeautifulSoup
import re

from src.dao.mentions_db import ChatContentType


async def get_list_of_chat_ids(client: TelegramClient) -> set[int]:
    """
    Get IDs of conversations that are not conversations with other users, i.e. chats, channels
    :param client: active session of telegram client
    :return: set with ids
    """
    chat_ids = set()
    async for dialog in client.iter_dialogs():
        if isinstance(dialog.entity, Chat) or isinstance(dialog.entity, Channel) \
                or isinstance(dialog.entity, ChatEmpty):
            chat_ids.add(dialog.entity.id)
    return chat_ids


async def send_join_requests(client, tg_chats, session_id):
    joined_chats = []
    for tg_chat in tg_chats:
        link = tg_chat.link
        logger.debug(f'LINK {link}')
        hash_to_join, is_invite_link = parse_username(link)
        if is_invite_link:
            request = ImportChatInviteRequest(hash_to_join)
        else:
            request = JoinChannelRequest(link)
        chat = await send_join_request(client, request, link)
        if chat is not None:
            chat_full_info = None
            if tg_chat.followers is None and tg_chat.chat_content == ChatContentType.wb_items_ads:
                if isinstance(chat, Channel):
                    chat_full_info = await client(GetFullChannelRequest(chat.id))
                    tg_chat.followers = chat_full_info.full_chat.participants_count
                elif isinstance(chat, Chat):
                    chat_full_info = await client(GetFullChatRequest(chat.id))
                    tg_chat.followers = len(chat_full_info.users)
                if chat_full_info is None:
                    logger.warning(f'COULD NOT RESOLVE FULL INFO FOR {chat.title} WITH LINK {link}')

            tg_chat.title = chat.title
            tg_chat.tg_id = str(chat.id)

            tg_chat.session_id = session_id
            tg_chat.update_required = True
            joined_chats.append(chat)
    return joined_chats


async def send_join_request(client, request, invite_link):
    joined_chat = None
    try:
        logger.debug(f'SENDING JOIN REQUEST USING {invite_link}')
        result = await make_request(client, request)
        logger.debug('GOT RESULT')
        joined_chat = get_chat_from_result(result)
        if isinstance(joined_chat, Chat) or isinstance(joined_chat, Channel):
            logger.debug(f'RETRIEVED CHAT {joined_chat.title} FROM {invite_link}')
    except UserAlreadyParticipantError:
        joined_chat = await client.get_entity(invite_link)
        # <editor-fold desc="log">
        logger.debug(f'CHAT {joined_chat.title} WAS ALREADY JOINED BEFORE')
        # </editor-fold>
    except FloodWaitError:
        raise
    except Exception as e:
        # <editor-fold desc="log">
        logger.warning(f'SOME ERROR OCCURRED WHILE PROCESSING LINK {invite_link}: {e}')
        # </editor-fold>
    return joined_chat


def get_chat_from_result(result):
    joined_chat = None
    if isinstance(result, Updates):
        for chat_from_result in result.chats:
            if chat_from_result.title != 'Unsupported Chat' and chat_from_result.title != 'Unsupported Channel':
                joined_chat = chat_from_result
        if joined_chat is not None:
            if len(result.updates) > 0:
                logger.debug(f'CHAT {joined_chat.title} WAS JOINED')
            else:
                logger.debug(f'CHAT {joined_chat.title} WAS ALREADY JOINED BEFORE')
        else:
            logger.warning(f"COULDN'T GET JOINED CHAT FROM RESULT FOR LINK ")
    return joined_chat


async def make_request(client: TelegramClient, request):
    result = await client(request)
    return result


def get_chat_info_by_link(chat_link: str) -> (str, str):
    try:
        res = requests.get(f'https://{chat_link}')
    except ConnectionError:
        return get_chat_info_by_link(chat_link)
    soup = BeautifulSoup(res.text, 'html.parser')
    try:
        title = soup.find('div', {'class': 'tgme_page_title'}).find('span').text
        followers_text = soup.find('div', {'class': 'tgme_page_extra'}).text.replace(' ', '')
    except AttributeError:
        return None, None
    match_result = re.compile(r'^\d+').match(followers_text)
    if match_result:
        return title, match_result[0]
    else:
        return None, None


def refactor_tg_url(url: str) -> str:
    """
    from telegram.me/some_link makes t.me/some_link
    :param url: str link
    """
    return f't{url[8:]}' if url.startswith('telegram') else url
