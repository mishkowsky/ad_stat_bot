import re
import threading
from datetime import datetime

from loguru import logger

from config import ROOT_DIR, THREAD_LOGGER_FORMAT


def format_message_to_print(message: str) -> str:
    """
    removes newlines, hashtags, multiple whitespaces and trims the first 200 characters for pretty print
    :param message: str non formatted string
    :return: str
    """
    message_to_print = message.replace('\n', ' ')
    message_to_print = re.sub(r'#\S+', ' ', message_to_print)[0:200]
    message_to_print = re.sub(r'\s+', ' ', message_to_print)
    return message_to_print


def add_logger(self):
    thread_id = threading.get_native_id()
    output_log_file = f'{ROOT_DIR}/logs/{self.__class__.__name__}/{self.session_id}/' \
                      f'{datetime.now().strftime("%d.%m.%Y_%H.%M")}/log.txt'
    logger.debug(f'ADDING LOGGER TO {output_log_file}')
    logger.add(output_log_file, format=THREAD_LOGGER_FORMAT, filter=lambda record: record['thread'].id == thread_id)