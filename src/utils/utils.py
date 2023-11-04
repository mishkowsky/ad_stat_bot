import re


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