import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

THREAD_LOGGER_FORMAT = '{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {thread.name} | ' \
                       '{name}:{function}:{line} - {message}{exception}'