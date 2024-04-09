import os
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

TEST_LOGGER_LEVEL = 'OFF'
LOGGER_LEVEL = 'DEBUG'
LOG_FILES_FOLDER = 'logs'
THREAD_LOGGER_FORMAT = '{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {thread.name} | ' \
                       '{name}:{function}:{line} - {message}{exception}'
PROCESS_LOGGER_FORMAT = '{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {process.name} | ' \
                        '{name}:{function}:{line} - {message}{exception}'

SESSIONS_FILE_PATH = 'sessions'

API_IDS = os.getenv('API_IDS')
API_HASHES = os.getenv('API_HASHES')
SESSION_COUNT = len(API_IDS)
