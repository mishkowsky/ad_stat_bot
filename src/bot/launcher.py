from aiogram import executor
from src.bot import session, engine, dp


async def on_shutdown():
    session.close()
    engine.dispose()
    print('session and engine closed')


if __name__ == "__main__":
    print('starting')
    executor.start_polling(dp, on_shutdown=on_shutdown)