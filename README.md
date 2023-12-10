
# ad_stat_bot
Telegram бот для получения упоминаний артикулов в Telegram каналах.
## Структура проекта
### src/bot
Бот, обработчик сообщений, написан на aiogram 2.
### src/dao
ORM-классы и методы доступа к данным, написаны с помощью SQLAlchemy.
### src/parsers/telegam
Парсеры Telegram каналов, написаны с использованием Telethon и opentele.
### src/parsers/tgstat
Парсеры сайта tgstat.ru, написаны на requests и BeatifulSoup.
