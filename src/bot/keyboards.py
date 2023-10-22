
from aiogram import types

check_sku_button = types.InlineKeyboardButton(text="Проверить артикул", callback_data='check_sku')
check_brand_button = types.InlineKeyboardButton(text="Проверить бренд", callback_data='check_brand')

main_menu_text = 'Главное меню'
main_menu_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
    [check_sku_button],
    [check_brand_button]
])

back_button = types.InlineKeyboardButton(text="Назад", callback_data='back_to_main_menu')
back_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[back_button]])

back_to_main_menu_button = types.InlineKeyboardButton(text="Главное меню", callback_data='send_main_menu')
back_to_main_menu_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[back_to_main_menu_button]])