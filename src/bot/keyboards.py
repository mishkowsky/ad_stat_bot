
from aiogram import types

check_sku_button = types.InlineKeyboardButton(text="Проверить артикул", callback_data='check_sku')
check_sku_button_res = types.InlineKeyboardButton(text="Проверить артикул", callback_data='check_sku_res')

check_brand_button = types.InlineKeyboardButton(text="Проверить бренд", callback_data='check_brand')
check_brand_button_res = types.InlineKeyboardButton(text="Проверить бренд", callback_data='check_brand_res')

main_menu_text = 'Главное меню'
main_menu_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
    [check_sku_button],
    [check_brand_button]
])

main_menu_keyboard_after_res = types.InlineKeyboardMarkup(inline_keyboard=[
    [check_sku_button_res],
    [check_brand_button_res]
])

back_button = types.InlineKeyboardButton(text="Назад", callback_data='back_to_main_menu')
back_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[back_button]])
