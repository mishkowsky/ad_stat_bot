from aiogram import types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

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

back_button = types.InlineKeyboardButton(text='Назад', callback_data='back_to_main_menu')
back_keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[back_button]])


def get_pagination_keyboard(current_page: int, pages_count: int) -> InlineKeyboardMarkup:
    if pages_count > 1:

        previous_page_button = InlineKeyboardButton(text='←', callback_data='go_to_prev_page')
        current_page_button = InlineKeyboardButton(text=f'{current_page}/{pages_count}', callback_data='none')
        next_page_button = InlineKeyboardButton(text='→', callback_data='go_to_next_page')

        return InlineKeyboardMarkup(inline_keyboard=[
            [previous_page_button, current_page_button, next_page_button],
            [check_sku_button_res],
            [check_brand_button_res]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [check_sku_button_res],
            [check_brand_button_res]
        ])
