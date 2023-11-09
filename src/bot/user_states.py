from aiogram.dispatcher.filters.state import StatesGroup, State


class UserStates(StatesGroup):
    EnterSKU = State()
    EnterBrand = State()
