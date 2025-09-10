from aiogram.dispatcher.filters.state import State, StatesGroup


class AuthStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


class PromoStates(StatesGroup):
    waiting_promo = State()


class ParserStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_chats = State()
    waiting_keywords = State()
    waiting_exclude = State()
    waiting_name = State()


class EditParserStates(StatesGroup):
    waiting_name = State()
    waiting_chats = State()
    waiting_keywords = State()
    waiting_exclude = State()


class ExpandProStates(StatesGroup):
    waiting_chats = State()


class TopUpStates(StatesGroup):
    waiting_amount = State()


class PartnerTransferStates(StatesGroup):
    waiting_amount = State()
