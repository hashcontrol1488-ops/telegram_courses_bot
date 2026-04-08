from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


INTERESTS = {
    "income": "💰 Заработок",
    "marketing": "📈 Маркетинг",
    "programming": "💻 Программирование",
    "design": "🎨 Дизайн",
    "health": "🩺 Здоровье и спорт",
    "pickup": "🔥 Пикап, искусство соблазнения",
}


def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key, title in INTERESTS.items():
        kb.button(text=title, callback_data=f"interest:{key}")
    kb.button(text="🔍 Найти курс", callback_data="search:start")
    kb.button(text="📂 Избранное", callback_data="fav:list")
    kb.button(text="🗂 Мои ссылки", callback_data="links:list")
    kb.button(text="👥 Реферальная программа", callback_data="ref:info")
    kb.button(text="🧭 С чего начать?", callback_data="rec:start")
    kb.button(text="🪙 Обменять 5 баллов на +1 курс", callback_data="points:buy")
    kb.adjust(2, 2, 1, 1, 1, 1, 1)
    return kb.as_markup()


def categories_kb(items: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for idx, title in items:
        kb.button(text=title, callback_data=f"cat:{idx}")
    kb.button(text="🔙 Назад", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def courses_kb(category_idx: int, course_titles: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for course_idx, title in course_titles:
        kb.button(text=title[:64], callback_data=f"course:{category_idx}:{course_idx}")
    kb.button(text="🔙 Назад", callback_data="back:cats")
    kb.button(text="🏠 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def course_actions_kb(category_idx: int, course_idx: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Добавить в избранное", callback_data=f"fav:add:{category_idx}:{course_idx}")
    kb.button(text="🔙 Назад", callback_data=f"cat:{category_idx}")
    kb.button(text="🏠 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def search_results_kb(items: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for idx, title in items:
        kb.button(text=title[:64], callback_data=f"search:pick:{idx}")
    kb.button(text="🔍 Новый поиск", callback_data="search:start")
    kb.button(text="🏠 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def favorites_kb(items: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for idx, title in items:
        kb.button(text=title[:64], callback_data=f"fav:pick:{idx}")
    kb.button(text="🏠 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def menu_only_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()
