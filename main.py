from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable, Dict

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, User
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from catalog import Catalog, Course, parse_courses_file
from config import get_config
from db import Database
from keyboards import (
    INTERESTS,
    categories_kb,
    course_actions_kb,
    courses_kb,
    favorites_kb,
    main_menu_kb,
    menu_only_kb,
    search_results_kb,
)
from states import SearchStates


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


INTEREST_TO_KEYWORDS: dict[str, tuple[str, ...]] = {
    "income": ("заработ", "бизнес", "покер", "казино", "арбитраж", "схем"),
    "marketing": ("маркет", "seo", "smm", "социальн", "tg", "блог"),
    "programming": ("программ", "администр", "python", "разработ"),
    "design": ("дизайн", "график", "figma", "арт", "моушн"),
    "health": ("здоров", "спорт", "тренир", "мед", "питани", "похуд", "фитнес", "биохакинг"),
    "pickup": ("пикап", "пикам", "искусство соблазнения", "соблазн", "знакомств", "отношен", "дейтинг"),
}


def category_match_score(category: str, interest_key: str) -> int:
    name = category.casefold()
    return sum(1 for kw in INTEREST_TO_KEYWORDS.get(interest_key, ()) if kw in name)


def pick_categories_by_interest(catalog: Catalog, interest_key: str) -> list[tuple[int, str]]:
    scored: list[tuple[int, int, str]] = []
    for idx, category in enumerate(catalog.categories):
        score = category_match_score(category, interest_key)
        if score > 0:
            scored.append((score, idx, category))
    if not scored:
        return [(idx, name) for idx, name in enumerate(catalog.categories[:12])]
    scored.sort(reverse=True)
    return [(idx, category) for _, idx, category in scored[:12]]


async def show_main_menu(target: Message | CallbackQuery, text: str = "") -> None:
    text = text or "Привет! 👋 Я помогу тебе получить бесплатные курсы. Выбери, что тебе интересно 👇"
    if isinstance(target, Message):
        await target.answer(text, reply_markup=main_menu_kb())
    else:
        await target.message.edit_text(text, reply_markup=main_menu_kb())


def is_admin(
    user: User | None,
    admin_usernames: tuple[str, ...],
    admin_user_ids: tuple[int, ...],
) -> bool:
    if not user:
        return False
    if user.id in admin_user_ids:
        return True
    if not user.username:
        return False
    return user.username.casefold() in admin_usernames


async def main() -> None:
    config = get_config()
    catalog_data = parse_courses_file(config.courses_file)
    catalog = Catalog(catalog_data)
    db = Database(config.db_path)
    await db.init()

    bot = Bot(token=config.bot_token)
    dp = Dispatcher()

    class MessageStatsMiddleware(BaseMiddleware):
        def __init__(self, database: Database) -> None:
            self.database = database

        async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message,
            data: Dict[str, Any],
        ) -> Any:
            await self.database.increment_message_counter()
            return await handler(event, data)

    dp.message.middleware(MessageStatsMiddleware(db))

    async def ensure_and_reward(msg: Message) -> tuple[int, str, bool]:
        await db.ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
        points, level, rewarded = await db.daily_visit_reward(msg.from_user.id)
        return points, level, rewarded

    @dp.message(Command("start"))
    async def cmd_start(message: Message, command: CommandObject) -> None:
        points, level, rewarded = await ensure_and_reward(message)

        if command.args and command.args.startswith("ref_"):
            try:
                inviter_id = int(command.args.replace("ref_", "", 1))
                await db.add_referral(inviter_id, message.from_user.id)
            except ValueError:
                pass

        extra = ""
        if rewarded:
            extra = f"\n\n🪙 +1 балл за ежедневный заход. Твой уровень: {level} ({points} баллов)"
        await show_main_menu(message, f"Привет! 👋 Я помогу тебе получить бесплатные курсы. Выбери, что тебе интересно 👇{extra}")

    @dp.message(Command("daily"))
    async def cmd_daily(message: Message) -> None:
        await ensure_and_reward(message)
        course = catalog.random_course()
        if not course:
            await message.answer("Пока нет курсов в базе.")
            return
        await message.answer(
            f"🎁 Курс дня:\n\n{course.title}\n\nЭтот курс не учитывается в дневном лимите."
        )
        await message.answer(course.url)

    @dp.message(Command("stats"))
    async def cmd_stats(message: Message) -> None:
        if not is_admin(message.from_user, config.admin_usernames, config.admin_user_ids):
            await message.answer("⛔ Команда доступна только администратору.")
            return
        users_count, messages_count = await db.get_admin_stats()
        await message.answer(
            "📊 Статистика бота\n\n"
            f"Пользователей: {users_count}\n"
            f"Сообщений: {messages_count}"
        )

    @dp.message(Command("stats_full"))
    async def cmd_stats_full(message: Message) -> None:
        if not is_admin(message.from_user, config.admin_usernames, config.admin_user_ids):
            await message.answer("⛔ Команда доступна только администратору.")
            return
        stats = await db.get_admin_stats_full()
        await message.answer(
            "📈 Расширенная статистика\n\n"
            f"Пользователи всего: {stats['users_total']}\n"
            f"Новых за 24ч: {stats['users_new_24h']}\n"
            f"Активных за 24ч: {stats['users_active_24h']}\n\n"
            f"Сообщений всего: {stats['messages_total']}\n\n"
            f"Скачиваний всего: {stats['downloads_total']}\n"
            f"Скачиваний за 24ч: {stats['downloads_24h']}\n"
            f"Скачиваний за 7д: {stats['downloads_7d']}\n\n"
            f"Рефералов всего: {stats['referrals_total']}\n"
            f"Избранных всего: {stats['favorites_total']}"
        )

    @dp.message(Command("myid"))
    async def cmd_myid(message: Message) -> None:
        username_line = f"Username: @{message.from_user.username}" if message.from_user.username else "Username: (none)"
        await message.answer(
            f"Your Telegram ID: {message.from_user.id}\n{username_line}"
        )

    @dp.message(Command("ref"))
    async def cmd_ref(message: Message) -> None:
        await ensure_and_reward(message)
        me = await bot.get_me()
        ref_count = await db.get_referrals_count(message.from_user.id)
        points, level = await db.get_points(message.from_user.id)
        referral_points = await db.get_referral_points(message.from_user.id)
        link = f"https://t.me/{me.username}?start=ref_{message.from_user.id}"
        await message.answer(
            "👥 Реферальная программа\n\n"
            f"Твоя уникальная ссылка:\n{link}\n\n"
            f"Приглашено: {ref_count}\n"
            f"Реферальные баллы: {referral_points}\n"
            f"Баллы: {points} ({level})\n"
            "За каждого приглашенного: +2 бонусных балла.",
            reply_markup=menu_only_kb(),
        )

    @dp.callback_query(F.data == "ref:info")
    async def cb_ref_info(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        me = await bot.get_me()
        ref_count = await db.get_referrals_count(callback.from_user.id)
        points, level = await db.get_points(callback.from_user.id)
        referral_points = await db.get_referral_points(callback.from_user.id)
        link = f"https://t.me/{me.username}?start=ref_{callback.from_user.id}"
        await callback.message.edit_text(
            "👥 Реферальная программа\n\n"
            f"Твоя уникальная ссылка:\n{link}\n\n"
            f"Приглашено: {ref_count}\n"
            f"Реферальные баллы: {referral_points}\n"
            f"Баллы: {points} ({level})\n"
            "За каждого приглашенного: +2 бонусных балла.",
            reply_markup=menu_only_kb(),
        )
        await callback.answer()

    @dp.callback_query(F.data == "menu")
    async def cb_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await db.touch_last_seen(callback.from_user.id)
        await show_main_menu(callback)
        await callback.answer()

    @dp.callback_query(F.data.startswith("interest:"))
    async def cb_interest(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        interest_key = callback.data.split(":", 1)[1]
        await db.set_interest(callback.from_user.id, interest_key)
        categories = pick_categories_by_interest(catalog, interest_key)
        await callback.message.edit_text(
            "Выбери категорию 👇",
            reply_markup=categories_kb(categories),
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("cat:"))
    async def cb_category(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        try:
            category_idx = int(callback.data.split(":")[1])
        except (IndexError, ValueError):
            await callback.answer("Ошибка категории")
            return

        category = catalog.get_category(category_idx)
        if not category:
            await callback.answer("Категория не найдена")
            return
        courses = catalog.data[category]
        items = [(i, c.title) for i, c in enumerate(courses[:30])]
        await callback.message.edit_text(
            f"Категория: {category}\nВыбери курс:",
            reply_markup=courses_kb(category_idx, items),
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("back:cats"))
    async def cb_back_cats(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        interest = await db.get_user_interest(callback.from_user.id) or "income"
        categories = pick_categories_by_interest(catalog, interest)
        await callback.message.edit_text("Выбери категорию 👇", reply_markup=categories_kb(categories))
        await callback.answer()

    @dp.callback_query(F.data.startswith("course:"))
    async def cb_course(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        try:
            _, category_idx_str, course_idx_str = callback.data.split(":")
            category_idx, course_idx = int(category_idx_str), int(course_idx_str)
        except ValueError:
            await callback.answer("Ошибка курса")
            return

        course = catalog.get_course(category_idx, course_idx)
        if not course:
            await callback.answer("Курс не найден")
            return

        allowed, used, limit = await db.can_download(callback.from_user.id)
        if not allowed:
            await callback.message.answer("❌ Ты уже скачал 2 курса сегодня. Попробуй завтра.")
            await callback.answer()
            return

        await db.register_download(callback.from_user.id, course.title, course.url)
        await callback.message.answer(
            f"✅ {course.title}\n\nЛимит на сегодня: {used + 1}/{limit}",
            reply_markup=course_actions_kb(category_idx, course_idx),
        )
        await callback.message.answer(course.url)
        await callback.answer()

    @dp.callback_query(F.data.startswith("fav:add:"))
    async def cb_fav_add(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        try:
            _, _, category_idx_str, course_idx_str = callback.data.split(":")
            category_idx, course_idx = int(category_idx_str), int(course_idx_str)
        except ValueError:
            await callback.answer("Ошибка")
            return

        course = catalog.get_course(category_idx, course_idx)
        if not course:
            await callback.answer("Курс не найден")
            return
        created = await db.add_favorite(callback.from_user.id, course.title, course.url)
        await callback.answer("Добавлено в избранное ⭐" if created else "Уже в избранном")

    @dp.callback_query(F.data == "fav:list")
    async def cb_fav_list(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await db.touch_last_seen(callback.from_user.id)
        favorites = await db.get_favorites(callback.from_user.id)
        if not favorites:
            await callback.message.edit_text("В избранном пока пусто.", reply_markup=main_menu_kb())
            await callback.answer()
            return
        await state.update_data(favorites=favorites)
        items = [(i, title) for i, (title, _) in enumerate(favorites)]
        await callback.message.edit_text("📂 Избранное:", reply_markup=favorites_kb(items))
        await callback.answer()

    @dp.callback_query(F.data.startswith("fav:pick:"))
    async def cb_fav_pick(callback: CallbackQuery, state: FSMContext) -> None:
        await db.touch_last_seen(callback.from_user.id)
        data = await state.get_data()
        favorites: list[tuple[str, str]] = data.get("favorites", [])
        try:
            idx = int(callback.data.split(":")[2])
        except (IndexError, ValueError):
            await callback.answer("Ошибка")
            return
        if not (0 <= idx < len(favorites)):
            await callback.answer("Не найдено")
            return
        title, url = favorites[idx]
        await callback.message.answer(f"⭐ {title}", reply_markup=menu_only_kb())
        await callback.message.answer(url)
        await callback.answer()

    @dp.callback_query(F.data == "links:list")
    async def cb_links_list(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        downloads = await db.get_downloads(callback.from_user.id, limit=20)
        if not downloads:
            await callback.message.edit_text(
                "Пока нет полученных ссылок. Выбери курс, и он появится в этом разделе.",
                reply_markup=menu_only_kb(),
            )
            await callback.answer()
            return
        lines = [f"{idx + 1}. {title}\n{url}" for idx, (title, url) in enumerate(downloads)]
        await callback.message.edit_text(
            "🗂 Твои сохраненные ссылки:\n\n" + "\n\n".join(lines),
            reply_markup=menu_only_kb(),
        )
        await callback.answer()

    @dp.callback_query(F.data == "search:start")
    async def cb_search_start(callback: CallbackQuery, state: FSMContext) -> None:
        await db.touch_last_seen(callback.from_user.id)
        await state.set_state(SearchStates.waiting_query)
        await callback.message.edit_text(
            "Напиши название курса или ключевое слово для поиска:",
            reply_markup=menu_only_kb(),
        )
        await callback.answer()

    @dp.message(SearchStates.waiting_query)
    async def search_query(message: Message, state: FSMContext) -> None:
        await ensure_and_reward(message)
        results = catalog.search(message.text or "")
        if not results:
            await message.answer("Ничего не найдено. Попробуй другой запрос.", reply_markup=menu_only_kb())
            return
        await state.update_data(search_results=[(c.title, c.url) for c in results])
        items = [(i, c.title) for i, c in enumerate(results)]
        await message.answer("Нашел такие варианты:", reply_markup=search_results_kb(items))

    @dp.callback_query(F.data.startswith("search:pick:"))
    async def cb_search_pick(callback: CallbackQuery, state: FSMContext) -> None:
        await db.touch_last_seen(callback.from_user.id)
        data = await state.get_data()
        results: list[tuple[str, str]] = data.get("search_results", [])
        try:
            idx = int(callback.data.split(":")[2])
        except (IndexError, ValueError):
            await callback.answer("Ошибка")
            return
        if not (0 <= idx < len(results)):
            await callback.answer("Курс не найден")
            return
        title, url = results[idx]

        allowed, used, limit = await db.can_download(callback.from_user.id)
        if not allowed:
            await callback.message.answer(
                "❌ Ты уже скачал 2 курса сегодня. Попробуй завтра.",
                reply_markup=menu_only_kb(),
            )
            await callback.answer()
            return

        await db.register_download(callback.from_user.id, title, url)
        await callback.message.answer(
            f"✅ {title}\n\nЛимит на сегодня: {used + 1}/{limit}",
            reply_markup=menu_only_kb(),
        )
        await callback.message.answer(url)
        await callback.answer()

    @dp.callback_query(F.data == "points:buy")
    async def cb_points_buy(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        ok = await db.try_spend_points_for_limit(callback.from_user.id, cost=5)
        points, level = await db.get_points(callback.from_user.id)
        if ok:
            await callback.message.answer(
                f"🔓 Лимит увеличен на +1 курс сегодня.\nОстаток баллов: {points}\nУровень: {level}",
                reply_markup=menu_only_kb(),
            )
        else:
            await callback.message.answer(
                f"Недостаточно баллов. Нужно 5.\nСейчас: {points}\nУровень: {level}",
                reply_markup=menu_only_kb(),
            )
        await callback.answer()

    @dp.callback_query(F.data == "rec:start")
    async def cb_recommendations(callback: CallbackQuery) -> None:
        await db.touch_last_seen(callback.from_user.id)
        interest = await db.get_user_interest(callback.from_user.id) or "income"
        categories = pick_categories_by_interest(catalog, interest)
        pool: list[Course] = []
        for idx, _ in categories[:5]:
            category = catalog.get_category(idx)
            if category:
                pool.extend(catalog.data[category])
        if not pool:
            await callback.message.answer("Пока не могу подобрать рекомендации.", reply_markup=menu_only_kb())
            await callback.answer()
            return
        picks = random.sample(pool, k=min(3, len(pool)))
        msg = "🧭 Рекомендую начать с:\n\n" + "\n".join(f"• {course.title}" for course in picks)
        await callback.message.answer(msg, reply_markup=menu_only_kb())
        for course in picks:
            await callback.message.answer(course.url)
        await callback.answer()

    async def reminder_loop() -> None:
        while True:
            try:
                user_ids = await db.users_for_reminder()
                for user_id in user_ids:
                    try:
                        await bot.send_message(user_id, "Мы добавили новые курсы 👀 Загляни!")
                        await db.mark_reminder_sent(user_id)
                    except Exception as send_error:  # noqa: BLE001
                        logger.warning("Reminder not sent to %s: %s", user_id, send_error)
            except Exception as loop_error:  # noqa: BLE001
                logger.exception("Reminder loop error: %s", loop_error)
            await asyncio.sleep(config.reminder_check_seconds)

    asyncio.create_task(reminder_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
