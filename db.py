from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import aiosqlite


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def level_by_points(points: int) -> str:
    if points >= 60:
        return "Профи"
    if points >= 20:
        return "Продвинутый"
    return "Новичок"


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    @asynccontextmanager
    async def connect(self):
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA foreign_keys=ON;")
        try:
            yield conn
        finally:
            await conn.close()

    async def init(self) -> None:
        async with self.connect() as conn:
            await conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    interest TEXT,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_reset_at TEXT NOT NULL,
                    daily_downloads INTEGER NOT NULL DEFAULT 0,
                    bonus_limit INTEGER NOT NULL DEFAULT 0,
                    referred_by INTEGER,
                    reminder_sent_at TEXT
                );

                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    course_title TEXT NOT NULL,
                    course_url TEXT NOT NULL,
                    downloaded_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS favorites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    course_title TEXT NOT NULL,
                    course_url TEXT NOT NULL,
                    added_at TEXT NOT NULL,
                    UNIQUE(user_id, course_url),
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inviter_id INTEGER NOT NULL,
                    invited_id INTEGER NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS points (
                    user_id INTEGER PRIMARY KEY,
                    points INTEGER NOT NULL DEFAULT 0,
                    level TEXT NOT NULL DEFAULT 'Новичок',
                    referral_points INTEGER NOT NULL DEFAULT 0,
                    last_daily_visit TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS bot_stats (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            try:
                await conn.execute(
                    "ALTER TABLE points ADD COLUMN referral_points INTEGER NOT NULL DEFAULT 0"
                )
            except aiosqlite.OperationalError:
                pass
            await conn.execute(
                """
                INSERT OR IGNORE INTO bot_stats(key, value)
                VALUES ('total_messages', 0)
                """
            )
            await conn.commit()

    async def increment_message_counter(self) -> None:
        async with self.connect() as conn:
            await conn.execute(
                """
                UPDATE bot_stats
                SET value = value + 1
                WHERE key = 'total_messages'
                """
            )
            await conn.commit()

    async def get_admin_stats(self) -> tuple[int, int]:
        async with self.connect() as conn:
            users_row = await (await conn.execute("SELECT COUNT(*) AS c FROM users")).fetchone()
            messages_row = await (
                await conn.execute(
                    """
                    SELECT value FROM bot_stats
                    WHERE key = 'total_messages'
                    """
                )
            ).fetchone()
            users_count = int(users_row["c"]) if users_row else 0
            messages_count = int(messages_row["value"]) if messages_row else 0
            return users_count, messages_count

    async def get_admin_stats_full(self) -> dict[str, int]:
        now = datetime.utcnow()
        since_24h = (now - timedelta(hours=24)).isoformat(timespec="seconds")
        since_7d = (now - timedelta(days=7)).isoformat(timespec="seconds")
        async with self.connect() as conn:
            users_total_row = await (await conn.execute("SELECT COUNT(*) AS c FROM users")).fetchone()
            users_new_24h_row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE created_at >= ?",
                    (since_24h,),
                )
            ).fetchone()
            users_active_24h_row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE last_seen_at >= ?",
                    (since_24h,),
                )
            ).fetchone()
            messages_total_row = await (
                await conn.execute(
                    "SELECT value FROM bot_stats WHERE key = 'total_messages'"
                )
            ).fetchone()
            downloads_total_row = await (await conn.execute("SELECT COUNT(*) AS c FROM downloads")).fetchone()
            downloads_24h_row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM downloads WHERE downloaded_at >= ?",
                    (since_24h,),
                )
            ).fetchone()
            downloads_7d_row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM downloads WHERE downloaded_at >= ?",
                    (since_7d,),
                )
            ).fetchone()
            referrals_total_row = await (await conn.execute("SELECT COUNT(*) AS c FROM referrals")).fetchone()
            favorites_total_row = await (await conn.execute("SELECT COUNT(*) AS c FROM favorites")).fetchone()

            return {
                "users_total": int(users_total_row["c"]) if users_total_row else 0,
                "users_new_24h": int(users_new_24h_row["c"]) if users_new_24h_row else 0,
                "users_active_24h": int(users_active_24h_row["c"]) if users_active_24h_row else 0,
                "messages_total": int(messages_total_row["value"]) if messages_total_row else 0,
                "downloads_total": int(downloads_total_row["c"]) if downloads_total_row else 0,
                "downloads_24h": int(downloads_24h_row["c"]) if downloads_24h_row else 0,
                "downloads_7d": int(downloads_7d_row["c"]) if downloads_7d_row else 0,
                "referrals_total": int(referrals_total_row["c"]) if referrals_total_row else 0,
                "favorites_total": int(favorites_total_row["c"]) if favorites_total_row else 0,
            }

    async def ensure_user(self, user_id: int, username: str | None, full_name: str) -> None:
        async with self.connect() as conn:
            now = now_iso()
            await conn.execute(
                """
                INSERT INTO users(user_id, username, full_name, created_at, last_seen_at, last_reset_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    full_name = excluded.full_name,
                    last_seen_at = excluded.last_seen_at
                """,
                (user_id, username, full_name, now, now, now),
            )
            await conn.execute(
                "INSERT OR IGNORE INTO points(user_id, points, level) VALUES (?, 0, 'Новичок')",
                (user_id,),
            )
            await conn.commit()

    async def touch_last_seen(self, user_id: int) -> None:
        async with self.connect() as conn:
            await conn.execute(
                "UPDATE users SET last_seen_at = ?, reminder_sent_at = NULL WHERE user_id = ?",
                (now_iso(), user_id),
            )
            await conn.commit()

    async def set_interest(self, user_id: int, interest: str) -> None:
        async with self.connect() as conn:
            await conn.execute(
                "UPDATE users SET interest = ?, last_seen_at = ? WHERE user_id = ?",
                (interest, now_iso(), user_id),
            )
            await conn.commit()

    async def get_user_interest(self, user_id: int) -> str | None:
        async with self.connect() as conn:
            row = await (
                await conn.execute("SELECT interest FROM users WHERE user_id = ?", (user_id,))
            ).fetchone()
            return row["interest"] if row else None

    async def daily_visit_reward(self, user_id: int) -> tuple[int, str, bool]:
        today = datetime.utcnow().date().isoformat()
        async with self.connect() as conn:
            row = await (
                await conn.execute(
                    "SELECT points, last_daily_visit FROM points WHERE user_id = ?", (user_id,)
                )
            ).fetchone()
            if row is None:
                points = 1
                level = level_by_points(points)
                await conn.execute(
                    "INSERT INTO points(user_id, points, level, last_daily_visit) VALUES (?, ?, ?, ?)",
                    (user_id, points, level, today),
                )
                await conn.commit()
                return points, level, True

            points = int(row["points"])
            rewarded = False
            if row["last_daily_visit"] != today:
                points += 1
                rewarded = True
            level = level_by_points(points)
            await conn.execute(
                "UPDATE points SET points = ?, level = ?, last_daily_visit = ? WHERE user_id = ?",
                (points, level, today, user_id),
            )
            await conn.commit()
            return points, level, rewarded

    async def get_points(self, user_id: int) -> tuple[int, str]:
        async with self.connect() as conn:
            row = await (
                await conn.execute(
                    "SELECT points, level FROM points WHERE user_id = ?",
                    (user_id,),
                )
            ).fetchone()
            if not row:
                return 0, "Новичок"
            return int(row["points"]), row["level"]

    async def get_referral_points(self, user_id: int) -> int:
        async with self.connect() as conn:
            row = await (
                await conn.execute(
                    "SELECT referral_points FROM points WHERE user_id = ?",
                    (user_id,),
                )
            ).fetchone()
            if not row:
                return 0
            return int(row["referral_points"] or 0)

    async def try_spend_points_for_limit(self, user_id: int, cost: int = 5) -> bool:
        async with self.connect() as conn:
            row = await (
                await conn.execute("SELECT points FROM points WHERE user_id = ?", (user_id,))
            ).fetchone()
            if not row or int(row["points"]) < cost:
                return False
            await conn.execute(
                "UPDATE points SET points = points - ?, level = ? WHERE user_id = ?",
                (cost, level_by_points(int(row["points"]) - cost), user_id),
            )
            await conn.execute(
                "UPDATE users SET bonus_limit = bonus_limit + 1 WHERE user_id = ?",
                (user_id,),
            )
            await conn.commit()
            return True

    async def add_referral(self, inviter_id: int, invited_id: int) -> bool:
        if inviter_id == invited_id:
            return False
        async with self.connect() as conn:
            inviter_exists = await (
                await conn.execute("SELECT user_id FROM users WHERE user_id = ?", (inviter_id,))
            ).fetchone()
            invited_exists = await (
                await conn.execute("SELECT user_id FROM users WHERE user_id = ?", (invited_id,))
            ).fetchone()
            if not inviter_exists or not invited_exists:
                return False
            exists = await (
                await conn.execute(
                    "SELECT invited_id FROM referrals WHERE invited_id = ?",
                    (invited_id,),
                )
            ).fetchone()
            if exists:
                return False
            await conn.execute(
                "INSERT INTO referrals(inviter_id, invited_id, created_at) VALUES (?, ?, ?)",
                (inviter_id, invited_id, now_iso()),
            )
            await conn.execute(
                "UPDATE users SET referred_by = ? WHERE user_id = ? AND referred_by IS NULL",
                (inviter_id, invited_id),
            )
            points_row = await (
                await conn.execute("SELECT points FROM points WHERE user_id = ?", (inviter_id,))
            ).fetchone()
            current_points = int(points_row["points"]) if points_row else 0
            new_points = current_points + 2
            await conn.execute(
                """
                INSERT INTO points(user_id, points, level, referral_points)
                VALUES (?, ?, ?, 2)
                ON CONFLICT(user_id) DO UPDATE SET
                    points = ?,
                    level = ?,
                    referral_points = COALESCE(referral_points, 0) + 2
                """,
                (inviter_id, new_points, level_by_points(new_points), new_points, level_by_points(new_points)),
            )
            await conn.commit()
            return True

    async def get_downloads(self, user_id: int, limit: int = 20) -> list[tuple[str, str]]:
        async with self.connect() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT course_title, course_url FROM downloads
                    WHERE user_id = ?
                    ORDER BY downloaded_at DESC
                    LIMIT ?
                    """,
                    (user_id, limit),
                )
            ).fetchall()
            return [(r["course_title"], r["course_url"]) for r in rows]

    async def get_referrals_count(self, inviter_id: int) -> int:
        async with self.connect() as conn:
            row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM referrals WHERE inviter_id = ?",
                    (inviter_id,),
                )
            ).fetchone()
            return int(row["c"]) if row else 0

    async def _reset_if_needed(self, conn: aiosqlite.Connection, user_id: int) -> None:
        row = await (
            await conn.execute(
                "SELECT last_reset_at FROM users WHERE user_id = ?",
                (user_id,),
            )
        ).fetchone()
        if not row:
            return
        last_date = datetime.fromisoformat(row["last_reset_at"]).date()
        if last_date < datetime.utcnow().date():
            await conn.execute(
                "UPDATE users SET daily_downloads = 0, bonus_limit = 0, last_reset_at = ? WHERE user_id = ?",
                (now_iso(), user_id),
            )

    async def can_download(self, user_id: int) -> tuple[bool, int, int]:
        async with self.connect() as conn:
            await self._reset_if_needed(conn, user_id)
            user = await (
                await conn.execute(
                    "SELECT daily_downloads, bonus_limit FROM users WHERE user_id = ?",
                    (user_id,),
                )
            ).fetchone()
            if not user:
                return False, 0, 0
            referrals_count_row = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM referrals WHERE inviter_id = ?",
                    (user_id,),
                )
            ).fetchone()
            referrals_count = int(referrals_count_row["c"]) if referrals_count_row else 0
            limit = 2 + int(user["bonus_limit"]) + referrals_count
            used = int(user["daily_downloads"])
            await conn.commit()
            return used < limit, used, limit

    async def register_download(self, user_id: int, title: str, url: str) -> None:
        async with self.connect() as conn:
            await self._reset_if_needed(conn, user_id)
            await conn.execute(
                "UPDATE users SET daily_downloads = daily_downloads + 1, last_seen_at = ? WHERE user_id = ?",
                (now_iso(), user_id),
            )
            await conn.execute(
                "INSERT INTO downloads(user_id, course_title, course_url, downloaded_at) VALUES (?, ?, ?, ?)",
                (user_id, title, url, now_iso()),
            )
            await conn.commit()

    async def add_favorite(self, user_id: int, title: str, url: str) -> bool:
        async with self.connect() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO favorites(user_id, course_title, course_url, added_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (user_id, title, url, now_iso()),
                )
                await conn.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def get_favorites(self, user_id: int, limit: int = 25) -> list[tuple[str, str]]:
        async with self.connect() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT course_title, course_url FROM favorites
                    WHERE user_id = ?
                    ORDER BY added_at DESC
                    LIMIT ?
                    """,
                    (user_id, limit),
                )
            ).fetchall()
            return [(r["course_title"], r["course_url"]) for r in rows]

    async def users_for_reminder(self) -> list[int]:
        threshold = (datetime.utcnow() - timedelta(days=2)).isoformat(timespec="seconds")
        async with self.connect() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT user_id FROM users
                    WHERE last_seen_at < ?
                      AND (reminder_sent_at IS NULL OR reminder_sent_at < last_seen_at)
                    """,
                    (threshold,),
                )
            ).fetchall()
            return [int(r["user_id"]) for r in rows]

    async def mark_reminder_sent(self, user_id: int) -> None:
        async with self.connect() as conn:
            await conn.execute(
                "UPDATE users SET reminder_sent_at = ? WHERE user_id = ?",
                (now_iso(), user_id),
            )
            await conn.commit()
