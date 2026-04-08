from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_COURSES_FILE = BASE_DIR / "courses.txt"


@dataclass(frozen=True)
class Config:
    bot_token: str
    db_path: Path
    courses_file: Path
    reminder_check_seconds: int = 3600


def get_config() -> Config:
    load_dotenv()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("BOT_TOKEN is not set. Put it in .env file.")

    db_path = Path(os.getenv("DB_PATH", str(BASE_DIR / "bot.db")))
    courses_file = Path(os.getenv("COURSES_FILE", str(DEFAULT_COURSES_FILE)))
    reminder_check_seconds = int(os.getenv("REMINDER_CHECK_SECONDS", "3600"))
    return Config(
        bot_token=token,
        db_path=db_path,
        courses_file=courses_file,
        reminder_check_seconds=reminder_check_seconds,
    )
