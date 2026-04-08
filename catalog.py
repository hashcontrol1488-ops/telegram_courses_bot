from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
import re


URL_RE = re.compile(r"(https?://\S+)$")


@dataclass(frozen=True)
class Course:
    category: str
    title: str
    url: str


def _clean_line(line: str) -> str:
    return line.strip().replace("\t", " ")


def parse_courses_file(path: Path) -> dict[str, list[Course]]:
    if not path.exists():
        raise FileNotFoundError(f"Courses file not found: {path}")

    categories: dict[str, list[Course]] = {}
    current_category: str | None = None

    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = _clean_line(raw)
        if not line:
            continue

        url_match = URL_RE.search(line)
        if not url_match:
            current_category = line
            categories.setdefault(current_category, [])
            continue

        if current_category is None:
            current_category = "Разное"
            categories.setdefault(current_category, [])

        url = url_match.group(1)
        title = line[: url_match.start()].strip().rstrip("+").strip()
        if not title:
            title = "Курс без названия"

        categories[current_category].append(
            Course(category=current_category, title=title, url=url)
        )

    return {k: v for k, v in categories.items() if v}


class Catalog:
    def __init__(self, data: dict[str, list[Course]]) -> None:
        self.data = data
        self.categories = list(data.keys())
        self.flat_courses: list[Course] = []
        for category in self.categories:
            self.flat_courses.extend(data[category])

    def get_category(self, idx: int) -> str | None:
        if 0 <= idx < len(self.categories):
            return self.categories[idx]
        return None

    def get_course(self, category_idx: int, course_idx: int) -> Course | None:
        category = self.get_category(category_idx)
        if not category:
            return None
        courses = self.data.get(category, [])
        if 0 <= course_idx < len(courses):
            return courses[course_idx]
        return None

    def random_course(self) -> Course | None:
        if not self.flat_courses:
            return None
        return random.choice(self.flat_courses)

    def search(self, query: str, limit: int = 15) -> list[Course]:
        q = query.casefold().strip()
        if not q:
            return []
        results = [c for c in self.flat_courses if q in c.title.casefold()]
        return results[:limit]
