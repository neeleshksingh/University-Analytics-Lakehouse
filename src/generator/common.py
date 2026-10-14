"""Small helpers shared by seed mode and delta mode."""

import random
from datetime import date, datetime, time, timedelta

from src.generator.reference import GRADES, PAYMENT_MODES


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute))


def term_calendar(first_year: int, last_year: int):
    """Academic years and their two terms (odd: Jul-Dec, even: Jan-May), in date order."""
    years, terms = [], []
    for y in range(first_year, last_year + 1):
        label = f"{y}-{(y + 1) % 100:02d}"
        years.append(dict(year=y, label=label, start=date(y, 7, 1), end=date(y + 1, 6, 30)))
        for term_no, (start, end) in enumerate(
            [(date(y, 7, 15), date(y, 12, 15)), (date(y + 1, 1, 10), date(y + 1, 5, 31))], start=1
        ):
            mid_start = start + timedelta(weeks=8)
            mid_start -= timedelta(days=mid_start.weekday())            # Monday of week 8
            end_start = end + timedelta(days=3)
            terms.append(dict(
                year=y, term_no=term_no, name=f"{'Odd' if term_no == 1 else 'Even'} {label}",
                start=start, end=end,
                mid_start=mid_start, mid_end=mid_start + timedelta(days=4),
                end_start=end_start, end_end=end_start + timedelta(days=11),
                supp_start=end_start + timedelta(days=50), supp_end=end_start + timedelta(days=56),
            ))
    for idx, t in enumerate(terms):
        t["idx"] = idx
    return years, terms


def attendance_status(rng: random.Random, rate: float) -> str:
    if rng.random() < rate:
        return "late" if rng.random() < 0.06 else "present"
    return "excused" if rng.random() < 0.08 else "absent"


def grade_for(pct: float) -> str:
    for minimum, grade in GRADES:
        if pct >= minimum:
            return grade
    return "F"


def payment_mode(rng: random.Random) -> str:
    return rng.choices([m for m, _ in PAYMENT_MODES], weights=[w for _, w in PAYMENT_MODES])[0]


def indian_phone(rng: random.Random) -> str:
    return str(rng.choice("6789")) + "".join(rng.choice("0123456789") for _ in range(9))


def invalid_phone(rng: random.Random) -> str:
    """Deliberately broken phone numbers for the data-quality layer to catch."""
    return rng.choice([
        indian_phone(rng)[:8],                       # too short
        "98765abcde",                                # letters
        "0000000000",                                # placeholder
        indian_phone(rng) + "1",                     # too long
    ])


def section_letter(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA ..."""
    letters = ""
    n += 1
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters
