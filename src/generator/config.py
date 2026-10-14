"""
Settings for the synthetic data generator.

Every random choice in the generator flows from `seed`, so the same settings
always produce exactly the same data.
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42                      # same seed -> identical data on every run
    scale: float = 1.0                  # multiplies student intake (0.1 = quick test, 0.33 = Azure)
    first_year: int = 2022              # first academic year (2022-23)
    seed_until: date = date(2026, 9, 30)  # "today" for the historical load; delta mode continues from here
    bad_data_rate: float = 0.01         # base rate for deliberately injected data-quality problems

    # Business rules the data is shaped around
    section_size: int = 60
    pass_percent: float = 0.40
