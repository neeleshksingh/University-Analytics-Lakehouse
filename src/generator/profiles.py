"""
Hidden behaviour profiles for students.

A profile is never stored in the database. It only decides how a student behaves:
how often they attend, how well they score, how they pay fees. The pipeline must
*discover* these patterns from the data, the way a real analytics team would.

Profiles are derived from (seed, student_id) alone, so seed mode and every later
delta run agree on how the same student behaves.
"""

import random
from dataclasses import dataclass

# kind: (share of students, base attendance rate, base exam score, chance to drop out over the program)
KINDS = {
    "regular":   (0.70, 0.88, 0.68, 0.01),
    "irregular": (0.20, 0.72, 0.55, 0.06),
    "at_risk":   (0.10, 0.52, 0.42, 0.30),
}

# fee behaviour: on_time ~70%, late ~20%, defaulter ~10%
FEE_BEHAVIOUR = [("on_time", 0.70), ("late", 0.20), ("defaulter", 0.10)]


@dataclass(frozen=True)
class StudentProfile:
    kind: str
    attendance_rate: float      # probability of attending a class
    ability: float              # baseline exam percentage (0..1)
    drop_probability: float
    fee_behaviour: str
    hostel: bool
    transport: bool


def student_profile(seed: int, student_id: int) -> StudentProfile:
    rng = random.Random(f"profile-{seed}-{student_id}")

    roll = rng.random()
    cumulative = 0.0
    kind = "regular"
    for name, (share, *_rest) in KINDS.items():
        cumulative += share
        if roll < cumulative:
            kind = name
            break
    _, attendance, ability, drop = KINDS[kind]

    # Behaviour is correlated: at-risk students are more likely to pay late
    fee_weights = [w for _, w in FEE_BEHAVIOUR]
    if kind == "at_risk":
        fee_weights = [0.45, 0.30, 0.25]
    fee = rng.choices([name for name, _ in FEE_BEHAVIOUR], weights=fee_weights)[0]

    hostel = rng.random() < 0.35
    transport = (not hostel) and rng.random() < 0.40

    return StudentProfile(
        kind=kind,
        attendance_rate=min(0.99, max(0.15, rng.gauss(attendance, 0.05))),
        ability=min(0.95, max(0.15, rng.gauss(ability, 0.07))),
        drop_probability=drop,
        fee_behaviour=fee,
        hostel=hostel,
        transport=transport,
    )
