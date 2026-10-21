import os

LAKE_ROOT = os.environ.get("LAKE_ROOT", "lake")

LANDING = f"{LAKE_ROOT}/landing"
BRONZE = f"{LAKE_ROOT}/bronze"
SILVER = f"{LAKE_ROOT}/silver"
GOLD = f"{LAKE_ROOT}/gold"
OPS = f"{LAKE_ROOT}/ops"