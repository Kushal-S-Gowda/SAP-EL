"""Apply idempotent PostgreSQL schema updates."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database.db import apply_schema


def main() -> None:
    apply_schema()
    print("PostgreSQL schema is up to date.")


if __name__ == "__main__":
    main()
