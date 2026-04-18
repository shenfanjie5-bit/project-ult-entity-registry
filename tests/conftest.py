from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_SRC = PROJECT_ROOT.parent / "contracts" / "src"

if CONTRACTS_SRC.exists():
    sys.path.insert(0, str(CONTRACTS_SRC))
