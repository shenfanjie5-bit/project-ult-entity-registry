from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_SRC = PROJECT_ROOT.parent / "contracts" / "src"


def _has_installed_contracts_package() -> bool:
    try:
        version("project-ult-contracts")
    except PackageNotFoundError:
        return False
    return True


if CONTRACTS_SRC.exists() and not _has_installed_contracts_package():
    sys.path.insert(0, str(CONTRACTS_SRC))
