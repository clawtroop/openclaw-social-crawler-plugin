from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

for candidate in (ROOT, SCRIPTS):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)


@pytest.fixture
def workspace_tmp_path() -> Path:
    path = ROOT / ".pytest-tmp" / uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    return path
