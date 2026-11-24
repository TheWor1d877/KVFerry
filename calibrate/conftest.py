"""pytest 收集时把 calibrate 目录加进 sys.path。"""

from __future__ import annotations

import sys
from pathlib import Path

_CALIBRATE_DIR = Path(__file__).resolve().parent
if str(_CALIBRATE_DIR) not in sys.path:
    sys.path.insert(0, str(_CALIBRATE_DIR))