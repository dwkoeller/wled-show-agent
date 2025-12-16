from __future__ import annotations

import sys
from pathlib import Path


# Allow `import config`, `import show_config`, etc when running `pytest` from repo root.
AGENT_DIR = Path(__file__).resolve().parents[1]
if str(AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_DIR))

