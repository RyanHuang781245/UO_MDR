from __future__ import annotations

import sys

from app.blueprints.task_compare import compat as _compat

sys.modules[__name__] = _compat
