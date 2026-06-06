from __future__ import annotations

import sys

from app.blueprints.task_compare import helpers as _helpers

sys.modules[__name__] = _helpers
