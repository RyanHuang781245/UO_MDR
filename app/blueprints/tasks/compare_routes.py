from __future__ import annotations

import sys

from app.blueprints.task_compare import routes as _routes

sys.modules[__name__] = _routes
