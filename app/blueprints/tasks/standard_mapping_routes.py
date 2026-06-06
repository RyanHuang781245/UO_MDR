from __future__ import annotations

import sys

from app.blueprints.standard_mapping import routes as _routes

sys.modules[__name__] = _routes
