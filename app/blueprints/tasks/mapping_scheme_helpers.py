from __future__ import annotations

import sys

from app.blueprints.task_mapping import scheme_helpers as _scheme_helpers

sys.modules[__name__] = _scheme_helpers
