from __future__ import annotations

from flask import Blueprint


global_batch_bp = Blueprint("global_batch_bp", __name__, template_folder="templates", url_prefix="/batch/global")
