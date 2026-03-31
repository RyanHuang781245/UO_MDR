from __future__ import annotations

from flask import Blueprint


auth_bp = Blueprint("auth_bp", __name__, template_folder="templates", url_prefix="/auth")
