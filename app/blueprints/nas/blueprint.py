from __future__ import annotations

from flask import Blueprint


nas_bp = Blueprint("nas_bp", __name__, template_folder="templates", url_prefix="/nas")
