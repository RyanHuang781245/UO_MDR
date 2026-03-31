from __future__ import annotations

from flask import Blueprint


tasks_bp = Blueprint("tasks_bp", __name__, template_folder="templates")
