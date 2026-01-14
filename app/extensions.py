from __future__ import annotations

from flask_ldap3_login import LDAP3LoginManager
from flask_login import LoginManager

from modules.auth_models import db

login_manager = LoginManager()
ldap_manager = LDAP3LoginManager()
