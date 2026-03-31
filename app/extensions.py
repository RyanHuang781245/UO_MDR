from __future__ import annotations

from flask_sqlalchemy import SQLAlchemy
from flask_ldap3_login import LDAP3LoginManager
from flask_login import LoginManager

db = SQLAlchemy()

login_manager = LoginManager()
ldap_manager = LDAP3LoginManager()
