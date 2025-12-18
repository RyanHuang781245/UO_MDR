import sys
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 測試時避免依賴外部 MSSQL（RBAC/auth 可用環境變數開關）。
os.environ.setdefault("AUTH_ENABLED", "0")
