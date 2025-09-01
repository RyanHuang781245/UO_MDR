from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))
from modules.file_copier import copy_files


def test_copy_files_overwrite(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    file_path = src / "example.txt"
    file_path.write_text("first")

    copy_files(str(src), str(dest), ["example"])
    dest_file = dest / "example.txt"
    assert dest_file.read_text() == "first"

    file_path.write_text("second")
    copy_files(str(src), str(dest), ["example"])
    assert dest_file.read_text() == "second"
