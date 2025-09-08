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


def test_copy_files_handles_destination_inside_source(tmp_path):
    src = tmp_path / "files"
    src.mkdir()
    (src / "example.txt").write_text("hello", encoding="utf-8")
    dest = src / "dest"
    copied = copy_files(str(src), str(dest), ["example"])
    assert copied == [str(dest / "example.txt")]
    assert (dest / "example.txt").read_text(encoding="utf-8") == "hello"
