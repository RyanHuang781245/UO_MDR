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


def test_copy_files_multiple_keywords(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # File that matches both keywords
    (src / "Shipping simulation test EO report.txt").write_text("data")
    # File that matches only one keyword and should not be copied
    (src / "Shipping simulation test only.txt").write_text("data")

    copied = copy_files(
        str(src),
        str(dest),
        ["Shipping simulation test", "EO"],
    )

    assert dest.joinpath("Shipping simulation test EO report.txt").exists()
    assert not dest.joinpath("Shipping simulation test only.txt").exists()
    assert len(copied) == 1


def test_copy_files_destination_inside_source(tmp_path):
    src = tmp_path / "root"
    dest = src / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "report.txt").write_text("data")

    copied = copy_files(str(src), str(dest), ["report"])

    dest_file = dest / "report.txt"
    assert dest_file.exists()
    assert dest_file.read_text() == "data"
    assert copied == [str(dest_file)]
    # Ensure only the copied file exists in destination
    assert len(list(dest.iterdir())) == 1
