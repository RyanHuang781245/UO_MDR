import base64
from pathlib import Path

import zipfile

from spire.doc import Document, FileFormat

from modules.Extract_AllFile_to_FinalWord import extract_word_all_content


PNG_BASE64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y2X1f8AAAAASUVORK5CYII="
JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDAREAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAb/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAH/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/AT//xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/AT//2Q=="
)


def _write_image(path: Path, data: str) -> None:
    path.write_bytes(base64.b64decode(data))


def test_extract_word_all_content_preserves_image_extensions(tmp_path: Path) -> None:
    png_path = tmp_path / "sample.png"
    jpg_path = tmp_path / "sample.jpg"
    _write_image(png_path, PNG_BASE64)
    _write_image(jpg_path, JPEG_BASE64)

    src = Document()
    sec = src.AddSection()
    sec.AddParagraph().AppendPicture(str(png_path))
    sec.AddParagraph().AppendPicture(str(jpg_path))
    src_path = tmp_path / "source.docx"
    src.SaveToFile(str(src_path), FileFormat.Docx)
    src.Close()

    result = extract_word_all_content(str(src_path))
    out_path = Path(result["output_docx"])
    assert out_path.is_file()

    with zipfile.ZipFile(out_path, "r") as zf:
        media_suffixes = {
            Path(name).suffix
            for name in zf.namelist()
            if name.startswith("word/media/")
        }
    assert ".png" in media_suffixes
    assert ".jpg" in media_suffixes
