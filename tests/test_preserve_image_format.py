import base64
from pathlib import Path

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

    out_doc = Document()
    out_section = out_doc.AddSection()
    image_dir = tmp_path / "images"

    extract_word_all_content(
        str(src_path),
        output_image_path=str(image_dir),
        output_doc=out_doc,
        section=out_section,
    )

    out_doc.Close()

    saved_images = {p.name for p in image_dir.iterdir() if p.is_file()}
    assert saved_images == {"Image-1.png", "Image-2.jpg"}
