import os
import re
import queue
import tempfile
from typing import Iterable, Optional
from uuid import uuid4
import fitz  # PyMuPDF
from docx import Document as DocxDocument
from docx.shared import Pt
from docx.enum.text import WD_LINE_SPACING
from docx.oxml.ns import qn
from spire.doc import *
from spire.doc.common import *
from modules.extract_word_all_content import extract_body_with_options
from modules.extract_word_chapter import extract_section_docx_xml


def set_run_font_eastasia(run, eastasia_name: str):
    """補設定東亞字型，避免中文顯示為預設字型。"""
    if eastasia_name:
        rPr = run._element.get_or_add_rPr()
        rFonts = rPr.get_or_add_rFonts()
        rFonts.set(qn("w:eastAsia"), eastasia_name)


def _build_output_docx_path(input_file: str, suffix: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(input_file))
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    safe_suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", suffix) or "extract"
    candidate = os.path.join(base_dir, f"{base_name}_{safe_suffix}.docx")
    if os.path.exists(candidate):
        candidate = os.path.join(
            tempfile.gettempdir(),
            f"{base_name}_{safe_suffix}_{uuid4().hex[:8]}.docx",
        )
    return candidate


def _append_docx_to_section(docx_path: str, output_doc: Document, section=None):
    target_section = section or output_doc.AddSection()
    temp_doc = Document()
    temp_doc.LoadFromFile(docx_path)
    try:
        for s_idx in range(temp_doc.Sections.Count):
            src_section = temp_doc.Sections.get_Item(s_idx)
            body = src_section.Body
            for i in range(body.ChildObjects.Count):
                cloned = body.ChildObjects.get_Item(i).Clone()
                target_section.Body.ChildObjects.Add(cloned)
    finally:
        temp_doc.Close()
    return target_section


def _read_first_paragraph_text(docx_path: str) -> str:
    try:
        doc = DocxDocument(docx_path)
    except Exception:
        return ""
    for para in doc.paragraphs:
        text = (para.text or "").strip()
        if text:
            return text
    return ""


def _get_paragraph_text(paragraph: Paragraph) -> str:
    text = paragraph.ListText + " " if paragraph.ListText else ""
    for j in range(paragraph.ChildObjects.Count):
        sub = paragraph.ChildObjects.get_Item(j)
        if sub.DocumentObjectType == DocumentObjectType.TextRange:
            text += sub.Text or ""
    return text


def _hide_titles_in_section(section, titles: list[str], start_index: int = 0):
    if not section or not titles:
        return
    targets = {_normalize_text(t) for t in titles if _normalize_text(t)}
    if not targets:
        return
    child_objects = section.Body.ChildObjects
    for idx in range(start_index, child_objects.Count):
        obj = child_objects.get_Item(idx)
        if isinstance(obj, Paragraph):
            para_text = _normalize_text(_get_paragraph_text(obj))
            if para_text in targets:
                for j in range(obj.ChildObjects.Count):
                    sub = obj.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        sub.CharacterFormat.Hidden = True


def extract_pdf_chapter_to_table(pdf_folder_path: str, target_section: str, output_doc=None, section=None):
    upper_ratio = 0.1
    lower_ratio = 0.9

    stop_pattern = re.compile(
        r"^\s*(?:\d+\.\d+\.\d+|\d+\.\d+|[A-Z]\.|圖\s*\d+|Fig\.?\s*\d+|Figure\s+\d+)",
        re.IGNORECASE | re.MULTILINE
    )
    section_pattern = re.compile(rf"^\s*\d*\.?\s*{re.escape(target_section)}:?", re.IGNORECASE | re.MULTILINE)
    english_pattern = re.compile(r'^[\x00-\x7F]+$')

    if output_doc is None or section is None:
        output_doc = DocxDocument()
        section = output_doc.add_section()
        table = output_doc.add_table(rows=1, cols=2)
        is_standalone = True
    else:
        table = section.AddTable(True)
        table.ResetCells(1, 2)
        is_standalone = False

    row = table.Rows.get_Item(0)
    cell1 = row.Cells.get_Item(0)
    cell2 = row.Cells.get_Item(1)
    cell1.AddParagraph().AppendText("Packaging test report No.")
    cell2.AddParagraph().AppendText("Rationale for Test Article Selection")
    bg_color = Color.FromRgb(0xBA, 0xE0, 0xD2)
    cell1.CellFormat.BackColor = bg_color
    cell2.CellFormat.BackColor = bg_color

    for filename in os.listdir(pdf_folder_path):
        if not filename.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(pdf_folder_path, filename)
        doc_pdf = fitz.open(pdf_path)
        all_text = []
        for page in doc_pdf:
            width, height = page.rect.width, page.rect.height
            capture_rect = fitz.Rect(0, height * upper_ratio, width, height * lower_ratio)
            blocks = page.get_text("blocks", clip=capture_rect)
            all_text.extend([block[4].strip() for block in blocks if block[4].strip()])
        doc_pdf.close()

        full_text = "\n".join(all_text)
        capture_mode = False
        section_lines = []
        for line in full_text.splitlines():
            if section_pattern.match(line):
                capture_mode = True
                if english_pattern.match(line):
                    section_lines.append(line)
            elif capture_mode and stop_pattern.match(line):
                break
            elif capture_mode and english_pattern.match(line):
                section_lines.append(line)

        extracted_text = " ".join(section_lines).strip()
        if extracted_text:
            match = re.search(r"(UOC|United)", extracted_text, re.IGNORECASE)
            if match:
                extracted_text = extracted_text[:match.end()]
            if not extracted_text.endswith("."):
                extracted_text += "."
        else:
            extracted_text = "（未找到英文內容）"

        new_row = TableRow(output_doc)
        cell1 = TableCell(output_doc)
        cell2 = TableCell(output_doc)
        new_row.Cells.Add(cell1)
        new_row.Cells.Add(cell2)

        table_filename = filename.split(' ')[0]
        cell1.AddParagraph().AppendText(table_filename)
        cell2.AddParagraph().AppendText(extracted_text)
        table.Rows.Add(new_row)

    if is_standalone:
        output_doc.save("pdf_chapter_table.docx")
    print(f"已將PDF章節 {target_section} 擷取至表格")


def extract_word_all_content(
    input_file: str,
    output_image_path: str | None = None,
    output_doc=None,
    section=None,
    *,
    output_docx_path: str | None = None,
    ignore_toc_and_before: bool = True,
    ignore_header_footer: bool = True,
):
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"input file not found: {input_file}")

    out_path = output_docx_path or _build_output_docx_path(input_file, "body")
    extract_body_with_options(
        input_docx=input_file,
        output_docx=out_path,
        ignore_toc_and_before=ignore_toc_and_before,
        ignore_header_footer=ignore_header_footer,
    )

    appended_section = None
    if output_doc is not None:
        target_section = section or output_doc.AddSection()
        _append_docx_to_section(out_path, output_doc, target_section)
        appended_section = target_section

    return {"output_docx": out_path, "section": appended_section}


def _normalize_text(value: str) -> str:
    return " ".join(value.split()) if value else ""


def _detect_image_extension(image_bytes: bytes, default_ext: str = "png") -> str:
    """Return a lowercase file extension based on the image signature."""

    signatures = {
        b"\x89PNG\r\n\x1a\n": "png",
        b"\xff\xd8\xff": "jpg",
        b"GIF87a": "gif",
        b"GIF89a": "gif",
        b"BM": "bmp",
        b"II*\x00": "tif",
        b"MM\x00*": "tif",
        b"\x00\x00\x01\x00": "ico",
    }
    for header, ext in signatures.items():
        if image_bytes.startswith(header):
            return ext
    return default_ext


def _save_picture_with_original_format(
    picture: DocPicture, image_dir: str, image_count: list[int]
) -> str:
    """Persist ``picture.ImageBytes`` using the detected image format.

    Parameters
    ----------
    picture : DocPicture
        The picture to save.
    image_dir : str
        Target directory for the exported image.
    image_count : list[int]
        Mutable counter used to build incrementing file names.

    Returns
    -------
    str
        The saved filename (not the absolute path).
    """

    image_bytes = picture.ImageBytes
    ext = _detect_image_extension(image_bytes)
    file_name = f"Image-{image_count[0]}.{ext}"
    img_path = os.path.join(image_dir, file_name)
    with open(img_path, "wb") as img:
        img.write(image_bytes)
    image_count[0] += 1
    return file_name


def is_heading_paragraph(paragraph: Paragraph) -> bool:
    """Return True when the paragraph uses a Heading style."""
    style_name = getattr(paragraph, "StyleName", "") or ""
    return "heading" in style_name.lower()


def extract_word_chapter(
    input_file: str,
    target_chapter_section: str,
    target_title: bool = False,
    target_title_section: str = "",
    output_image_path: str | None = None,
    output_doc=None,
    section=None,
    *,
    explicit_end_title: str | None = None,
    subheading_text: str | None = None,
    subheading_strict_match: bool = True,
    ignore_header_footer: bool = True,
    ignore_toc: bool = True,
    output_docx_path: str | None = None,
):
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"input file not found: {input_file}")

    heading_text = target_title_section.strip()
    if not heading_text and target_title:
        heading_text = target_chapter_section

    start_heading = heading_text or target_chapter_section
    out_path = output_docx_path or _build_output_docx_path(input_file, f"section_{start_heading}")

    subheading_to_use = subheading_text
    if not subheading_to_use and heading_text:
        subheading_to_use = heading_text

    extract_section_docx_xml(
        input_docx=input_file,
        output_docx=out_path,
        start_heading_text=start_heading,
        start_number=target_chapter_section,
        explicit_end_title=(explicit_end_title or None),
        ignore_header_footer=ignore_header_footer,
        ignore_toc=ignore_toc,
        subheading_text=subheading_to_use,
        subheading_strict_match=subheading_strict_match,
        subheading_debug=False,
    )

    captured_title = _read_first_paragraph_text(out_path) or start_heading
    if captured_title:
        hide_paragraphs_with_text(out_path, [captured_title])

    appended_section = None
    if output_doc is not None:
        target_section = section or output_doc.AddSection()
        start_idx = target_section.Body.ChildObjects.Count
        _append_docx_to_section(out_path, output_doc, target_section)
        _hide_titles_in_section(target_section, [captured_title], start_index=start_idx)
        appended_section = target_section

    result = {"captured_titles": [captured_title] if captured_title else [], "output_docx": out_path}
    return result


def is_inline_subtitle_spire(paragraph: Paragraph) -> bool:
    """判斷這個 Spire 段落是不是像 'Device trade name' 這種小標題。
    規則：
    - 段落有文字
    - StyleName == 'Normal'
    - 所有有文字的 TextRange 都是粗體
    """
    # 1. 取得整段文字
    full_text = ""
    for i in range(paragraph.ChildObjects.Count):
        sub = paragraph.ChildObjects.get_Item(i)
        if sub.DocumentObjectType == DocumentObjectType.TextRange:
            full_text += sub.Text or ""
    if not full_text.strip():
        return False

    # 2. 只處理 Normal 樣式
    style_name = getattr(paragraph, "StyleName", None)
    if style_name != "Normal":
        return False

    # 3. 所有有文字的 run 都要是粗體
    has_text_run = False
    for i in range(paragraph.ChildObjects.Count):
        sub = paragraph.ChildObjects.get_Item(i)
        if sub.DocumentObjectType == DocumentObjectType.TextRange:
            tr: TextRange = sub
            text = (tr.Text or "").strip()
            if not text:
                continue
            has_text_run = True
            # Bold 不是 True 就視為不是小標題
            if tr.CharacterFormat.Bold is not True:
                return False

    return has_text_run

def extract_word_subsection(
    input_file: str,
    outer_chapter_section: str,   # 例如 "1.1.1"
    subsection_title: str,        # 例如 "Principles of operation and mode of action"
    output_image_path: str = "images",
    output_doc=None,
    section=None,):
    """
    從 Word 檔中擷取「指定章節內的某一個小節」內容。

    範圍：
      1. 先進入 outer_chapter_section 章節，例如 1.1.1
      2. 在章節內找到 subsection_title 這一行（Normal + 粗體）
      3. 從小節標題「下一行開始」擷取內容（文字、圖片、表格）
      4. 遇到下一個 inline 小標題（Normal + 粗體）或者下一個章節 (1.1.2...) 就停止
    """

    if not os.path.exists(output_image_path):
        os.makedirs(output_image_path)

    # 章節起始：例如 "1.1.1"
    chapter_pattern = re.compile(rf"^\s*{re.escape(outer_chapter_section)}(\s|$)", re.IGNORECASE)

    # 章節停止：以前綴 "1.1" 找下一個章節 (1.1.2, 1.1.3 ...)
    stop_prefix = outer_chapter_section.rsplit(".", 1)[0]
    chapter_stop_pattern = re.compile(rf"^\s*{re.escape(stop_prefix)}(\.\d+)?(\s|$)", re.IGNORECASE)

    # 小節標題：整行比對文字
    subsection_pattern = re.compile(rf"^\s*{re.escape(subsection_title.strip())}\s*$", re.IGNORECASE)

    input_doc = Document()
    input_doc.LoadFromFile(input_file)

    is_standalone_doc = False
    if output_doc is None or section is None:
        output_doc = Document()
        section = output_doc.AddSection()
        is_standalone_doc = True

    nodes = queue.Queue()
    nodes.put(input_doc)

    image_count = [1]

    in_chapter = False
    in_subsection = False
    skip_first_line_after_title = False
    done = False

    def add_table_to_section(sec, table):
        """將表格複製到輸出文件，避免跨頁斷行問題。"""
        try:
            cloned = table.Clone()
            cloned.TableFormat.IsBreakAcrossPages = False
            for i in range(cloned.Rows.Count):
                cloned.Rows.get_Item(i).RowFormat.IsBreakAcrossPages = False
            sec.Tables.Add(cloned)
            sep_para = sec.AddParagraph()
            sep_para.AppendText("\u200B")
        except Exception as e:
            print("處理表格錯誤:", e)

    while nodes.qsize() > 0 and not done:
        node = nodes.get()
        for i in range(node.ChildObjects.Count):
            child = node.ChildObjects.get_Item(i)

            if isinstance(child, Paragraph):
                # 略過目錄樣式
                if "toc" in child.StyleName.lower() or "目錄" in child.StyleName.lower():
                    continue

                # 組出原段落文字（含 ListText）
                paragraph_text = child.ListText + " " if child.ListText else ""
                for j in range(child.ChildObjects.Count):
                    sub = child.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        paragraph_text += sub.Text or ""

                paragraph_text_stripped = paragraph_text.strip()

                # 1) 判斷是否進入指定章節 1.1.1
                if not in_chapter:
                    if chapter_pattern.match(paragraph_text_stripped) or (
                        child.ListText and chapter_pattern.match(child.ListText.strip())
                    ):
                        in_chapter = True
                    continue

                # 2) 章節內，先看是否遇到下一個章節（1.1.2...），整個擷取結束
                stop_hit = False
                if child.ListText and chapter_stop_pattern.match(child.ListText.strip()):
                    stop_hit = True
                if not stop_hit and paragraph_text_stripped and chapter_stop_pattern.match(paragraph_text_stripped):
                    stop_hit = True

                if stop_hit:
                    in_chapter = False
                    in_subsection = False
                    done = True
                    break

                # 3) 在 1.1.1 內，尚未進入小節 → 尋找目標小節標題
                if in_chapter and not in_subsection:
                    if subsection_pattern.match(paragraph_text_stripped) and is_inline_subtitle_spire(child):
                        in_subsection = True
                        skip_first_line_after_title = True
                    continue

                # 4) 已在小節範圍內
                if in_subsection:
                    # 4-1) 遇到下一個 inline 小標題（Normal + 全粗體，文字不同）→ 結束小節
                    if is_inline_subtitle_spire(child):
                        if _normalize_text(paragraph_text_stripped) != _normalize_text(subsection_title):
                            print("STOP BY NEXT SUBTITLE")
                            print(f"-> text: {paragraph_text_stripped}")
                            print(f"-> style: {child.StyleName}")
                            in_subsection = False
                            done = True
                            break
                        else:
                            # 再遇到同標題，保守跳過
                            continue

                    # 4-2) 小節標題下一行開始才真正輸出內容
                    paragraph_alignment = getattr(child.Format, "HorizontalAlignment", None)

                    # 建立一個文字＋圖片標記的字串
                    text_with_markers = ""
                    for j in range(child.ChildObjects.Count):
                        sub = child.ChildObjects.get_Item(j)
                        if sub.DocumentObjectType == DocumentObjectType.TextRange:
                            text_with_markers += sub.Text or ""
                        elif sub.DocumentObjectType == DocumentObjectType.Picture and isinstance(sub, DocPicture):
                            img_name = _save_picture_with_original_format(
                                sub, output_image_path, image_count
                            )
                            text_with_markers += f"[Image: {img_name}|{sub.Width}|{sub.Height}]"

                    # 小節標題下一行的第一個內容段落也要輸出
                    if skip_first_line_after_title:
                        skip_first_line_after_title = False

                    if text_with_markers.strip():
                        para = section.AddParagraph()
                        # 拆解 [Image: ...] 標記，把圖片塞回去
                        for part in re.split(r'(\[Image:.+?\])', text_with_markers):
                            if part.startswith("[Image:"):
                                try:
                                    content = part[7:-1]
                                    img_name, width, height = content.split("|")
                                    width = float(width)
                                    height = float(height)
                                except ValueError:
                                    img_name = part[7:-1].strip()
                                    width = height = None
                                img_path = os.path.join(output_image_path, img_name.strip())
                                if os.path.isfile(img_path):
                                    pic = para.AppendPicture(img_path)
                                    if width and height:
                                        pic.Width = width
                                        pic.Height = height
                                    if paragraph_alignment is not None:
                                        para.Format.HorizontalAlignment = paragraph_alignment
                            else:
                                if part:
                                    para.AppendText(part)

            elif isinstance(child, Table):
                # 表格只有在小節範圍內才複製
                if in_subsection:
                    add_table_to_section(section, child)
            elif isinstance(child, Section):
                nodes.put(child.Body)
            elif isinstance(child, ICompositeObject):
                doc_type = getattr(child, "DocumentObjectType", None)
                # if doc_type in (DocumentObjectType.Header, DocumentObjectType.Footer):
                #     continue
                if _is_header_or_footer(doc_type):
                    continue
                nodes.put(child)

    if is_standalone_doc:
        output_doc.SaveToFile("word_subsection_result.docx", FileFormat.Docx)
        input_doc.Close()
    else:
        input_doc.Close()

    print(f"已擷取 {outer_chapter_section} 中小節「{subsection_title}」的文字與圖片")

def center_table_figure_paragraphs(input_file: str) -> bool:
    pattern = re.compile(r'^\s*(Table|Figure)\s+', re.IGNORECASE)
    doc = Document()
    try:
        doc.LoadFromFile(input_file)
    except Exception as e:
        print(f"錯誤：無法加載文件 {input_file}: {str(e)}")
        return False

    nodes = queue.Queue()
    nodes.put(doc)

    while nodes.qsize() > 0:
        node = nodes.get()
        for i in range(node.ChildObjects.Count):
            child = node.ChildObjects.get_Item(i)
            if isinstance(child, Paragraph):
                if "toc" in child.StyleName.lower() or "目錄" in child.StyleName.lower():
                    continue
                paragraph_text = ""
                if child.ListText:
                    paragraph_text += child.ListText + " "
                for j in range(child.ChildObjects.Count):
                    sub = child.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        paragraph_text += sub.Text
                paragraph_text = paragraph_text.strip()
                if pattern.match(paragraph_text):
                    child.Format.HorizontalAlignment = HorizontalAlignment.Center
            elif isinstance(child, ICompositeObject):
                nodes.put(child)

    try:
        doc.SaveToFile(input_file, FileFormat.Docx)
        print(f"{input_file}，以將表格標題或圖片標題置中")
        return True
    except Exception as e:
        print(f"錯誤：保存文件 {input_file} 時出錯: {str(e)}")
        return False
    finally:
        doc.Close()


def _iter_paragraphs(parent):
    """Yield paragraphs in parent recursively, including those in tables.""" 
    if hasattr(parent, "paragraphs"):
        for p in parent.paragraphs:
            yield p
    if hasattr(parent, "tables"):
        for table in parent.tables:
            for row in table.rows:
                for cell in row.cells:
                    yield from _iter_paragraphs(cell)

def remove_hidden_runs(
    input_file: str,
    preserve_texts: Optional[Iterable[str]] = None,
) -> bool:
    """Remove runs marked as hidden and drop empty paragraphs without losing images."""
    try:
        doc = DocxDocument(input_file)
        preserve_set = {
            _normalize_text(t)
            for t in (preserve_texts or [])
            if isinstance(t, str) and _normalize_text(t)
        }
        for para in list(_iter_paragraphs(doc)):
            normalized_para_text = _normalize_text(para.text)
            if preserve_set and normalized_para_text in preserve_set:
                continue
            to_remove = [run for run in para.runs if run.font.hidden]
            for run in to_remove:
                para._element.remove(run._element)
            has_image = bool(
                para._element.xpath(
                    './/w:drawing | .//w:pict'
                )
            )
            if not para.text.strip() and not has_image:
                if preserve_set and normalized_para_text in preserve_set:
                    continue
                parent = para._element.getparent()
                if parent is not None and parent.tag == qn('w:tc'):
                    # Ensure each table cell keeps at least one paragraph
                    paragraph_count = len(parent.findall(qn('w:p')))
                    if paragraph_count <= 1:
                        continue
                p = para._element
                p.getparent().remove(p)
        doc.save(input_file)
        return True
    except Exception as e:
        print(f"錯誤：移除隱藏文字 {input_file} 時出錯: {str(e)}")
        return False


def hide_paragraphs_with_text(
    input_file: str,
    texts_to_hide: Iterable[str],
) -> bool:
    """Mark paragraphs whose text matches any provided strings as hidden."""
    cleaned = [
        t.strip()
        for t in texts_to_hide
        if isinstance(t, str) and t.strip()
    ]
    if not cleaned:
        return True
    targets = {_normalize_text(t) for t in cleaned}
    try:
        doc = DocxDocument(input_file)
        for para in _iter_paragraphs(doc):
            if _normalize_text(para.text) in targets:
                for run in para.runs:
                    run.font.hidden = True
        doc.save(input_file)
        return True
    except Exception as e:
        print(f"錯誤：隱藏段落於 {input_file} 時出錯: {str(e)}")
        return False


def remove_paragraphs_with_text(
    input_file: str,
    texts_to_remove: Iterable[str],
) -> bool:
    """Remove paragraphs whose text matches any provided strings.

    Paragraphs inside table cells keep an empty placeholder if they would be
    the last remaining paragraph to avoid corrupting the table structure.
    """

    cleaned = [
        t.strip()
        for t in texts_to_remove
        if isinstance(t, str) and t.strip()
    ]
    if not cleaned:
        return True

    targets = {_normalize_text(t) for t in cleaned}
    try:
        doc = DocxDocument(input_file)
        for para in list(_iter_paragraphs(doc)):
            if _normalize_text(para.text) not in targets:
                continue

            parent = para._element.getparent()
            if parent is not None and parent.tag == qn('w:tc'):
                paragraph_count = len(parent.findall(qn('w:p')))
                if paragraph_count <= 1:
                    for run in list(para.runs):
                        para._element.remove(run._element)
                    continue

            element = para._element
            container = element.getparent()
            if container is not None:
                container.remove(element)

        doc.save(input_file)
        return True
    except Exception as e:
        print(f"錯誤：移除段落於 {input_file} 時出錯: {str(e)}")
        return False


def apply_basic_style(
    input_file: str,
    western_font: str = "Times New Roman",
    east_asian_font: str = "新細明體",
    font_size: int = 12,
    line_spacing: float = 1.5,
    space_before: int = 6,
    space_after: int = 6,
) -> bool:
    """為整份文件套用基本字型與行距設定。"""
    try:
        doc = DocxDocument(input_file)
        for para in _iter_paragraphs(doc):
            pf = para.paragraph_format
            pf.line_spacing_rule = WD_LINE_SPACING.MULTIPLE
            pf.line_spacing = line_spacing
            pf.space_before = Pt(space_before)
            pf.space_after = Pt(space_after)
            for run in para.runs:
                run.font.name = western_font
                set_run_font_eastasia(run, east_asian_font)
                run.font.size = Pt(font_size)
        doc.save(input_file)
        return True
    except Exception as e:
        print(f"錯誤：套用樣式至 {input_file} 時出錯: {str(e)}")
        return False
    

def _is_header_or_footer(doc_type) -> bool:
    header = getattr(DocumentObjectType, "Header", None)
    footer = getattr(DocumentObjectType, "Footer", None)
    header_footer = getattr(DocumentObjectType, "HeaderFooter", None)
    return doc_type in (header, footer, header_footer)


def extract_specific_figure_from_word(
    input_file: str,
    target_chapter_section: str,   # 例如 "2.1.1"
    target_figure_label: str,      # 例如 "Figure 1."
    target_subtitle: str | None = None,  # 可選，有就填，沒有就 None
    output_image_path: str = "figure_output",
    output_doc=None,
    section=None,
):
    """
    從指定 Word 檔中，擷取「某章節」(可選擇是否限制到某小節) 裡，
    對應指定 Figure caption 的那一張圖。

    參數說明
    ----------
    input_file : str
        要處理的 Word 檔路徑。
    target_chapter_section : str
        章節編號，例如 "2.1.1"。用來鎖定章節範圍。
    target_figure_label : str
        要找的 Figure 標題文字，例如 "Figure 1."。
    target_subtitle : str 或 None
        若有特定小節標題，例如 "Information on product label"，就填入；
        若整個章節沒有小標題或不想限定，就給 None 或空字串。
    output_image_path : str
        圖片輸出資料夾路徑。
    output_doc : spire.doc.Document, optional
        若有傳入，會將找到的圖片插入到這份 Document 的 section 中。
    section : spire.doc.Section, optional
        搭配 output_doc 使用，指定插入圖片的 section。

    回傳
    ----------
    dict 或 None
        若成功，回傳:
        {
            "image_filename": "Image-1.png",
            "caption": "Figure 1. xxx",
        }
        若找不到，回傳 None。
    """

    if not os.path.exists(output_image_path):
        os.makedirs(output_image_path)

    # 章節起始與停止條件
    section_pattern = re.compile(rf"^\s*{re.escape(target_chapter_section)}(\s|$)", re.IGNORECASE)
    stop_prefix = target_chapter_section.rsplit('.', 1)[0]
    stop_pattern = re.compile(rf"^\s*{re.escape(stop_prefix)}(\.\d+)?(\s|$)", re.IGNORECASE)

    # 小節標題 (可選)
    if target_subtitle and target_subtitle.strip():
        subtitle_pattern = re.compile(rf"^\s*{re.escape(target_subtitle.strip())}\s*$", re.IGNORECASE)
    else:
        subtitle_pattern = None  # 不限制小節

    # Figure caption
    figure_pattern = re.compile(rf"^\s*{re.escape(target_figure_label)}", re.IGNORECASE)

    input_doc = Document()
    input_doc.LoadFromFile(input_file)

    is_standalone_doc = False
    if output_doc is None or section is None:
        output_doc = Document()
        section = output_doc.AddSection()
        is_standalone_doc = True

    nodes = queue.Queue()
    nodes.put(input_doc)

    image_count = [1]

    in_target_chapter = False
    in_target_subtitle = False  # 如果沒有 subtitle，會在進入章節時直接視為 True

    recent_pictures = []
    result = None

    def save_picture_and_record(pic: DocPicture) -> str:
        file_name = _save_picture_with_original_format(pic, output_image_path, image_count)
        return file_name

    while nodes.qsize() > 0 and result is None:
        node = nodes.get()
        for i in range(node.ChildObjects.Count):
            child = node.ChildObjects.get_Item(i)

            if isinstance(child, Paragraph):
                paragraph_text = child.ListText + " " if child.ListText else ""
                for j in range(child.ChildObjects.Count):
                    sub = child.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        paragraph_text += sub.Text

                paragraph_text_stripped = paragraph_text.strip()

                # 1) 章節開頭
                if section_pattern.match(paragraph_text_stripped):
                    in_target_chapter = True
                    recent_pictures.clear()
                    # 若沒有指定 subtitle，整個章節都視為有效範圍
                    if subtitle_pattern is None:
                        in_target_subtitle = True
                    else:
                        in_target_subtitle = False
                    continue

                # 2) 超出章節範圍
                if in_target_chapter and child.ListText and stop_pattern.match(child.ListText):
                    in_target_chapter = False
                    in_target_subtitle = False
                    recent_pictures.clear()
                    continue

                # 3) 有指定 subtitle 的情況：在章節內遇到目標小節標題才開始算
                if in_target_chapter and subtitle_pattern is not None and subtitle_pattern.match(paragraph_text_stripped):
                    in_target_subtitle = True
                    recent_pictures.clear()
                    continue

                # 4) 若有 subtitle，就簡單用「看起來像新標題」來判斷離開小節
                if in_target_chapter and subtitle_pattern is not None and in_target_subtitle:
                    if paragraph_text_stripped and (
                        section_pattern.match(paragraph_text_stripped)
                        or re.match(r'^\s*\d+(\.\d+)*\s+', paragraph_text_stripped)
                    ):
                        in_target_subtitle = False
                        recent_pictures.clear()
                        continue

                # 5) 在有效範圍內處理圖片與 Figure caption
                if in_target_chapter and in_target_subtitle:
                    # 先抓圖
                    for j in range(child.ChildObjects.Count):
                        sub = child.ChildObjects.get_Item(j)
                        if sub.DocumentObjectType == DocumentObjectType.Picture and isinstance(sub, DocPicture):
                            img_file = save_picture_and_record(sub)
                            recent_pictures.append(img_file)

                    # 再判斷是不是目標 Figure caption
                    if figure_pattern.match(paragraph_text_stripped) and recent_pictures:
                        target_img = recent_pictures[-1]
                        img_path = os.path.join(output_image_path, target_img)

                        if os.path.isfile(img_path):
                            para = section.AddParagraph()
                            pic = para.AppendPicture(img_path)

                            cap_para = section.AddParagraph()
                            cap_para.AppendText(paragraph_text_stripped)

                        result = {
                            "image_filename": target_img,
                            "caption": paragraph_text_stripped,
                        }
                        break

            elif isinstance(child, Table):
                nodes.put(child)
            elif isinstance(child, Section):
                nodes.put(child.Body)
            elif isinstance(child, ICompositeObject):
                doc_type = getattr(child, "DocumentObjectType", None)
                # if doc_type in (DocumentObjectType.Header, DocumentObjectType.Footer):
                #     continue
                if _is_header_or_footer(doc_type):
                    continue
                nodes.put(child)

    input_doc.Close()

    if result is None:
        print("未在指定章節範圍內找到對應的 Figure 圖片。")
    else:
        print(f"已擷取圖片 {result['image_filename']}，對應 caption: {result['caption']}")

    return result
