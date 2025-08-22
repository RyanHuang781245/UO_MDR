import os
import re
import queue
import fitz  # PyMuPDF
from docx import Document as DocxDocument
from spire.doc import *
from spire.doc.common import *


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


def extract_word_all_content(input_file: str, output_image_path: str = "word_all_images", output_doc=None, section=None):
    if not os.path.exists(output_image_path):
        os.makedirs(output_image_path)

    input_doc = Document()
    input_doc.LoadFromFile(input_file)

    if output_doc is None or section is None:
        output_doc = Document()
        section = output_doc.AddSection()
        is_standalone = True
    else:
        is_standalone = False

    nodes = queue.Queue()
    nodes.put(input_doc)
    image_count = [1]

    def add_table_to_section(sec, table):
        try:
            cloned = table.Clone()
            sec.Tables.Add(cloned)
        except Exception as e:
            print("處理表格錯誤:", e)

    while nodes.qsize() > 0:
        node = nodes.get()
        for i in range(node.ChildObjects.Count):
            child = node.ChildObjects.get_Item(i)
            if isinstance(child, Paragraph):
                if "toc" in child.StyleName.lower() or "目錄" in child.StyleName.lower():
                    continue
                paragraph_text = ""
                for j in range(child.ChildObjects.Count):
                    sub = child.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        paragraph_text += sub.Text
                    elif sub.DocumentObjectType == DocumentObjectType.Picture and isinstance(sub, DocPicture):
                        file_name = f"Image-{image_count[0]}.png"
                        img_path = os.path.join(output_image_path, file_name)
                        with open(img_path, 'wb') as img:
                            img.write(sub.ImageBytes)
                        paragraph_text += f"[Image: {file_name}]"
                        image_count[0] += 1
                if paragraph_text.strip():
                    para = section.AddParagraph()
                    if child.ListFormat.ListType != ListType.NoList:
                        para.ListFormat.ListType = child.ListFormat.ListType
                        para.ListFormat.ListLevelNumber = child.ListFormat.ListLevelNumber
                        para.ListFormat.ContinueListNumbering()
                    for part in re.split(r'(\[Image:.+?\])', paragraph_text):
                        if part.startswith("[Image:"):
                            img_name = part[7:-1].strip()
                            img_path = os.path.join(output_image_path, img_name)
                            if os.path.isfile(img_path):
                                para.AppendPicture(img_path)
                                para.Format.HorizontalAlignment = HorizontalAlignment.Center
                        else:
                            para.AppendText(part)
            elif isinstance(child, Table):
                add_table_to_section(section, child)
            elif isinstance(child, ICompositeObject):
                nodes.put(child)

    if is_standalone:
        output_doc.SaveToFile("word_all_result.docx", FileFormat.Docx)
    input_doc.Close()
    print(f"已將所有內容擷取")


def extract_word_chapter(input_file: str, target_chapter_section: str, target_title=False, target_title_section="", output_image_path="images", output_doc=None, section=None):
    if not os.path.exists(output_image_path):
        os.makedirs(output_image_path)

    if target_title and target_title_section:
        section_pattern = re.compile(rf"^\s*{re.escape(target_title_section)}\s*$", re.IGNORECASE)
    else:
        section_pattern = re.compile(rf"^\s*{re.escape(target_chapter_section)}(\s|$)", re.IGNORECASE)
    stop_prefix = target_chapter_section.rsplit('.', 1)[0]
    stop_pattern = re.compile(rf"^\s*{re.escape(stop_prefix)}(\.\d+)?(\s|$)", re.IGNORECASE)

    input_doc = Document()
    input_doc.LoadFromFile(input_file)

    if output_doc is None or section is None:
        output_doc = Document()
        section = output_doc.AddSection()
        is_standalone = True
    else:
        is_standalone = False

    nodes = queue.Queue()
    nodes.put(input_doc)
    image_count = [1]
    capture_mode = False

    def add_table_to_section(sec, table):
        try:
            cloned = table.Clone()
            sec.Tables.Add(cloned)
        except Exception as e:
            print("處理表格錯誤:", e)

    while nodes.qsize() > 0:
        node = nodes.get()
        for i in range(node.ChildObjects.Count):
            child = node.ChildObjects.get_Item(i)
            if isinstance(child, Paragraph):
                if "toc" in child.StyleName.lower() or "目錄" in child.StyleName.lower():
                    continue
                raw_text = child.ListText + " " if child.ListText else ""
                paragraph_text = ""
                for j in range(child.ChildObjects.Count):
                    sub = child.ChildObjects.get_Item(j)
                    if sub.DocumentObjectType == DocumentObjectType.TextRange:
                        raw_text += sub.Text
                        paragraph_text += sub.Text
                    elif sub.DocumentObjectType == DocumentObjectType.Picture and isinstance(sub, DocPicture) and capture_mode:
                        file_name = f"Image-{image_count[0]}.png"
                        img_path = os.path.join(output_image_path, file_name)
                        with open(img_path, 'wb') as img:
                            img.write(sub.ImageBytes)
                        paragraph_text += f"[Image: {file_name}]"
                        image_count[0] += 1
                raw_text = raw_text.strip()
                paragraph_text = paragraph_text.strip()
                if section_pattern.match(raw_text):
                    capture_mode = True
                    continue
                elif capture_mode and child.ListText and stop_pattern.match(child.ListText):
                    capture_mode = False
                if capture_mode and paragraph_text:
                    para = section.AddParagraph()
                    if child.ListFormat.ListType != ListType.NoList:
                        para.ListFormat.ListType = child.ListFormat.ListType
                        para.ListFormat.ListLevelNumber = child.ListFormat.ListLevelNumber
                        para.ListFormat.ContinueListNumbering()
                    for part in re.split(r'(\[Image:.+?\])', paragraph_text):
                        if part.startswith("[Image:"):
                            img_name = part[7:-1].strip()
                            img_path = os.path.join(output_image_path, img_name)
                            if os.path.isfile(img_path):
                                para.AppendPicture(img_path)
                                para.Format.HorizontalAlignment = HorizontalAlignment.Center
                        else:
                            para.AppendText(part)
            elif isinstance(child, Table) and capture_mode:
                add_table_to_section(section, child)
            elif isinstance(child, ICompositeObject):
                nodes.put(child)

    if is_standalone:
        output_doc.SaveToFile("word_chapter_result.docx", FileFormat.Docx)
    input_doc.Close()
    print(f"以將章節 {target_chapter_section} 擷取")

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
