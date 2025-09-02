import re
from collections import defaultdict
from io import BytesIO
from typing import DefaultDict, List
from zipfile import ZipFile

from xml.etree import ElementTree as ET


def renumber_figures_tables(docx_path: str) -> None:
    """Renumber figure and table captions and update references in-place.

    This implementation edits the ``document.xml`` part directly rather than
    loading the document through :mod:`python-docx`.  Doing so avoids the loss of
    images that can occur when saving a document with unsupported drawing types.
    The rest of the package parts (media, relationships, etc.) are preserved
    verbatim.
    """

    with ZipFile(docx_path, "r") as zin:
        document_xml = zin.read("word/document.xml")

    # Preserve existing namespace prefixes when writing the XML back
    namespaces = {}
    for event, elem in ET.iterparse(BytesIO(document_xml), events=("start-ns",)):
        prefix, uri = elem
        namespaces[prefix] = uri
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    tree = ET.fromstring(document_xml)
    ns = {"w": namespaces.get("w", "http://schemas.openxmlformats.org/wordprocessingml/2006/main")}

    caption_pattern = re.compile(r"^(Figure|Fig\.?|Table)\s*(\d+)", re.IGNORECASE)
    fig_map: DefaultDict[str, List[str]] = defaultdict(list)
    table_map: DefaultDict[str, List[str]] = defaultdict(list)
    fig_counter = 1
    table_counter = 1

    # First pass: update caption numbers and build mapping of old->new numbers
    for p in tree.findall(".//w:p", ns):
        texts = p.findall(".//w:t", ns)
        full_text = "".join(t.text or "" for t in texts).strip()
        match = caption_pattern.match(full_text)
        if not match:
            continue
        label, old_num = match.group(1), match.group(2)
        if label.lower().startswith("fig"):
            new_num = str(fig_counter)
            fig_map[old_num].append(new_num)
            fig_counter += 1
        else:
            new_num = str(table_counter)
            table_map[old_num].append(new_num)
            table_counter += 1
        for t in texts:
            if t.text and old_num in t.text:
                t.text = t.text.replace(old_num, new_num, 1)
                break

    ref_pattern = re.compile(r"(Figure|Fig\.?|Table)\s*(\d+)", re.IGNORECASE)
    for t in tree.findall(".//w:t", ns):
        if not t.text:
            continue

        def _repl(m: re.Match) -> str:
            label, num = m.group(1), m.group(2)
            if re.match(r"Fig\.?|Figure", label, re.IGNORECASE):
                nums = fig_map.get(num)
            else:
                nums = table_map.get(num)
            if nums:
                new_num = nums[0]
                if len(nums) > 1:
                    nums.pop(0)
                return f"{label} {new_num}"
            return m.group(0)

        t.text = ref_pattern.sub(_repl, t.text)

    new_xml = ET.tostring(tree, encoding="utf-8", xml_declaration=True)

    # Write the modified document.xml back while preserving other parts
    with ZipFile(docx_path, "r") as zin:
        filelist = zin.infolist()
        buffer = BytesIO()
        with ZipFile(buffer, "w") as zout:
            for item in filelist:
                if item.filename == "word/document.xml":
                    zout.writestr(item, new_xml)
                else:
                    zout.writestr(item, zin.read(item.filename))

    with open(docx_path, "wb") as f:
        f.write(buffer.getvalue())

