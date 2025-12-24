from docx import Document
from docxcompose.composer import Composer
from typing import List


def merge_word_docs(file_list: List[str], output_path: str) -> None:
    """
    Merge multiple DOCX files in order using docxcompose.

    Parameters
    ----------
    file_list : list[str]
        File paths to merge in order.
    output_path : str
        Destination path for the merged DOCX.
    """
    if not file_list:
        return

    master_doc = Document(file_list[0])
    composer = Composer(master_doc)

    for path in file_list[1:]:
        composer.append(Document(path))

    composer.save(output_path)
