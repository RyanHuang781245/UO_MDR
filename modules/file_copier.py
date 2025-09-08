import os
import shutil
from typing import Iterable, List


def copy_files(source: str, destination: str, keywords: Iterable[str]) -> List[str]:
    """Copy files whose names contain all provided keywords.

    Keywords are matched case-insensitively. A file is copied only when its
    name contains *all* of the specified keywords. If a file with the same
    name already exists in the destination directory, it will be overwritten.

    Parameters
    ----------
    source: str
        Directory to search for files.
    destination: str
        Directory where matched files will be copied.
    keywords: Iterable[str]
        Keywords that must all be present in the filename. For example,
        passing ["Shipping simulation test", "EO"] will match only files whose
        names contain both phrases.

    Returns
    -------
    List[str]
        Paths of the copied files in the destination directory.
    """
    if not os.path.isdir(source):
        raise ValueError(f"Source directory '{source}' does not exist")

    os.makedirs(destination, exist_ok=True)
    copied_files: List[str] = []
    lowered_keywords = [k.strip().lower() for k in keywords if k.strip()]

    for root, _dirs, files in os.walk(source):
        for name in files:
            lower_name = name.lower()
            if all(k in lower_name for k in lowered_keywords):
                src_path = os.path.join(root, name)
                dest_path = os.path.join(destination, name)
                shutil.copy2(src_path, dest_path)
                copied_files.append(dest_path)
    return copied_files


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Copy files whose names contain keywords")
    parser.add_argument("source", help="Directory to search")
    parser.add_argument("destination", help="Directory to copy files to")
    parser.add_argument(
        "keywords",
        help="Comma-separated keywords that must all appear in the filename",
    )
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    results = copy_files(args.source, args.destination, keywords)
    print(f"Copied {len(results)} file(s).")
