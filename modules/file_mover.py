import os
import shutil
from typing import Iterable, List


def move_files(source: str, destination: str, keywords: Iterable[str]) -> List[str]:
    """Move files whose names contain all provided keywords.

    Keywords are matched case-insensitively. A file is moved only when its
    name contains *all* of the specified keywords.

    Parameters
    ----------
    source: str
        Directory to search for files.
    destination: str
        Directory where matched files will be moved.
    keywords: Iterable[str]
        Keywords that must all be present in the filename.

    Returns
    -------
    List[str]
        Paths of the files after they have been moved.
    """
    if not os.path.isdir(source):
        raise ValueError(f"Source directory '{source}' does not exist")

    os.makedirs(destination, exist_ok=True)
    moved_files: List[str] = []
    lowered_keywords = [k.strip().lower() for k in keywords if k.strip()]

    for root, _dirs, files in os.walk(source):
        for name in files:
            lower_name = name.lower()
            if all(k in lower_name for k in lowered_keywords):
                src_path = os.path.join(root, name)
                dest_path = os.path.join(destination, name)
                base, ext = os.path.splitext(name)
                count = 1
                while os.path.exists(dest_path):
                    dest_path = os.path.join(destination, f"{base}_{count}{ext}")
                    count += 1
                shutil.move(src_path, dest_path)
                moved_files.append(dest_path)
    return moved_files


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Move files whose names contain keywords")
    parser.add_argument("source", help="Directory to search")
    parser.add_argument("destination", help="Directory to move files to")
    parser.add_argument(
        "keywords",
        help="Comma-separated keywords that must all appear in the filename",
    )
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    results = move_files(args.source, args.destination, keywords)
    print(f"Moved {len(results)} file(s).")
