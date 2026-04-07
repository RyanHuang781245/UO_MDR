import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.docx_toc import extract_toc_entries, toc_entries_as_json


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract DOCX table-of-contents entries.")
    parser.add_argument("docx_path", help="Path to the DOCX file")
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Output compact JSON without indentation",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    docx_path = Path(args.docx_path)
    if not docx_path.is_file():
        parser.error(f"file not found: {docx_path}")

    entries = extract_toc_entries(docx_path)
    print(toc_entries_as_json(entries, pretty=not args.compact))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
