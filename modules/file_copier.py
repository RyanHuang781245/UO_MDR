import os
import re
import shutil
from typing import Any, Dict, Iterable, List


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
    dest_abs = os.path.abspath(destination)

    for root, dirs, files in os.walk(source):
        # Avoid walking into the destination directory to prevent copying files
        # onto themselves when the destination lives inside the source.
        dirs[:] = [
            d for d in dirs if os.path.abspath(os.path.join(root, d)) != dest_abs
        ]

        for name in files:
            lower_name = name.lower()
            if all(k in lower_name for k in lowered_keywords):
                src_path = os.path.join(root, name)
                dest_path = os.path.join(destination, name)
                # Skip copying when the source and destination resolve to the
                # same file (e.g., destination inside source and already visited)
                if os.path.abspath(src_path) == os.path.abspath(dest_path):
                    continue
                shutil.copy2(src_path, dest_path)
                copied_files.append(dest_path)
    return copied_files


_GENERIC_SUFFIX_TOKENS = {
    "a",
    "an",
    "and",
    "by",
    "for",
    "from",
    "ii",
    "iii",
    "information",
    "input",
    "inputs",
    "manufacturer",
    "of",
    "path",
    "section",
    "supplied",
    "system",
    "task",
    "td",
    "test",
    "the",
    "to",
    "ustar",
}


def _tokenize_component(value: str) -> list[str]:
    parts = re.split(r"[^0-9A-Za-z]+", (value or "").lower())
    return [p for p in parts if p]


def _meaningful_tokens(value: str, folder_name: str) -> list[str]:
    folder_tokens = set(_tokenize_component(folder_name))
    tokens = []
    for token in _tokenize_component(value):
        if token in folder_tokens:
            continue
        if token in _GENERIC_SUFFIX_TOKENS:
            continue
        if token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _fallback_suffix(source: str, folder_name: str) -> str | None:
    parts = list(os.path.normpath(source).split(os.sep))
    for component in reversed(parts[:-1]):
        tokens = _meaningful_tokens(component, folder_name)
        if tokens:
            return tokens[-1]
    return None


def _infer_conflict_suffix(source: str, other_source: str, folder_name: str) -> str | None:
    source_parts = list(os.path.normpath(source).split(os.sep))[:-1]
    other_parts = list(os.path.normpath(other_source).split(os.sep))[:-1]

    max_common = min(len(source_parts), len(other_parts))
    differing_pairs = []
    for idx in range(max_common):
        if source_parts[idx] != other_parts[idx]:
            differing_pairs.append((source_parts[idx], other_parts[idx]))
    if len(source_parts) != len(other_parts):
        tail_len = max(len(source_parts), len(other_parts))
        for idx in range(max_common, tail_len):
            left = source_parts[idx] if idx < len(source_parts) else ""
            right = other_parts[idx] if idx < len(other_parts) else ""
            differing_pairs.append((left, right))

    for left, right in reversed(differing_pairs):
        left_tokens = _meaningful_tokens(left, folder_name)
        right_tokens = _meaningful_tokens(right, folder_name)
        if left_tokens and left_tokens != right_tokens:
            return left_tokens[-1]

    return _fallback_suffix(source, folder_name)


def _build_target_path(destination: str, folder_name: str, suffix: str | None) -> str:
    safe_suffix = re.sub(r"[^0-9A-Za-z]+", "_", (suffix or "").strip("_ ").lower()).strip("_")
    final_name = folder_name if not safe_suffix else f"{folder_name}_{safe_suffix}"
    return os.path.join(destination, final_name)


def _build_file_target_path(destination: str, filename: str, suffix: str | None) -> str:
    stem, ext = os.path.splitext(filename)
    safe_suffix = re.sub(r"[^0-9A-Za-z]+", "_", (suffix or "").strip("_ ").lower()).strip("_")
    final_name = filename if not safe_suffix else f"{stem}_{safe_suffix}{ext}"
    return os.path.join(destination, final_name)


def _dedupe_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    parent = os.path.dirname(path)
    name = os.path.basename(path)
    index = 2
    while True:
        candidate = os.path.join(parent, f"{name}_{index}")
        if not os.path.exists(candidate):
            return candidate
        index += 1


def copy_directory(
    source: str,
    destination: str,
    target_name: str | None = None,
    copied_registry: Dict[str, Dict[str, Any]] | None = None,
    registry_entry: Dict[str, Any] | None = None,
) -> str:
    """Copy a directory into the destination directory, preserving its name."""
    if not os.path.isdir(source):
        raise ValueError(f"Source directory '{source}' does not exist")

    os.makedirs(destination, exist_ok=True)

    source_abs = os.path.abspath(source)
    destination_abs = os.path.abspath(destination)
    folder_name = (target_name or "").strip() or os.path.basename(os.path.normpath(source_abs)) or "copied_folder"
    target_abs = os.path.abspath(os.path.join(destination_abs, folder_name))

    if target_abs == source_abs or target_abs.startswith(source_abs + os.sep):
        raise ValueError("Destination cannot be the same as or inside the source directory")

    registry = copied_registry if copied_registry is not None else {}
    existing_info = registry.get(target_abs) or {}
    existing_source = existing_info.get("source") if isinstance(existing_info, dict) else str(existing_info)

    if os.path.exists(target_abs):
        if existing_source and os.path.abspath(existing_source) != source_abs:
            existing_suffix = _infer_conflict_suffix(existing_source, source_abs, folder_name)
            renamed_existing = _dedupe_path(
                _build_target_path(destination_abs, folder_name, existing_suffix)
            )
            if os.path.abspath(renamed_existing) != target_abs:
                shutil.move(target_abs, renamed_existing)
                registry.pop(target_abs, None)
                registry[os.path.abspath(renamed_existing)] = existing_info

            current_suffix = _infer_conflict_suffix(source_abs, existing_source, folder_name)
            target_abs = _dedupe_path(
                _build_target_path(destination_abs, folder_name, current_suffix)
            )
        else:
            target_abs = _dedupe_path(
                _build_target_path(destination_abs, folder_name, _fallback_suffix(source_abs, folder_name))
            )

    shutil.copytree(source_abs, target_abs, dirs_exist_ok=True)
    info = dict(registry_entry or {})
    info["source"] = source_abs
    registry[os.path.abspath(target_abs)] = info
    return target_abs


def copy_directories(
    source: str,
    destination: str,
    keywords: Iterable[str],
    copied_registry: Dict[str, Dict[str, Any]] | None = None,
    registry_entry_factory=None,
) -> List[str]:
    """Copy directories whose basenames contain all provided keywords."""
    if not os.path.isdir(source):
        raise ValueError(f"Source directory '{source}' does not exist")

    lowered_keywords = [k.strip().lower() for k in keywords if k and k.strip()]
    if not lowered_keywords:
        entry = registry_entry_factory(source) if callable(registry_entry_factory) else None
        return [copy_directory(source, destination, None, copied_registry, entry)]

    source_abs = os.path.abspath(source)
    destination_abs = os.path.abspath(destination)
    os.makedirs(destination_abs, exist_ok=True)
    matched_sources: List[str] = []

    source_name = os.path.basename(os.path.normpath(source_abs)).lower()
    source_matches = all(k in source_name for k in lowered_keywords)
    destination_inside_source = (
        destination_abs == source_abs
        or destination_abs.startswith(source_abs + os.sep)
    )

    for root, dirs, _files in os.walk(source_abs):
        dirs.sort()
        if destination_inside_source:
            dirs[:] = [
                d
                for d in dirs
                if os.path.abspath(os.path.join(root, d)) != destination_abs
                and not os.path.abspath(os.path.join(root, d)).startswith(destination_abs + os.sep)
            ]
        for name in list(dirs):
            lower_name = name.lower()
            if not all(k in lower_name for k in lowered_keywords):
                continue
            src_path = os.path.join(root, name)
            matched_sources.append(os.path.abspath(src_path))
            entry = registry_entry_factory(src_path) if callable(registry_entry_factory) else None
            copy_directory(
                src_path,
                destination_abs,
                None,
                copied_registry,
                entry,
            )

    registry = copied_registry if copied_registry is not None else {}
    copied_dirs: List[str] = []
    for src_path in matched_sources:
        final_path = next(
            (
                target_path
                for target_path, info in registry.items()
                if isinstance(info, dict) and os.path.abspath(str(info.get("source") or "")) == src_path
            ),
            None,
        )
        if final_path:
            copied_dirs.append(final_path)

    if not copied_dirs and source_matches:
        entry = registry_entry_factory(source_abs) if callable(registry_entry_factory) else None
        copied_path = copy_directory(
            source_abs,
            destination_abs,
            None,
            copied_registry,
            entry,
        )
        copied_dirs.append(copied_path)

    return copied_dirs


def copy_file(
    source: str,
    destination: str,
    target_name: str | None = None,
    copied_registry: Dict[str, Dict[str, Any]] | None = None,
    registry_entry: Dict[str, Any] | None = None,
) -> str:
    """Copy a file into the destination directory with conflict-aware suffixing."""
    if not os.path.isfile(source):
        raise ValueError(f"Source file '{source}' does not exist")

    os.makedirs(destination, exist_ok=True)

    source_abs = os.path.abspath(source)
    destination_abs = os.path.abspath(destination)
    source_name = os.path.basename(source_abs)
    target_filename = (target_name or "").strip() or source_name
    if not os.path.splitext(target_filename)[1]:
        target_filename = f"{target_filename}{os.path.splitext(source_name)[1]}"
    target_abs = os.path.abspath(os.path.join(destination_abs, target_filename))

    if target_abs == source_abs:
        raise ValueError("Destination cannot be the same as the source file")

    registry = copied_registry if copied_registry is not None else {}
    existing_info = registry.get(target_abs) or {}
    existing_source = existing_info.get("source") if isinstance(existing_info, dict) else str(existing_info)
    folder_name = os.path.splitext(os.path.basename(target_filename))[0] or "copied_file"

    if os.path.exists(target_abs):
        if existing_source and os.path.abspath(existing_source) != source_abs:
            existing_suffix = _infer_conflict_suffix(existing_source, source_abs, folder_name)
            renamed_existing = _dedupe_path(
                _build_file_target_path(destination_abs, target_filename, existing_suffix)
            )
            if os.path.abspath(renamed_existing) != target_abs:
                shutil.move(target_abs, renamed_existing)
                registry.pop(target_abs, None)
                registry[os.path.abspath(renamed_existing)] = existing_info

            current_suffix = _infer_conflict_suffix(source_abs, existing_source, folder_name)
            target_abs = _dedupe_path(
                _build_file_target_path(destination_abs, target_filename, current_suffix)
            )
        else:
            target_abs = _dedupe_path(
                _build_file_target_path(destination_abs, target_filename, _fallback_suffix(source_abs, folder_name))
            )

    shutil.copy2(source_abs, target_abs)
    info = dict(registry_entry or {})
    info["source"] = source_abs
    registry[os.path.abspath(target_abs)] = info
    return target_abs


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
