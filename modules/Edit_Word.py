"""Deprecated Word editing helpers.

The active workflow implementation now handles heading insertion and
figure/table renumbering with python-docx/XML logic in ``modules.workflow``.
These compatibility names remain only to fail clearly for stale callers.
"""


def _unsupported(name: str):
    raise RuntimeError(f"{name} is no longer supported; use modules.workflow instead.")


def insert_text(*_args, **_kwargs):
    return _unsupported("insert_text")


def insert_numbered_heading(*_args, **_kwargs):
    return _unsupported("insert_numbered_heading")


def insert_roman_heading(*_args, **_kwargs):
    return _unsupported("insert_roman_heading")


def insert_bulleted_heading(*_args, **_kwargs):
    return _unsupported("insert_bulleted_heading")


def renumber_figures_tables(*_args, **_kwargs):
    return _unsupported("renumber_figures_tables")


def renumber_figures_tables_file(*_args, **_kwargs):
    return _unsupported("renumber_figures_tables_file")
